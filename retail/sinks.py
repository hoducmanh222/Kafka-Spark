"""Atomic local epoch output for retry-safe foreachBatch publication.

Contract: one writer, immutable completed epoch directories, POSIX local filesystem
or a Docker bind mount on the same filesystem. Not an object-storage protocol.
"""
import json
import logging
import os
import shutil
import time
import uuid
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from retail.transforms import merge_updates, recommendations

LOG = logging.getLogger('retail.sinks')


def committed_epochs(root):
    root = Path(root)
    return sorted(p for p in root.glob('[0-9]*') if p.is_dir() and (p / 'manifest.json').is_file())


class EpochSink:
    def __init__(self, root, threshold=5, top_k=3, fail_after_publish_batch=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.threshold, self.top_k = threshold, top_k
        self.fail_after_publish_batch = fail_after_publish_batch

    def __call__(self, batch, batch_id):
        started = time.monotonic()
        batch.persist()
        try:
            # Completely consume the stateful batch even when the sink epoch exists.
            changed_rows = batch.count()
            final = self.root / f'{batch_id:020d}'
            if final.exists():
                if not (final / 'manifest.json').exists():
                    raise RuntimeError(f'Incomplete committed epoch: {final}')
                LOG.info('batch_retry_skipped batch_id=%s aggregate_rows=%s', batch_id, changed_rows)
                return
            if changed_rows == 0:
                LOG.info('batch_empty batch_id=%s aggregate_rows=0 execution_ms=%.1f',
                         batch_id, (time.monotonic() - started) * 1000)
                return
            spark = batch.sparkSession
            epochs = [p for p in committed_epochs(self.root) if int(p.name) < batch_id]
            previous_max = 0
            previous = batch.limit(0)
            if epochs:
                prior = epochs[-1]
                previous = spark.read.parquet(str(prior / 'state')).drop('event_date')
                previous_max = json.loads((prior / 'manifest.json').read_text())['event_max_ms']
            event_max = max(previous_max, batch.select(F.max(F.unix_millis('last_event_time'))).first()[0])
            state = merge_updates(previous, batch, event_max).persist()
            recs = recommendations(state, event_max, self.threshold, self.top_k).persist()
            stage = self.root / f'.stage-{batch_id}-{uuid.uuid4().hex}'
            stage.mkdir()
            try:
                state_count, recommendation_count = state.count(), recs.count()
                state.withColumn('event_date', F.to_date('window_start')).write.mode('overwrite').partitionBy('event_date').parquet(str(stage / 'state'))
                writer = recs.write.mode('overwrite')
                if recommendation_count:
                    writer = writer.partitionBy('event_date')
                writer.parquet(str(stage / 'recommendations'))
                # Readiness and test clients use JSONL as a serving view of the same rows.
                with (stage / 'recommendations.jsonl').open('w', encoding='utf-8') as out:
                    for row in recs.orderBy('visitorid', 'rank').toLocalIterator():
                        out.write(json.dumps(row.asDict(), default=str) + '\n')
                manifest = {'batch_id': batch_id, 'aggregate_rows': changed_rows,
                            'retained_state_rows': state_count, 'recommendation_rows': recommendation_count,
                            'event_max_ms': event_max, 'application_watermark_ms': event_max - 300_000,
                            'execution_ms': round((time.monotonic() - started) * 1000, 2)}
                (stage / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
                os.replace(stage, final)  # Publish all epoch files together.
                LOG.info('batch_committed %s', json.dumps(manifest))
                print(f'CURRENT RECOMMENDATIONS batch={batch_id} rows={recommendation_count}', flush=True)
                recs.orderBy('visitorid', 'rank').show(20, truncate=False)
                if batch_id == self.fail_after_publish_batch:
                    marker = self.root / f'.failure-injected-{batch_id}'
                    if not marker.exists():
                        marker.write_text('one-time failure after sink publication, before Spark commit')
                        raise RuntimeError('Injected failure after epoch publication')
            finally:
                state.unpersist()
                recs.unpersist()
                if stage.exists():
                    shutil.rmtree(stage)
        finally:
            batch.unpersist()


class ProgressLogger(StreamingQueryListener):
    def __init__(self, path):
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event):
        LOG.info('query_started name=%s id=%s', event.name, event.id)

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        record = {'query': progress.get('name'), 'query_id': progress.get('id'),
                  'run_id': progress.get('runId'), 'batch_id': progress['batchId'],
                  'record_count': progress['numInputRows'],
                  'execution_ms': progress.get('durationMs', {}).get('triggerExecution'),
                  'input_rows_per_second': progress.get('inputRowsPerSecond'),
                  'processed_rows_per_second': progress.get('processedRowsPerSecond'),
                  'event_time': progress.get('eventTime'),
                  'state_operators': progress.get('stateOperators'),
                  'sources': progress.get('sources')}
        with self.path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(record) + '\n')
        LOG.info('stream_progress %s', json.dumps(record))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        LOG.info('query_terminated id=%s exception=%s', event.id, event.exception)
