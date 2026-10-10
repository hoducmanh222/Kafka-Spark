"""Kafka extraction, validated Parquet audit, sliding intent and live console."""
import argparse
import json
import logging
import os
import signal
import time
from pathlib import Path

from pyspark.sql import SparkSession

from retail.sinks import EpochSink, ProgressLogger
from retail.transforms import aggregate_intent, parse_events


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))
    p.add_argument('--topic', default='retail-rocket-events')
    p.add_argument('--output', default='runtime')
    p.add_argument('--trigger', default='5 seconds')
    p.add_argument('--threshold', type=int, default=5)
    p.add_argument('--top-k', type=int, default=3)
    p.add_argument('--max-offsets', type=int, default=1000)
    p.add_argument('--console', action='store_true')
    p.add_argument('--fail-after-publish-batch', type=int, default=None)
    args = p.parse_args()
    if min(args.threshold, args.top_k, args.max_offsets) <= 0:
        p.error('threshold, top-k and max-offsets must be positive')
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s',
                        handlers=[logging.StreamHandler(), logging.FileHandler(root / 'application.log', encoding='utf-8')])
    spark = (SparkSession.builder.appName('RetailRocketSessionPersonalization')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.adaptive.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    listener = ProgressLogger(root / 'metrics' / 'progress.jsonl')
    spark.streams.addListener(listener)
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', args.bootstrap)
           .option('subscribe', args.topic).option('startingOffsets', 'earliest')
           .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', args.max_offsets).load())
    parsed = parse_events(raw)
    queries = []
    stopped = False

    def stop(signum=None, frame=None):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    ready = root / 'ready.json'
    ready.unlink(missing_ok=True)
    try:
        # All rows, including invalid JSON, remain auditable by Kafka coordinates.
        queries.append(parsed.writeStream.queryName('curated_audit').format('parquet')
                       .outputMode('append').partitionBy('is_valid', 'ingest_date')
                       .option('path', str(root / 'curated'))
                       .option('checkpointLocation', str(root / 'checkpoints' / 'curated'))
                       .trigger(processingTime=args.trigger).start())
        sink = EpochSink(root / 'intent' / 'epochs', args.threshold, args.top_k, args.fail_after_publish_batch)
        queries.append(aggregate_intent(parsed).writeStream.queryName('intent_recommendations')
                       .outputMode('update').foreachBatch(sink)
                       .option('checkpointLocation', str(root / 'checkpoints' / 'intent'))
                       .trigger(processingTime=args.trigger).start())
        if args.console:
            queries.append(parsed.select('visitorid', 'event', 'itemid', 'event_time', 'weight', 'is_valid')
                           .writeStream.queryName('live_events_console').format('console')
                           .option('truncate', 'false').option('numRows', 10)
                           .option('checkpointLocation', str(root / 'checkpoints' / 'console'))
                           .trigger(processingTime=args.trigger).start())
        while not stopped:
            for query in queries:
                if not query.isActive:
                    exc = query.exception()
                    raise RuntimeError(f'Query {query.name} stopped: {exc}')
            temp = ready.with_suffix('.tmp')
            temp.write_text(json.dumps({'heartbeat': time.time(), 'queries': [q.name for q in queries]}))
            os.replace(temp, ready)
            time.sleep(2)
    finally:
        ready.unlink(missing_ok=True)
        for query in queries:
            if query.isActive:
                query.stop()
        spark.streams.removeListener(listener)
        spark.stop()


if __name__ == '__main__':
    main()
