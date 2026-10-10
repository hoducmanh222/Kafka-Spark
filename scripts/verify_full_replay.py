"""Verify every acknowledged full-source event reached the Parquet audit and intent query."""
import argparse
import json
import sqlite3
import tempfile
import time
import sys
from collections import Counter
from pathlib import Path

import pyarrow.dataset as ds
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.audit import committed_audit


def offset_filter(receipt):
    expression = None
    for partition, bounds in receipt['partitions'].items():
        part = ((ds.field('partition') == int(partition)) &
                (ds.field('offset') >= bounds['first_offset']) &
                (ds.field('offset') <= bounds['last_offset']))
        expression = part if expression is None else expression | part
    return expression & (ds.field('topic') == receipt['topic'])


def intent_caught_up(root, receipt):
    path = root / 'metrics/progress.jsonl'
    if not path.exists():
        return False
    for line in reversed(path.read_text(encoding='utf-8').splitlines()):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue  # A listener may still be appending the final line.
        if record.get('query') != 'intent_recommendations':
            continue
        for source in record.get('sources') or []:
            end = source.get('endOffset') or {}
            if isinstance(end, str):
                end = json.loads(end)
            offsets = end.get(receipt['topic'], {})
            if all(int(offsets.get(p, -1)) >= b['last_offset'] + 1
                   for p, b in receipt['partitions'].items()):
                return True
    return False


def verify(root='runtime', timeout=7200, expected_rows=2_756_101):
    root = Path(root)
    (root / 'full_replay_verified.json').unlink(missing_ok=True)
    deadline = time.monotonic() + timeout
    receipt = None
    while time.monotonic() < deadline:
        completed = root / 'producer_complete.json'
        if completed.exists():
            receipt = json.loads(completed.read_text(encoding='utf-8'))
            if receipt['sent'] != expected_rows or receipt['skipped']:
                raise AssertionError(f'The producer did not complete the full {expected_rows:,}-event source')
            if any(b['last_offset'] - b['first_offset'] + 1 != b['count']
                   for b in receipt['partitions'].values()):
                raise AssertionError('Another producer interleaved the full-replay offset ranges')
            audit = committed_audit(root)
            if audit is not None:
                count = audit.count_rows(filter=offset_filter(receipt))
                print(f'FULL REPLAY: {count:,}/{receipt["sent"]:,} Parquet rows', flush=True)
                if count >= receipt['sent'] and intent_caught_up(root, receipt):
                    break
        time.sleep(10)
    else:
        raise TimeoutError('Full replay has not finished; inspect producer and Spark logs. No completion is claimed.')
    counts = Counter()
    with tempfile.TemporaryDirectory(prefix='verify-full-') as temp:
        connection = sqlite3.connect(str(Path(temp) / 'offsets.sqlite'))
        try:
            connection.execute('CREATE TABLE offsets (partition INTEGER, offset INTEGER, PRIMARY KEY(partition,offset))')
            scanner = audit.scanner(columns=['partition', 'offset', 'event', 'is_valid'],
                                    filter=offset_filter(receipt), batch_size=65536)
            total = 0
            for batch in scanner.to_batches():
                rows = batch.to_pylist()
                if not all(row['is_valid'] in (True, 'true') for row in rows):
                    raise AssertionError('Invalid records found in the full genuine replay')
                connection.executemany('INSERT INTO offsets VALUES(?,?)',
                                       [(r['partition'], r['offset']) for r in rows])
                counts.update(r['event'] for r in rows)
                total += len(rows)
            assert total == receipt['sent'], 'Parquet count differs from acknowledged CSV count'
            assert dict(counts) == receipt['event_counts'], 'Event category counts differ from the source'
            assert len(receipt['partitions']) >= 2, 'Full replay did not use both Kafka partitions'
        finally:
            connection.close()
    result = {'status': 'passed', 'source_rows': total, 'event_counts': dict(counts),
              'unique_kafka_coordinates': True, 'intent_query_caught_up': True}
    (root / 'full_replay_verified.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    label = 'PASS FULL DATASET END TO END' if expected_rows == 2_756_101 else 'PASS VERIFIER FIXTURE'
    print(label, json.dumps(result), flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=int, default=7200)
    verify(timeout=parser.parse_args().timeout)
