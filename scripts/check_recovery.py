"""Verify persisted offsets are unique and restarted queries retain their identity."""
import json
import sqlite3
import tempfile
import sys
from collections import defaultdict
from pathlib import Path
import pyarrow.dataset as ds
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.audit import committed_audit

root = Path('runtime')
audit = committed_audit(root)
assert audit is not None, 'no committed Parquet audit exists'
with tempfile.TemporaryDirectory(prefix='recovery-offsets-') as temp:
    connection = sqlite3.connect(str(Path(temp) / 'offsets.sqlite'))
    try:
        connection.execute('CREATE TABLE offsets (topic TEXT, partition INTEGER, offset INTEGER, PRIMARY KEY(topic,partition,offset))')
        for batch in audit.scanner(columns=['topic', 'partition', 'offset'], batch_size=65536).to_batches():
            connection.executemany('INSERT INTO offsets VALUES(?,?,?)',
                                   [(r['topic'], r['partition'], r['offset']) for r in batch.to_pylist()])
    except sqlite3.IntegrityError as exc:
        raise AssertionError('duplicate Kafka coordinates in curated Parquet') from exc
    finally:
        connection.close()
progress = [json.loads(s) for s in (root / 'metrics/progress.jsonl').read_text().splitlines() if s]
runs = defaultdict(set)
ids = defaultdict(set)
for record in progress:
    runs[record['query']].add(record['run_id'])
    ids[record['query']].add(record['query_id'])
for name in ['curated_audit', 'intent_recommendations']:
    assert len(runs[name]) >= 2, f'{name}: no evidence of restart'
    assert len(ids[name]) == 1, f'{name}: checkpoint identity changed'
print('PASS restart retained query identity, changed run identity, and produced no duplicate Kafka coordinates')
