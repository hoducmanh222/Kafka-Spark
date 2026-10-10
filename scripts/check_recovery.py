"""Verify persisted offsets are unique and restarted queries retain their identity."""
import json
from collections import defaultdict
from pathlib import Path
import pyarrow.dataset as ds

root = Path('runtime')
audit = ds.dataset(str(root / 'curated'), format='parquet', partitioning='hive')
coordinates = [(r['topic'], r['partition'], r['offset']) for r in audit.to_table(columns=['topic', 'partition', 'offset']).to_pylist()]
assert len(coordinates) == len(set(coordinates)), 'duplicate Kafka coordinates in curated Parquet'
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
