"""Verify that downstream evidence cannot pass with duplicates or invalid rows."""
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.verify_full_replay import intent_caught_up, verify
from retail.audit import committed_audit


class VerifierTests(unittest.TestCase):
    def fixture(self, root, rows=None):
        root = Path(root)
        (root / 'curated').mkdir()
        (root / 'metrics').mkdir()
        rows = rows or [dict(topic='t', partition=0, offset=0, event='view', is_valid=True),
                        dict(topic='t', partition=0, offset=1, event='addtocart', is_valid=True),
                        dict(topic='t', partition=1, offset=0, event='view', is_valid=True)]
        pq.write_table(pa.Table.from_pylist(rows), root / 'curated/part.parquet')
        metadata = root / 'curated/_spark_metadata'
        metadata.mkdir()
        record = {'path': (root / 'curated/part.parquet').resolve().as_uri(), 'isDir': False, 'action': 'add'}
        (metadata / '0').write_text('v1\n' + json.dumps(record) + '\n')
        receipt = {'sent': len(rows), 'skipped': 0, 'topic': 't',
                   'event_counts': {'view': 2, 'addtocart': len(rows) - 2},
                   'partitions': {'0': {'first_offset': 0, 'last_offset': len(rows) - 2, 'count': len(rows) - 1},
                                  '1': {'first_offset': 0, 'last_offset': 0, 'count': 1}}}
        (root / 'producer_complete.json').write_text(json.dumps(receipt))
        progress = {'query': 'intent_recommendations', 'sources': [
            {'endOffset': {'t': {'0': len(rows) - 1, '1': 1}}}]}
        (root / 'metrics/progress.jsonl').write_text(json.dumps(progress) + '\n')
        return root, receipt

    def test_complete_offsets_and_categories_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            root, receipt = self.fixture(temp)
            result = verify(root, timeout=1, expected_rows=3)
            self.assertEqual(result['source_rows'], 3)
            self.assertTrue((root / 'full_replay_verified.json').exists())

    def test_duplicate_coordinate_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            rows = [dict(topic='t', partition=0, offset=0, event='view', is_valid=True),
                    dict(topic='t', partition=0, offset=1, event='addtocart', is_valid=True),
                    dict(topic='t', partition=0, offset=1, event='addtocart', is_valid=True),
                    dict(topic='t', partition=1, offset=0, event='view', is_valid=True)]
            root, receipt = self.fixture(temp, rows)
            import sqlite3
            with self.assertRaises(sqlite3.IntegrityError):
                verify(root, timeout=1, expected_rows=4)
            self.assertFalse((root / 'full_replay_verified.json').exists())

    def test_invalid_record_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root, receipt = self.fixture(temp)
            table = pq.read_table(root / 'curated/part.parquet').to_pylist()
            table[0]['is_valid'] = False
            pq.write_table(pa.Table.from_pylist(table), root / 'curated/part.parquet')
            with self.assertRaisesRegex(AssertionError, 'Invalid records'):
                verify(root, timeout=1, expected_rows=3)

    def test_intent_progress_must_cover_every_partition(self):
        with tempfile.TemporaryDirectory() as temp:
            root, receipt = self.fixture(temp)
            path = root / 'metrics/progress.jsonl'
            progress = {'query': 'intent_recommendations', 'sources': [
                {'endOffset': json.dumps({'t': {'0': 2, '1': 0}})}]}
            path.write_text(json.dumps(progress) + '\n')
            self.assertFalse(intent_caught_up(root, receipt))
            progress['sources'][0]['endOffset'] = json.dumps({'t': {'0': 2, '1': 1}})
            path.write_text(json.dumps(progress) + '\n' + '{')
            self.assertTrue(intent_caught_up(root, receipt))

    def test_uncommitted_orphan_and_compacted_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root, receipt = self.fixture(temp)
            original = (root / 'curated/_spark_metadata/0').read_text()
            (root / 'curated/_spark_metadata/9.compact').write_text(original)
            pq.write_table(pa.Table.from_pylist([dict(topic='t', partition=0, offset=999,
                                                     event='view', is_valid=True)]),
                           root / 'curated/orphan.parquet')
            self.assertEqual(committed_audit(root).count_rows(), 3)
            result = verify(root, timeout=1, expected_rows=3)
            self.assertEqual(result['source_rows'], 3)


if __name__ == '__main__':
    unittest.main()
