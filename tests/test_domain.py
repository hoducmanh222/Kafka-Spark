import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

from producer import replay, send_event
from retail.domain import active_window_start, normalize_event, windows_for
from scripts.prepare_data import prepare

BASE = 1700000000000


def event(action='view', tx=None, timestamp=BASE):
    return {'timestamp': timestamp, 'visitorid': 12, 'event': action, 'itemid': 101, 'transactionid': tx}


class DomainTests(unittest.TestCase):
    def test_null_transaction_id(self):
        for value in ['', None, 'null', 'NaN']:
            self.assertIsNone(normalize_event(event(tx=value))['transactionid'])

    def test_required_ids_and_milliseconds(self):
        for field, value in [('visitorid', -1), ('itemid', '3.4'), ('visitorid', True), ('timestamp', 1700000000)]:
            row = event(); row[field] = value
            with self.assertRaises(ValueError): normalize_event(row)

    def test_transaction_contract(self):
        for row in [event('transaction'), event('view', 99), event('purchase')]:
            with self.assertRaises(ValueError): normalize_event(row)
        self.assertEqual(normalize_event(event('transaction', '99'))['transactionid'], 99)

    def test_half_open_windows(self):
        timestamp = (BASE // 60000) * 60000
        windows = windows_for(timestamp)
        self.assertEqual(len(windows), 10)
        self.assertTrue(all(start <= timestamp < end for start, end in windows))
        self.assertEqual(active_window_start(timestamp), min(start for start, _ in windows))

    def test_json_numeric_types_and_kafka_key(self):
        producer = Mock()
        send_event(producer, event('addtocart'))
        _, call = producer.send.call_args
        self.assertEqual(call['key'], 12)
        self.assertIsInstance(call['value']['timestamp'], int)
        self.assertIn('"transactionid": null', json.dumps(call['value']))
        producer.send.return_value.get.assert_called_once_with(timeout=30)

    def test_csv_stream_sorted_replay_and_pacing(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / 'events.csv'
            rows = [event(timestamp=BASE), event('addtocart', timestamp=BASE + 5000)]
            with p.open('w', newline='') as f:
                w = csv.DictWriter(f, fieldnames=rows[0]); w.writeheader(); w.writerows(rows)
            producer = Mock()
            with patch('producer.time.sleep') as sleep, patch('producer.time.time', return_value=(BASE + 100000) / 1000):
                result = replay(p, producer, 'retail-rocket-events', .1, replay_clock=True)
            self.assertEqual(result['sent'], 2)
            values = [c.kwargs['value'] for c in producer.send.call_args_list]
            self.assertEqual(values[1]['timestamp'] - values[0]['timestamp'], 5000)
            self.assertEqual(values[0]['timestamp'], BASE + 100000)
            self.assertEqual(sleep.call_count, 2)

    def test_disk_sort_zip_and_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            src, out = Path(temp) / 'source.zip', Path(temp) / 'out.csv'
            text = io.StringIO(); w = csv.DictWriter(text, fieldnames=event()); w.writeheader()
            w.writerows([event(timestamp=BASE + 1000), event(timestamp=BASE)])
            with zipfile.ZipFile(src, 'w') as z: z.writestr('events.csv', text.getvalue())
            prepare(src, out, 1)
            with out.open() as f: result = list(csv.DictReader(f))
            self.assertEqual(len(result), 1)
            self.assertEqual(int(result[0]['timestamp']), BASE)

    def test_unsorted_source_fails_with_fix(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / 'events.csv'
            with p.open('w', newline='') as f:
                w = csv.DictWriter(f, fieldnames=event()); w.writeheader()
                w.writerows([event(timestamp=BASE + 1000), event(timestamp=BASE)])
            with self.assertRaisesRegex(ValueError, 'prepare_data'):
                replay(p, Mock(), 'topic', 0)


if __name__ == '__main__':
    unittest.main()
