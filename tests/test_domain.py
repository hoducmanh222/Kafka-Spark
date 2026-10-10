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
from scripts.ensure_data import verify_complete
from scripts.live_demo import find_real_trace

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


    def test_asynchronous_replay_acknowledges_every_future(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'events.csv'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=event()); writer.writeheader()
                writer.writerows([event(timestamp=BASE + i) for i in range(5)])
            producer = Mock()
            futures = [Mock() for _ in range(5)]
            for i, future in enumerate(futures):
                future.get.return_value.partition = i % 2
                future.get.return_value.offset = i // 2
            producer.send.side_effect = futures
            result = replay(path, producer, 'topic', 0, max_pending=2, expected_rows=5)
            self.assertEqual(result['sent'], 5)
            self.assertEqual(result['event_counts'], {'view': 5})
            self.assertEqual(sum(p['count'] for p in result['partitions'].values()), 5)
            self.assertTrue(all(future.get.call_count == 1 for future in futures))

    def test_full_data_guard_and_acknowledgement_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'events.csv'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=event()); writer.writeheader(); writer.writerow(event())
            with self.assertRaisesRegex(ValueError, 'Full dataset required'):
                verify_complete(path)
            producer = Mock()
            producer.send.return_value.get.side_effect = TimeoutError('broker acknowledgement failed')
            with self.assertRaisesRegex(TimeoutError, 'acknowledgement'):
                replay(path, producer, 'topic', 0)

    def test_real_demo_selects_only_source_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'events.csv'
            source = [{**event(timestamp=BASE), 'visitorid': 20, 'itemid': 202},
                      {**event(timestamp=BASE + 1), 'visitorid': 21, 'itemid': 303},
                      event(timestamp=BASE + 1000), event('addtocart', timestamp=BASE + 2000),
                      event('transaction', 99, BASE + 3000)]
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=event()); writer.writeheader(); writer.writerows(source)
            before, purchase = find_real_trace(path)
            self.assertEqual(purchase, source[-1])
            self.assertTrue(all(row in source for row in before))
            self.assertEqual({r['event'] for r in before}, {'view', 'addtocart'})


if __name__ == '__main__':
    unittest.main()
