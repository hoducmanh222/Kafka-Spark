"""Run with Java 17 and PySpark 3.5.7, or inside the project image."""
import json
import unittest
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T
from retail.transforms import aggregate_intent, merge_updates, parse_events, recommendations
from tests.test_domain import BASE, event


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('retail-tests')
                     .config('spark.sql.session.timeZone', 'UTC')
                     .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def parse(self, events):
        # Native SQL input mirrors the connector's JVM rows without Python workers.
        selects = []
        for i, e in enumerate(events):
            encoded = (e if isinstance(e, str) else json.dumps(e)).encode().hex()
            selects.append(f"SELECT CAST(unhex('{encoded}') AS STRING) AS value, 'retail-rocket-events' AS topic, {i % 2} AS partition, CAST({i} AS BIGINT) AS offset, timestamp_millis({BASE}) AS timestamp")
        return parse_events(self.spark.sql(' UNION ALL '.join(selects)))

    def test_schema_malformed_and_missing_fields(self):
        data = self.parse([event(), '{oops', {'event': 'view'}, event('view', 77), event('transaction', 77)])
        self.assertEqual([r.is_valid for r in data.orderBy('offset').collect()], [True, False, False, False, True])
        self.assertIsInstance(data.schema['event_time'].dataType, T.TimestampType)
        self.assertEqual(data.filter('is_valid').first().event_time.microsecond, (BASE % 1000) * 1000)

    def test_weights_rolling_score_and_transaction_suppression(self):
        events = [event(), event('addtocart'), {**event(), 'visitorid': 21, 'itemid': 202},
                  {**event(), 'visitorid': 22, 'itemid': 303}]
        scores = aggregate_intent(self.parse(events))
        self.assertTrue(all(r.intent_score == 6 for r in scores.filter('visitorid = 12').collect()))
        recs = recommendations(scores, BASE).collect()
        self.assertEqual([r.recommended_itemid for r in recs], [202, 303])
        after = aggregate_intent(self.parse(events + [event('transaction', 7)]))
        self.assertEqual(recommendations(after, BASE).count(), 0)

    def test_views_alone_never_trigger_and_seen_items_excluded(self):
        rows = [event() for _ in range(10)]
        self.assertEqual(recommendations(aggregate_intent(self.parse(rows)), BASE).count(), 0)
        rows += [event('addtocart'), {**event(), 'itemid': 202}, {**event(), 'visitorid': 21, 'itemid': 202},
                 {**event(), 'visitorid': 22, 'itemid': 303}]
        recs = recommendations(aggregate_intent(self.parse(rows)), BASE).collect()
        self.assertEqual([r.recommended_itemid for r in recs], [303])

    def test_updates_replace_absolute_totals_and_state_expires(self):
        old = aggregate_intent(self.parse([event()]))
        new = aggregate_intent(self.parse([event(), event('addtocart')]))
        merged = merge_updates(old, new, BASE)
        self.assertEqual(merged.count(), 10)
        self.assertEqual({r.intent_score for r in merged.collect()}, {6})
        expired = merge_updates(old, old.limit(0), BASE + 20 * 60000)
        self.assertEqual(expired.count(), 0)

    def test_one_window_prevents_popularity_double_counting(self):
        scores = aggregate_intent(self.parse([event('addtocart'), {**event(), 'visitorid': 21, 'itemid': 202}]))
        recs = recommendations(scores, BASE).collect()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].popularity_score, 1)


if __name__ == '__main__':
    unittest.main()
