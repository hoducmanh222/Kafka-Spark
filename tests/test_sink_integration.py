"""POSIX filesystem acceptance of sink failure and replay behavior."""
import tempfile
import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from retail.sinks import EpochSink, committed_epochs
from retail.transforms import aggregate_intent
from tests.test_spark import SparkTests
from tests.test_domain import BASE, event


class SinkTests(SparkTests):
    def test_atomic_publish_then_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            scores = aggregate_intent(self.parse([event('addtocart'), {**event(), 'visitorid': 21, 'itemid': 202}]))
            sink = EpochSink(temp, fail_after_publish_batch=0)
            with self.assertRaisesRegex(RuntimeError, 'Injected failure'):
                sink(scores, 0)
            sink(scores, 0)
            self.assertEqual(len(committed_epochs(temp)), 1)
            files = list(Path(temp).glob('.stage-*'))
            self.assertEqual(files, [])
            state = self.spark.read.parquet(str(committed_epochs(temp)[0] / 'state'))
            self.assertEqual(state.count(), 20)
            self.assertEqual({r.intent_score for r in state.filter('visitorid=12').collect()}, {5})


if __name__ == '__main__':
    unittest.main()
