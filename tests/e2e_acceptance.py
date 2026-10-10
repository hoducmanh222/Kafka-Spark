"""Real Kafka, Spark, Parquet and checkpoint acceptance test. Fresh stack preferred."""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from producer import make_producer, send_event
from retail.files import latest_epoch, read_recommendations
from scripts.live_demo import wait_for


def main():
    from kafka import KafkaConsumer, TopicPartition
    import pyarrow.dataset as ds
    root = Path('runtime')
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
    producer = make_producer(bootstrap)
    epoch = latest_epoch(root)
    current_max = json.loads((epoch / 'manifest.json').read_text())['event_max_ms'] if epoch else 0
    base = max(int(time.time() * 1000), current_max + 60000)
    user = 800000000 + int(time.time()) % 1000000
    item = 700000000 + int(time.time()) % 1000000
    sent = []
    def emit(visitor, action, product, delta=0, tx=None):
        event = {'timestamp': base + delta, 'visitorid': visitor, 'event': action, 'itemid': product, 'transactionid': tx}
        meta = send_event(producer, event)
        sent.append((meta.partition, meta.offset))
    def rows():
        return [r for r in read_recommendations(root) if r['visitorid'] == user]
    def state_score(visitor, product, score):
        ep = latest_epoch(root)
        if ep is None: return False
        table = ds.dataset(str(ep / 'state'), format='parquet', partitioning='hive').to_table(
            filter=(ds.field('visitorid') == visitor) & (ds.field('itemid') == product))
        return any(r['intent_score'] == score for r in table.to_pylist())
    def new_epoch(after):
        ep = latest_epoch(root)
        return ep is not None and ep != after
    try:
        # Find keys on both partitions to verify actual Kafka partition coverage.
        partitions = set()
        for i in range(20):
            emit(user + i + 1, 'view', item + 2 + i, i)
            partitions.add(sent[-1][0])
            if len(partitions) >= 2: break
        assert len(partitions) >= 2, 'producer did not use both Kafka partitions'
        emit(user, 'view', item, 100)
        emit(user, 'addtocart', item, 200)
        wait_for(lambda: rows() and state_score(user, item, 6))
        assert all(r['recommended_itemid'] != item for r in rows())
        print('PASS cart score 6, unseen recommendations, two Kafka partitions', flush=True)
        emit(user + 50, 'view', item + 50, -2 * 60000)
        emit(user + 50, 'addtocart', item + 50, -2 * 60000 + 1)
        wait_for(lambda: state_score(user + 50, item + 50, 6))
        print('PASS two-minute-late input updated retained state', flush=True)
        prior = latest_epoch(root)
        emit(user, 'transaction', item, 300, 12345)
        wait_for(lambda: new_epoch(prior) and not rows() and state_score(user, item, 16))
        print('PASS transaction suppression and replacement snapshot', flush=True)
        # Advance watermark beyond old windows, then inject a definitely expired event.
        emit(user + 100, 'view', item + 100, 20 * 60000)
        wait_for(lambda: json.loads((latest_epoch(root) / 'manifest.json').read_text())['event_max_ms'] >= base + 20 * 60000)
        assert not state_score(user, item, 16), 'old purchased visitor windows were not evicted'
        time.sleep(12)  # Allow Spark's following no-data trigger to apply the new watermark.
        old_epoch = latest_epoch(root)
        emit(user + 101, 'addtocart', item + 101, -60 * 60000)
        emit(user + 102, 'view', item + 102, 20 * 60000 + 1000)
        wait_for(lambda: new_epoch(old_epoch))
        assert not state_score(user + 101, item + 101, 5), 'expired event unexpectedly entered state'
        print('PASS watermark state eviction and very late event rejection', flush=True)
        # Raw audit persists malformed payloads even when intent excludes them.
        producer.send('retail-rocket-events', key=user, value={'event': 'invalid'}).get(timeout=30)
        producer.flush()
        def audit_ready():
            if not list((root / 'curated').rglob('*.parquet')): return False
            audit = ds.dataset(str(root / 'curated'), format='parquet', partitioning='hive')
            return any('invalid' in (r['raw_json'] or '') and r['is_valid'] in (False, 'false')
                       for r in audit.to_table(columns=['raw_json', 'is_valid']).to_pylist())
        wait_for(audit_ready)
        audit = ds.dataset(str(root / 'curated'), format='parquet', partitioning='hive')
        assert 'event_time' in audit.schema.names and 'offset' in audit.schema.names
        assert any((root / 'checkpoints' / 'intent' / 'commits').iterdir())
        progress = [json.loads(s) for s in (root / 'metrics' / 'progress.jsonl').read_text().splitlines() if s]
        assert any(p['record_count'] > 0 and p['execution_ms'] is not None for p in progress)
        consumer = KafkaConsumer(bootstrap_servers=bootstrap.split(','), enable_auto_commit=False)
        try:
            assert len(consumer.partitions_for_topic('retail-rocket-events')) >= 2
        finally:
            consumer.close()
        print('PASS partitioned Parquet, invalid audit, offsets, checkpoint commits and batch metrics', flush=True)
        print('ALL END TO END ACCEPTANCE CHECKS PASSED', flush=True)
    finally:
        producer.close()


if __name__ == '__main__':
    main()
