"""Deterministic mock telemetry for explaining carts and purchase retraction."""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from producer import make_producer, send_event
from retail.files import latest_epoch, read_recommendations


def wait_for(predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(2)
    raise TimeoutError('Expected output did not arrive; inspect docker compose logs spark')


def run(root='runtime', bootstrap=None):
    epoch = latest_epoch(root)
    old_max = json.loads((epoch / 'manifest.json').read_text())['event_max_ms'] if epoch else 0
    base = max(int(time.time() * 1000), old_max + 60000)
    visitor = 900000001 + int(time.time()) % 100000
    producer = make_producer(bootstrap or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))
    def emit(v, action, item, delta, tx=None):
        e = {'timestamp': base + delta, 'visitorid': v, 'event': action, 'itemid': item, 'transactionid': tx}
        m = send_event(producer, e)
        print('MOCK DEMO', json.dumps(e), 'partition', m.partition, 'offset', m.offset, flush=True)
    try:
        emit(visitor + 1, 'view', 202, 0)
        emit(visitor + 2, 'addtocart', 303, 500)
        emit(visitor, 'view', 101, 1000)
        emit(visitor, 'addtocart', 101, 2000)
        wait_for(lambda: any(r['visitorid'] == visitor for r in read_recommendations(root)))
        print('CART OBSERVED', json.dumps([r for r in read_recommendations(root) if r['visitorid'] == visitor]), flush=True)
        time.sleep(12)
        previous = latest_epoch(root)
        emit(visitor, 'transaction', 101, 3000, 777)
        wait_for(lambda: latest_epoch(root) != previous and not any(r['visitorid'] == visitor for r in read_recommendations(root)))
        print('PURCHASE OBSERVED: recommendations retracted for visitor', visitor, flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    run()
