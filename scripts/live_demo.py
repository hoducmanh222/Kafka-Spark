"""Show a genuine cart-to-purchase trace from the full Retail Rocket events.csv."""
import argparse
import json
import os
import sys
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from producer import make_producer, send_event
from retail.domain import csv_stream, normalize_event
from retail.files import latest_epoch, read_recommendations


def wait_for(predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(2)
    raise TimeoutError('Expected output did not arrive; inspect docker compose logs spark')


def find_real_trace(path):
    """Select actual view/cart/transaction rows, with actual candidate views nearby."""
    recent = deque()
    with csv_stream(path) as source:
        for raw in source:
            row = normalize_event(raw)
            while recent and recent[0]['timestamp'] < row['timestamp'] - 120000:
                recent.popleft()
            if row['event'] == 'transaction':
                cart = view = None
                for prior in reversed(recent):
                    if (prior['visitorid'], prior['itemid']) != (row['visitorid'], row['itemid']):
                        continue
                    if cart is None and prior['event'] == 'addtocart':
                        cart = prior
                    elif cart is not None and prior['event'] == 'view':
                        view = prior
                        break
                if cart and view and row['timestamp'] - view['timestamp'] <= 60000:
                    candidates = []
                    items = set()
                    for candidate in reversed(recent):
                        if (candidate['event'] == 'view' and candidate['visitorid'] != row['visitorid']
                                and candidate['itemid'] != row['itemid']
                                and candidate['timestamp'] <= view['timestamp']
                                and candidate['itemid'] not in items):
                            candidates.append(candidate)
                            items.add(candidate['itemid'])
                            if len(candidates) == 2:
                                return sorted(candidates + [view, cart], key=lambda e: e['timestamp']), row
            recent.append(row)
    raise ValueError('No genuine short view/cart/purchase trace with candidates found in the full CSV')


def run(root='runtime', bootstrap=None, csv_path='data/events.csv'):
    root = Path(root)
    verified = root / 'full_replay_verified.json'
    if not verified.exists() or json.loads(verified.read_text())['source_rows'] != 2_756_101:
        raise RuntimeError('First complete and verify the full dataset: docker compose --profile tools run --rm verify-full')
    before_purchase, purchase = find_real_trace(csv_path)
    epoch = latest_epoch(root)
    old_max = json.loads((epoch / 'manifest.json').read_text())['event_max_ms'] if epoch else 0
    base = max(int(time.time() * 1000), old_max + 20 * 60000)
    shift = base - before_purchase[0]['timestamp']
    visitor = purchase['visitorid']
    producer = make_producer(bootstrap or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))

    def emit(original):
        row = dict(original)
        row['timestamp'] += shift
        metadata = send_event(producer, row)
        print('REAL DATASET TRACE', json.dumps(row), 'original_timestamp', original['timestamp'],
              'partition', metadata.partition, 'offset', metadata.offset, flush=True)

    try:
        print('Full source verified: 2,756,101 events. Replaying selected genuine rows from', csv_path,
              'with constant timestamp_shift_ms', shift, flush=True)
        for row in before_purchase:
            emit(row)
        wait_for(lambda: any(r['visitorid'] == visitor for r in read_recommendations(root)))
        print('REAL CART OBSERVED', json.dumps([r for r in read_recommendations(root)
                                               if r['visitorid'] == visitor]), flush=True)
        time.sleep(12)
        previous = latest_epoch(root)
        emit(purchase)
        wait_for(lambda: latest_epoch(root) != previous and
                 not any(r['visitorid'] == visitor for r in read_recommendations(root)))
        print('REAL PURCHASE OBSERVED: recommendations retracted for visitor', visitor, flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', default='data/events.csv')
    run(csv_path=parser.parse_args().csv)
