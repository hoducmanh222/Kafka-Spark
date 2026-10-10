"""Paced, acknowledged Retail Rocket CSV to Kafka producer.

Example: python producer.py --csv data/events.csv --replay-clock --expected-rows 2756101
Use an event-time sorted CSV for watermark-safe historical replay.
"""
import argparse
import json
import logging
import os
import time
from collections import Counter, deque
from pathlib import Path

from retail.domain import csv_stream, normalize_event

LOG = logging.getLogger('retail.producer')


def make_producer(bootstrap):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(','), client_id='retail-rocket-producer',
        acks='all', enable_idempotence=True, retries=10, max_in_flight_requests_per_connection=5,
        linger_ms=10, batch_size=65536,
        request_timeout_ms=30000,
        key_serializer=lambda k: str(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v, separators=(',', ':'), allow_nan=False).encode('utf-8'))


def send_event(producer, event, topic='retail-rocket-events'):
    event = normalize_event(event)
    metadata = producer.send(topic, key=event['visitorid'], value=event).get(timeout=30)
    return metadata


def replay(path, producer, topic, interval, limit=0, replay_clock=False, allow_out_of_order=False,
           max_pending=2000, expected_rows=0):
    sent = skipped = 0
    first_timestamp = None
    shift = 0
    previous = None
    pending = deque()
    counts = Counter()
    partitions = {}

    def acknowledge():
        number, future = pending.popleft()
        metadata = future.get(timeout=60)
        partition = str(metadata.partition)
        bounds = partitions.setdefault(partition, {'first_offset': metadata.offset,
                                                   'last_offset': metadata.offset, 'count': 0})
        bounds['last_offset'] = metadata.offset
        bounds['count'] += 1
        if number <= 5 or number % 10000 == 0:
            LOG.info('acknowledged count=%s partition=%s offset=%s', number, metadata.partition, metadata.offset)
    with csv_stream(path) as rows:
        for line_number, row in enumerate(rows, start=2):
            try:
                event = normalize_event(row)
            except ValueError as exc:
                skipped += 1
                LOG.error('invalid_csv_row line=%s error=%s', line_number, exc)
                continue
            original_timestamp = event['timestamp']
            if previous is not None and original_timestamp < previous and not allow_out_of_order:
                raise ValueError(f'CSV is not sorted at line {line_number}; run scripts/prepare_data.py')
            previous = original_timestamp
            if first_timestamp is None:
                first_timestamp = original_timestamp
                shift = int(time.time() * 1000) - first_timestamp if replay_clock else 0
            event['timestamp'] += shift
            before = time.monotonic()
            future = producer.send(topic, key=event['visitorid'], value=event)
            sent += 1
            counts[event['event']] += 1
            pending.append((sent, future))
            if interval > 0 or len(pending) >= max_pending:
                acknowledge()
            if limit and sent >= limit:
                break
            if interval > 0:
                time.sleep(max(0, interval - (time.monotonic() - before)))
    while pending:
        acknowledge()
    if expected_rows and (sent != expected_rows or skipped):
        raise ValueError(f'Full replay incomplete: acknowledged={sent}, expected={expected_rows}, skipped={skipped}')
    LOG.info('producer_complete sent=%s skipped=%s timestamp_shift_ms=%s', sent, skipped, shift)
    return {'sent': sent, 'skipped': skipped, 'timestamp_shift_ms': shift,
            'source': str(path), 'topic': topic, 'event_counts': dict(counts), 'partitions': partitions}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv', default='data/events.csv')
    p.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))
    p.add_argument('--topic', default='retail-rocket-events')
    p.add_argument('--interval', type=float, default=0, help='seconds per event; 0 uses bounded asynchronous batching')
    p.add_argument('--limit', type=int, default=0, help='0 reads all rows')
    p.add_argument('--replay-clock', action='store_true', help='shift all event times by a single constant')
    p.add_argument('--allow-out-of-order', action='store_true', help='intentional lateness experiments only')
    p.add_argument('--max-pending', type=int, default=2000)
    p.add_argument('--expected-rows', type=int, default=0, help='fail unless this many records are acknowledged')
    p.add_argument('--completion-file', default=None)
    args = p.parse_args()
    if args.interval < 0 or min(args.limit, args.expected_rows) < 0 or args.max_pending < 1:
        p.error('interval, limit and expected-rows must be nonnegative; max-pending must be positive')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.completion_file:
        Path(args.completion_file).unlink(missing_ok=True)
        (Path(args.completion_file).parent / 'full_replay_verified.json').unlink(missing_ok=True)
    producer = make_producer(args.bootstrap)
    try:
        result = replay(args.csv, producer, args.topic, args.interval, args.limit, args.replay_clock,
                        args.allow_out_of_order, args.max_pending, args.expected_rows)
        if args.completion_file:
            target = Path(args.completion_file)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix('.tmp')
            temporary.write_text(json.dumps(result, indent=2), encoding='utf-8')
            os.replace(temporary, target)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


if __name__ == '__main__':
    main()
