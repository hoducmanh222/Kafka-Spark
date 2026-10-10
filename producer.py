"""Paced, acknowledged Retail Rocket CSV to Kafka producer.

Example: python producer.py --csv data/events_sample.csv --replay-clock
Use an event-time sorted CSV for watermark-safe historical replay.
"""
import argparse
import json
import logging
import os
import time

from retail.domain import csv_stream, normalize_event

LOG = logging.getLogger('retail.producer')


def make_producer(bootstrap):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(','), client_id='retail-rocket-producer',
        acks='all', enable_idempotence=True, retries=10, max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
        key_serializer=lambda k: str(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v, separators=(',', ':'), allow_nan=False).encode('utf-8'))


def send_event(producer, event, topic='retail-rocket-events'):
    event = normalize_event(event)
    metadata = producer.send(topic, key=event['visitorid'], value=event).get(timeout=30)
    return metadata


def replay(path, producer, topic, interval, limit=0, replay_clock=False, allow_out_of_order=False):
    sent = skipped = 0
    first_timestamp = None
    shift = 0
    previous = None
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
            metadata = send_event(producer, event, topic)
            sent += 1
            if sent <= 5 or sent % 100 == 0:
                LOG.info('sent count=%s partition=%s offset=%s event=%s', sent, metadata.partition, metadata.offset, event)
            if limit and sent >= limit:
                break
            time.sleep(max(0, interval - (time.monotonic() - before)))
    LOG.info('producer_complete sent=%s skipped=%s timestamp_shift_ms=%s', sent, skipped, shift)
    return {'sent': sent, 'skipped': skipped, 'timestamp_shift_ms': shift}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv', default='data/events_sample.csv')
    p.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))
    p.add_argument('--topic', default='retail-rocket-events')
    p.add_argument('--interval', type=float, default=0.15, help='seconds per event, including acknowledgement')
    p.add_argument('--limit', type=int, default=0, help='0 reads all rows')
    p.add_argument('--replay-clock', action='store_true', help='shift all event times by a single constant')
    p.add_argument('--allow-out-of-order', action='store_true', help='intentional lateness experiments only')
    args = p.parse_args()
    if args.interval < 0 or args.limit < 0:
        p.error('interval and limit must be nonnegative')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    producer = make_producer(args.bootstrap)
    try:
        replay(args.csv, producer, args.topic, args.interval, args.limit, args.replay_clock, args.allow_out_of_order)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


if __name__ == '__main__':
    main()
