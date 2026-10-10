"""Prepare the COMPLETE Retail Rocket input before the default producer starts."""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.download_data import download
from scripts.prepare_data import prepare

EXPECTED_ROWS = 2_756_101
FIELDS = ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']


def verify_complete(path, expected_rows=EXPECTED_ROWS):
    with Path(path).open('r', encoding='utf-8', newline='') as stream:
        reader = csv.reader(stream)
        if next(reader, None) != FIELDS:
            raise ValueError('Incorrect Retail Rocket CSV header')
        count = sum(1 for _ in reader)
    if count != expected_rows:
        raise ValueError(f'Full dataset required: found {count:,} rows, expected {expected_rows:,}. '
                         'The default demonstration does not accept the small sample.')
    print(f'FULL DATASET READY: {count:,} events in {path}', flush=True)
    return count


def ensure_data(directory='data'):
    directory = Path(directory)
    target = directory / 'events.csv'
    archive = directory / 'retailrocket_events.csv.zip'
    if not target.exists():
        if not archive.exists():
            download(archive)
        prepare(archive, target)
    verify_complete(target)


if __name__ == '__main__':
    ensure_data()
