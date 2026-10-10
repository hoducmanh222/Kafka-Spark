"""Input validation and event-time arithmetic shared by producer and tests."""
import csv
import io
import zipfile
from contextlib import contextmanager
from pathlib import Path

WEIGHTS = {'view': 1, 'addtocart': 5, 'transaction': 10}
WINDOW_MS = 600_000
SLIDE_MS = 60_000
WATERMARK_MS = 300_000


def integer(value, name, nullable=False):
    if nullable and (value is None or str(value).strip().lower() in ('', 'null', 'nan')):
        return None
    if isinstance(value, bool):
        raise ValueError(f'{name} must be an integer')
    text = str(value).strip()
    try:
        number = int(text)
    except (ValueError, TypeError) as exc:
        raise ValueError(f'{name} must be an integer: {value!r}') from exc
    if number < 0:
        raise ValueError(f'{name} must be nonnegative')
    return number


def normalize_event(row):
    event = str(row.get('event', '')).strip()
    if event not in WEIGHTS:
        raise ValueError(f'unsupported event: {event!r}')
    tx = integer(row.get('transactionid'), 'transactionid', nullable=True)
    if (event == 'transaction') != (tx is not None):
        raise ValueError('transactionid must be populated only for a transaction')
    timestamp = integer(row.get('timestamp'), 'timestamp')
    if not 946684800000 <= timestamp < 4102444800000:
        raise ValueError('timestamp must be epoch milliseconds between 2000 and 2100')
    return {'timestamp': timestamp,
            'visitorid': integer(row.get('visitorid'), 'visitorid'),
            'event': event, 'itemid': integer(row.get('itemid'), 'itemid'),
            'transactionid': tx}


def active_window_start(event_max_ms):
    """Earliest still-active 10-minute window aligned to one-minute boundaries."""
    return (event_max_ms // SLIDE_MS) * SLIDE_MS - WINDOW_MS + SLIDE_MS


def windows_for(timestamp):
    latest = (timestamp // SLIDE_MS) * SLIDE_MS
    return [(latest - i * SLIDE_MS, latest - i * SLIDE_MS + WINDOW_MS)
            for i in range(WINDOW_MS // SLIDE_MS)]


@contextmanager
def csv_stream(path):
    path = Path(path)
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as archive:
            files = [n for n in archive.namelist() if Path(n).name == 'events.csv']
            if len(files) != 1:
                raise ValueError('ZIP must contain exactly one events.csv')
            with archive.open(files[0]) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig') as f:
                yield csv.DictReader(f)
    else:
        with path.open(encoding='utf-8-sig', newline='') as f:
            yield csv.DictReader(f)
