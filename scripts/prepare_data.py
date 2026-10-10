"""Sort full events.csv using disk-backed SQLite, without loading it into RAM."""
import argparse
import csv
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.domain import csv_stream, normalize_event


def prepare(source, destination, limit=0):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError('Use a separate output path to preserve the source')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='retail-sort-', dir=destination.parent) as tmp:
        db = sqlite3.connect(str(Path(tmp) / 'events.sqlite'))
        try:
            db.execute('CREATE TABLE events (seq INTEGER PRIMARY KEY, timestamp INTEGER, visitorid INTEGER, event TEXT, itemid INTEGER, transactionid INTEGER)')
            count = 0
            with csv_stream(source) as rows:
                buffer = []
                for row in rows:
                    e = normalize_event(row)
                    buffer.append(tuple(e[k] for k in ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']))
                    count += 1
                    if len(buffer) == 10000:
                        db.executemany('INSERT INTO events(timestamp,visitorid,event,itemid,transactionid) VALUES(?,?,?,?,?)', buffer)
                        buffer.clear()
                if buffer:
                    db.executemany('INSERT INTO events(timestamp,visitorid,event,itemid,transactionid) VALUES(?,?,?,?,?)', buffer)
            db.commit()
            query = 'SELECT timestamp,visitorid,event,itemid,transactionid FROM events ORDER BY timestamp,seq'
            if limit:
                query += ' LIMIT ' + str(int(limit))
            temp_out = Path(tmp) / 'events.csv'
            with temp_out.open('w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'visitorid', 'event', 'itemid', 'transactionid'])
                writer.writerows(db.execute(query))
            temp_out.replace(destination)
            print(f'Validated {count} source events. Saved sorted CSV: {destination}')
        finally:
            db.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('source')
    p.add_argument('destination')
    p.add_argument('--limit', type=int, default=0)
    a = p.parse_args()
    if a.limit < 0:
        p.error('limit must be nonnegative')
    prepare(a.source, a.destination, a.limit)
