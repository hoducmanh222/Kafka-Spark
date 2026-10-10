"""Download the public Kaggle events archive, preserving dataset attribution.

If Kaggle requests authentication, download events.csv in your browser instead.
"""
import argparse
import urllib.request
import zipfile
from pathlib import Path

URL = 'https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset?file_name=events.csv'


def download(destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix('.download')
    try:
        with urllib.request.urlopen(URL, timeout=90) as response, temporary.open('wb') as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
        with zipfile.ZipFile(temporary) as z:
            if 'events.csv' not in [Path(p).name for p in z.namelist()]:
                raise ValueError('Response is not the expected Retail Rocket events archive')
        temporary.replace(destination)
        print(f'Download complete: {destination}')
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', default='data/retailrocket_events.csv.zip')
    download(p.parse_args().output)
