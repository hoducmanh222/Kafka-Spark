"""Print current recommendations, batch metadata and physical Parquet schemas."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.files import latest_epoch, read_recommendations


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', default='runtime')
    p.add_argument('--parquet', action='store_true')
    a = p.parse_args()
    epoch = latest_epoch(a.root)
    if epoch is None:
        raise SystemExit('No committed intent output yet. Check spark logs.')
    print((epoch / 'manifest.json').read_text())
    print(json.dumps(read_recommendations(a.root), indent=2))
    if a.parquet:
        import pyarrow.dataset as ds
        for path in [Path(a.root) / 'curated', epoch / 'state', epoch / 'recommendations']:
            data = ds.dataset(str(path), format='parquet', partitioning='hive', exclude_invalid_files=True)
            print(path, 'rows=', data.count_rows(), 'schema=', data.schema)
