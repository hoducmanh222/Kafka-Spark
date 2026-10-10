"""Read only files committed by Spark's native streaming Parquet sink."""
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow.dataset as ds


def committed_audit(root='runtime'):
    audit = (Path(root) / 'curated').resolve()
    metadata = audit / '_spark_metadata'
    logs = []
    for path in metadata.glob('*'):
        if path.is_file() and (path.name.isdigit() or
                               (path.name.endswith('.compact') and path.stem.isdigit())):
            logs.append((int(path.name.split('.')[0]), path.name.endswith('.compact'), path))
    if not logs:
        return None
    compact = max((batch for batch, is_compact, _ in logs if is_compact), default=-1)
    files = {}
    for batch, is_compact, path in sorted(logs):
        if batch < compact or (batch == compact and not is_compact):
            continue
        lines = path.read_text(encoding='utf-8').splitlines()
        if not lines or lines[0] != 'v1':
            raise ValueError(f'Unsupported Spark file-sink metadata version: {path}')
        for line in lines[1:]:
            record = json.loads(line)
            parsed = urlparse(record['path'])
            value = unquote(parsed.path) if parsed.scheme == 'file' else record['path']
            if os.name == 'nt' and len(value) > 2 and value[0] == '/' and value[2] == ':':
                value = value[1:]
            file = Path(value).resolve()
            if not file.is_relative_to(audit):
                raise ValueError('Spark audit metadata points outside its output directory')
            if record.get('action', 'add') == 'delete':
                files.pop(str(file), None)
            elif not record.get('isDir', False):
                files[str(file)] = file
    if not files:
        return None
    return ds.dataset(list(files), format='parquet', partitioning='hive', partition_base_dir=str(audit))
