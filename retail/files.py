"""Read only completed serving epochs. Safe for lightweight demo clients."""
import json
from pathlib import Path


def latest_epoch(root='runtime'):
    epochs = sorted(p for p in (Path(root) / 'intent' / 'epochs').glob('[0-9]*')
                    if p.is_dir() and (p / 'manifest.json').is_file())
    return epochs[-1] if epochs else None


def read_recommendations(root='runtime'):
    epoch = latest_epoch(root)
    if epoch is None:
        return []
    return [json.loads(line) for line in (epoch / 'recommendations.jsonl').read_text().splitlines() if line]
