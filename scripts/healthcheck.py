"""Reject stale readiness files left behind by a previous driver."""
import json
import time
from pathlib import Path

p = Path('/app/runtime/ready.json')
assert p.exists(), 'driver has not started all queries'
assert time.time() - json.loads(p.read_text())['heartbeat'] < 30, 'driver heartbeat is stale'
