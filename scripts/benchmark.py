"""Reproducible single-process benchmark. Numbers are local observations, not promises."""

import argparse
import json
import platform
import tempfile
import time
from pathlib import Path

from ledgerflow import Ledger

parser = argparse.ArgumentParser()
parser.add_argument("--events", type=int, default=10000)
args = parser.parse_args()
if not 1 <= args.events <= 1000000:
    parser.error("--events must be between 1 and 1,000,000")
with tempfile.TemporaryDirectory() as temporary:
    with Ledger(Path(temporary) / "bench.db") as ledger:
        ledger.register_contract(
            "bench", 1, {"fields": {"amount": {"type": "integer", "required": True}}}
        )

        def records(start, stop):
            for i in range(start, stop):
                yield dict(
                    stream="bench",
                    partition=0,
                    offset=i,
                    event_id=f"e-{i}",
                    key=f"k-{i // 4}",
                    revision=i % 4 + 1,
                    effective_at=f"2026-01-0{i % 4 + 1}T00:00:00Z",
                    schema_version=1,
                    op="upsert",
                    data={"amount": i},
                )

        started = time.perf_counter()
        for start in range(0, args.events, 1000):
            ledger.ingest(records(start, min(start + 1000, args.events)))
        elapsed = time.perf_counter() - started
        report = {
            "events": args.events,
            "batch_size": 1000,
            "elapsed_seconds": round(elapsed, 4),
            "events_per_second": round(args.events / elapsed),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "audit": ledger.audit(),
        }
        print(json.dumps(report, indent=2))
        if not report["audit"]["ok"]:
            raise SystemExit(1)
