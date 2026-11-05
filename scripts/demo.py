"""Run a complete demonstration in an isolated temporary directory."""

import json
import tempfile
from pathlib import Path

from ledgerflow import Ledger
from ledgerflow.cli import read_events
from ledgerflow.snapshot import export_snapshot, verify_snapshot

root = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="ledgerflow-demo-") as temporary:
    path = Path(temporary)
    with Ledger(path / "ledger.db") as ledger:
        ledger.register_contract(
            "orders",
            1,
            json.loads((root / "examples/orders.contract.v1.json").read_text()),
        )
        with (root / "examples/orders.jsonl").open() as source:
            print("First ingestion:", ledger.ingest(read_events(source)))
        with (root / "examples/orders.jsonl").open() as source:
            print("Replay:", ledger.ingest(read_events(source)))
        print("History:", json.dumps(ledger.history("orders"), indent=2))
        print("Status:", ledger.status())
        print("Audit:", ledger.audit())
        export_snapshot(ledger, "orders", path / "snapshot")
        print("Snapshot verification:", verify_snapshot(path / "snapshot"))
