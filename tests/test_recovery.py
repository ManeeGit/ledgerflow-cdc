"""Actual process crash recovery and randomized ordering properties."""

import json
import os
import random
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ledgerflow import Ledger
from test_engine import SPEC, event


class RecoveryTests(unittest.TestCase):
    def test_abrupt_process_exit_leaves_no_partial_checkpoint_or_event(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ledger.db"
            with Ledger(path) as ledger:
                ledger.register_contract("orders", 1, SPEC)
            code = """
import json, os, sys
from ledgerflow import Ledger
with Ledger(sys.argv[1]) as ledger:
    def records():
        yield json.loads(sys.argv[2])
        os._exit(17)
    ledger.ingest(records())
"""
            completed = subprocess.run(
                [sys.executable, "-c", code, str(path), json.dumps(event())],
                capture_output=True,
            )
            self.assertEqual(completed.returncode, 17, completed.stderr.decode())
            with Ledger(path) as recovered:
                self.assertEqual(recovered.status()["records"], 0)
                self.assertEqual(recovered.status()["checkpoints"], [])
                self.assertTrue(recovered.audit()["ok"])
                self.assertEqual(recovered.ingest([event()])["accepted"], 1)

    def test_arrival_order_does_not_change_event_time_history(self):
        baseline = None
        for seed in range(20):
            versions = list(range(20))
            random.Random(seed).shuffle(versions)
            with Ledger(":memory:") as ledger:
                ledger.register_contract("orders", 1, SPEC)
                ledger.ingest(
                    [
                        {**event(version), "offset": offset}
                        for offset, version in enumerate(versions)
                    ]
                )
                history = ledger.history("orders")
                if baseline is None:
                    baseline = history
                else:
                    self.assertEqual(history, baseline)

    @unittest.skipIf(os.name == "nt", "POSIX permission semantics")
    def test_new_database_is_owner_private(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ledger.db"
            with Ledger(path):
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
