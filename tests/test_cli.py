import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ledgerflow.cli import main, read_events
from ledgerflow.engine import PipelineError
from test_engine import SPEC, event


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "ledger.db"
        self.contract = self.root / "contract.json"
        self.contract.write_text(json.dumps(SPEC))

    def run_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(["--db", str(self.db), *map(str, args)])
        return code, stdout.getvalue(), stderr.getvalue()

    def initialize(self):
        self.assertEqual(
            self.run_cli(
                "register", "--stream", "orders", "--version", "1", self.contract
            )[0],
            0,
        )

    def test_all_success_commands(self):
        self.initialize()
        source = self.root / "events.jsonl"
        source.write_text(json.dumps(event()) + "\n")
        self.assertEqual(self.run_cli("ingest", source)[0], 0)
        for args in [
            ("status",),
            ("audit",),
            ("current", "--stream", "orders"),
            ("history", "--stream", "orders", "--key", "key-1"),
        ]:
            code, output, errors = self.run_cli(*args)
            self.assertEqual(code, 0, errors)
            json.loads(output)
        code, output, _ = self.run_cli("metrics")
        self.assertEqual(code, 0)
        self.assertIn('ledgerflow_records_total{outcome="accepted"} 1', output)
        snapshot = self.root / "snapshot"
        self.assertEqual(self.run_cli("export", "--stream", "orders", snapshot)[0], 0)
        self.assertEqual(self.run_cli("verify", snapshot)[0], 0)
        (snapshot / "history.jsonl").write_text("{}\n")
        self.assertEqual(self.run_cli("verify", snapshot)[0], 3)

    def test_stdin_ingestion(self):
        self.initialize()
        with patch("sys.stdin", io.StringIO(json.dumps(event()) + "\n")):
            self.assertEqual(self.run_cli("ingest", "-")[0], 0)

    def test_missing_database_read_is_not_empty_success(self):
        code, _, errors = self.run_cli("audit")
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(errors)["error"], "DATABASE_NOT_FOUND")
        self.assertFalse(self.db.exists())

    def test_io_error_does_not_expose_path(self):
        code, _, errors = self.run_cli(
            "register",
            "--stream",
            "orders",
            "--version",
            "1",
            self.root / "private-name",
        )
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(errors), {"error": "IO_ERROR"})
        self.assertNotIn("private-name", errors)

    def test_invalid_contract_and_oversize(self):
        self.contract.write_text("{bad")
        self.assertEqual(
            self.run_cli(
                "register", "--stream", "orders", "--version", "1", self.contract
            )[0],
            2,
        )
        self.contract.write_text(" " * 65537)
        self.assertIn(
            "CONTRACT_TOO_LARGE",
            self.run_cli(
                "register", "--stream", "orders", "--version", "1", self.contract
            )[2],
        )

    def test_blank_and_oversize_line(self):
        for text, code in [("\n", "EMPTY_RECORD"), ("x" * 1048577, "EVENT_TOO_LARGE")]:
            with self.subTest(code=code), self.assertRaisesRegex(PipelineError, code):
                list(read_events(io.StringIO(text)))

    def test_storage_error_is_sanitized(self):
        self.db.write_bytes(b"not a database")
        code, _, errors = self.run_cli("status")
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(errors), {"error": "STORAGE_ERROR"})
