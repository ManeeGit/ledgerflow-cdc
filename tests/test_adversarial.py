"""Independent adversarial tests for transactional and event-time invariants."""

import concurrent.futures
import copy
import contextlib
import io
import json
from unittest.mock import patch
import tempfile
import unittest
from pathlib import Path

from ledgerflow.contracts import ContractError, validate_contract
from ledgerflow.engine import Ledger, PipelineError


SPEC = {"fields": {"value": {"type": "number", "required": True}}}


def event(offset=0, **changes):
    value = dict(
        stream="accounts",
        partition=0,
        offset=offset,
        event_id=f"event-{offset}",
        key="account-a",
        revision=offset + 1,
        effective_at=f"2026-01-{offset + 1:02d}T00:00:00Z",
        schema_version=1,
        op="upsert",
        data={"value": offset},
    )
    value.update(changes)
    return value


class AdversarialTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "ledger.db"
        self.ledger = Ledger(self.path)
        self.addCleanup(self.ledger.close)
        self.ledger.register_contract("accounts", 1, SPEC)

    def test_changed_replay_rolls_back_entire_batch(self):
        self.ledger.ingest([event()])
        with self.assertRaisesRegex(PipelineError, "OFFSET_CONTENT_CONFLICT"):
            self.ledger.ingest([event(1), event(data={"value": 900})])
        self.assertEqual(self.ledger.status()["records"], 1)
        self.assertEqual(len(self.ledger.history("accounts")), 1)
        self.assertTrue(self.ledger.audit()["ok"])

    def test_iterable_failure_rolls_back(self):
        def failing():
            yield event()
            raise RuntimeError("source disconnected")

        with self.assertRaisesRegex(RuntimeError, "source disconnected"):
            self.ledger.ingest(failing())
        self.assertEqual(self.ledger.status()["records"], 0)
        self.assertEqual(self.ledger.ingest([event()])["accepted"], 1)

    def test_batch_limit_rolls_back(self):
        with self.assertRaisesRegex(PipelineError, "BATCH_TOO_LARGE"):
            self.ledger.ingest([event(), event(1)], max_batch=1)
        self.assertEqual(self.ledger.status()["records"], 0)

    def test_each_partition_starts_at_zero(self):
        with self.assertRaisesRegex(PipelineError, "NON_CONTIGUOUS_OFFSET"):
            self.ledger.ingest([event(), event(1, partition=1)])
        self.assertEqual(self.ledger.status()["records"], 0)

    def test_payload_free_quarantine_survives_reopen(self):
        secret = "DO-NOT-PERSIST-THIS-PRIVATE-PAYLOAD"
        self.ledger.ingest([event(data={"unexpected": secret}), event(1)])
        with Ledger(self.path) as reopened:
            self.assertEqual(
                reopened.status()["quarantine_reasons"], {"UNKNOWN_FIELD": 1}
            )
            self.assertEqual(
                reopened.ingest([event(data={"unexpected": secret})])["replays"], 1
            )
            dump = "\n".join(reopened.db.iterdump())
            self.assertNotIn(secret, dump)
            self.assertEqual(reopened.status()["checkpoints"][0]["last_offset"], 1)

    def test_normalized_time_deduplicates_across_partitions(self):
        original = event(effective_at="2026-01-01T00:00:00Z")
        equivalent = dict(
            original, partition=1, effective_at="2025-12-31T19:00:00-05:00"
        )
        self.assertEqual(self.ledger.ingest([original, equivalent])["duplicates"], 1)
        self.assertEqual(self.ledger.status()["events"], 1)

    def test_raw_replay_requires_identical_timestamp_representation(self):
        self.ledger.ingest([event()])
        with self.assertRaisesRegex(PipelineError, "OFFSET_CONTENT_CONFLICT"):
            self.ledger.ingest([event(effective_at="2026-01-01T00:00:00+00:00")])

    def test_late_events_rebuild_intervals_and_ties_use_revision(self):
        self.ledger.ingest(
            [
                event(0, revision=3, effective_at="2026-03-01T00:00:00Z"),
                event(1, revision=1, effective_at="2026-01-01T00:00:00Z"),
                event(2, revision=2, effective_at="2026-02-01T00:00:00Z"),
                event(3, revision=4, effective_at="2026-02-01T00:00:00Z"),
            ]
        )
        history = self.ledger.history("accounts")
        self.assertEqual([r["revision"] for r in history], [1, 4, 3])
        self.assertEqual(
            [r["valid_to"] for r in history[:-1]],
            [r["valid_from"] for r in history[1:]],
        )
        self.assertEqual(self.ledger.current("accounts")[0]["revision"], 3)

    def test_delete_and_late_prior_event_do_not_resurrect(self):
        self.ledger.ingest(
            [
                event(),
                event(1, op="delete", data={}),
                event(2, effective_at="2026-01-01T12:00:00Z"),
            ]
        )
        self.assertEqual(self.ledger.current("accounts"), [])
        self.ledger.ingest([event(3)])
        self.assertEqual(len(self.ledger.current("accounts")), 1)

    def test_event_and_revision_conflicts_quarantined(self):
        result = self.ledger.ingest(
            [event(), event(1, event_id="event-0"), event(2, revision=1)]
        )
        self.assertEqual(result["quarantined"], 2)
        self.assertEqual(
            self.ledger.status()["quarantine_reasons"],
            {"EVENT_ID_CONFLICT": 1, "REVISION_CONFLICT": 1},
        )

    def test_boolean_not_accepted_as_number(self):
        result = self.ledger.ingest([event(data={"value": True})])
        self.assertEqual(result["quarantined"], 1)

    def test_huge_numeric_value_quarantined_without_aborting_batch(self):
        result = self.ledger.ingest([event(data={"value": 10**400}), event(1)])
        self.assertEqual(result["quarantined"], 1)
        self.assertEqual(result["accepted"], 1)

    def test_malformed_contract_type_is_safe_validation_error(self):
        with self.assertRaises(ContractError):
            validate_contract({"fields": {"value": {"type": []}}})

    def test_unrepresentable_contract_bound_is_safe_validation_error(self):
        with self.assertRaises(ContractError):
            validate_contract(
                {"fields": {"value": {"type": "number", "maximum": 10**400}}}
            )

    def test_schema_registration_copies_and_locks_contract(self):
        spec = copy.deepcopy(SPEC)
        spec["fields"]["optional"] = {"type": "string"}
        self.ledger.register_contract("accounts", 2, spec)
        spec["fields"]["optional"]["required"] = True
        with self.assertRaisesRegex(PipelineError, "IMMUTABLE_CONTRACT"):
            self.ledger.register_contract("accounts", 2, spec)
        self.assertTrue(self.ledger.audit()["ok"])

    def test_concurrent_identical_ingestions_commit_once(self):
        def ingest_once(_):
            with Ledger(self.path, timeout=15) as ledger:
                return ledger.ingest([event()])

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(ingest_once, range(8)))
        self.assertEqual(sum(r["accepted"] for r in results), 1)
        self.assertEqual(sum(r["replays"] for r in results), 7)
        self.assertTrue(self.ledger.audit()["ok"])

    def test_snapshot_manifest_and_tamper_detection(self):
        from ledgerflow.snapshot import export_snapshot, verify_snapshot

        self.ledger.ingest([event(), event(1)])
        destination = Path(self.tmp.name) / "snapshot"
        manifest = export_snapshot(self.ledger, "accounts", destination)
        self.assertEqual(manifest["files"]["history.jsonl"]["rows"], 2)
        self.assertTrue(verify_snapshot(destination)["ok"])
        with (destination / "current.jsonl").open("ab") as output:
            output.write(b"{}\n")
        self.assertIn(
            "CHECKSUM_MISMATCH:current.jsonl", verify_snapshot(destination)["issues"]
        )

    def test_snapshot_does_not_overwrite_existing_directory(self):
        from ledgerflow.snapshot import export_snapshot

        destination = Path(self.tmp.name) / "snapshot"
        destination.mkdir()
        sentinel = destination / "keep.txt"
        sentinel.write_text("keep")
        with self.assertRaisesRegex(PipelineError, "SNAPSHOT_ALREADY_EXISTS"):
            export_snapshot(self.ledger, "accounts", destination)
        self.assertEqual(sentinel.read_text(), "keep")

    def test_snapshot_lock_cleaned_after_temporary_directory_failure(self):
        from ledgerflow.snapshot import export_snapshot

        destination = Path(self.tmp.name) / "snapshot"
        with patch(
            "ledgerflow.snapshot.tempfile.mkdtemp", side_effect=OSError("disk full")
        ):
            with self.assertRaises(OSError):
                export_snapshot(self.ledger, "accounts", destination)
        self.assertFalse(destination.with_name("snapshot.lock").exists())
        self.assertFalse(destination.exists())

    def test_snapshot_fsync_failure_cleans_staging_and_lock(self):
        from ledgerflow.snapshot import export_snapshot

        destination = Path(self.tmp.name) / "snapshot"
        with patch("ledgerflow.snapshot.os.fsync", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                export_snapshot(self.ledger, "accounts", destination)
        self.assertFalse(destination.exists())
        self.assertFalse(destination.with_name("snapshot.lock").exists())
        self.assertEqual(list(Path(self.tmp.name).glob(".snapshot-*")), [])

    def test_snapshot_rejects_symlink_and_boolean_version(self):
        from ledgerflow.snapshot import export_snapshot, verify_snapshot

        destination = Path(self.tmp.name) / "snapshot"
        export_snapshot(self.ledger, "accounts", destination)
        path = destination / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["format_version"] = True
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(PipelineError, "INVALID_MANIFEST"):
            verify_snapshot(destination)
        manifest["format_version"] = 1
        path.write_text(json.dumps(manifest))
        data = destination / "current.jsonl"
        data.unlink()
        data.symlink_to(destination / "history.jsonl")
        self.assertIn(
            "MISSING_OR_UNSAFE_FILE:current.jsonl",
            verify_snapshot(destination)["issues"],
        )

    def test_cli_duplicate_keys_and_nan_rejected_without_payload_echo(self):
        from ledgerflow.cli import main

        for payload in (
            '{"secret":"PRIVATE-SENTINEL", "secret":2}\n',
            '{"value":NaN}\n',
        ):
            with self.subTest(payload=payload):
                stderr = io.StringIO()
                with (
                    patch("sys.stdin", io.StringIO(payload)),
                    contextlib.redirect_stderr(stderr),
                ):
                    code = main(["--db", str(self.path), "ingest", "-"])
                self.assertEqual(code, 2)
                self.assertNotIn("PRIVATE-SENTINEL", stderr.getvalue())
                self.assertEqual(self.ledger.status()["records"], 0)

    def test_cli_invalid_second_record_rolls_back_first(self):
        from ledgerflow.cli import main

        stderr = io.StringIO()
        payload = json.dumps(event()) + "\n{invalid-secret-payload\n"
        with (
            patch("sys.stdin", io.StringIO(payload)),
            contextlib.redirect_stderr(stderr),
        ):
            code = main(["--db", str(self.path), "ingest", "-"])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stderr.getvalue()), {"error": "INVALID_JSON"})
        self.assertEqual(self.ledger.status()["records"], 0)

    def test_snapshot_stays_consistent_during_concurrent_writer(self):
        from ledgerflow import snapshot

        self.ledger.ingest([event()])
        destination = Path(self.tmp.name) / "snapshot"
        original = snapshot.canonical
        mutated = False

        def concurrent_write(value):
            nonlocal mutated
            if not mutated:
                mutated = True
                with Ledger(self.path) as other:
                    other.ingest([event(1)])
            return original(value)

        with patch("ledgerflow.snapshot.canonical", side_effect=concurrent_write):
            snapshot.export_snapshot(self.ledger, "accounts", destination)
        checkpoints = [
            json.loads(line)
            for line in (destination / "checkpoints.jsonl").read_text().splitlines()
        ]
        self.assertEqual(checkpoints[0]["last_offset"], 0)
        self.assertEqual(self.ledger.status()["records"], 2)
        self.assertTrue(snapshot.verify_snapshot(destination)["ok"])

    def test_audit_detects_modified_event_and_checkpoint(self):
        self.ledger.ingest([event()])
        self.ledger.db.execute("UPDATE events SET data = ?", ('{"value":999}',))
        self.ledger.db.execute("UPDATE checkpoints SET last_offset = 42")
        result = self.ledger.audit()
        self.assertFalse(result["ok"])
        self.assertIn("EVENT_HASH_MISMATCH", result["issues"])
        self.assertIn("CHECKPOINT_DIVERGENCE", result["issues"])


if __name__ == "__main__":
    unittest.main()
