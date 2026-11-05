import copy
import tempfile
import unittest
from pathlib import Path

from ledgerflow import Ledger, PipelineError
from ledgerflow.contracts import ContractError, timestamp

SPEC = {"fields": {"value": {"type": "integer", "required": True, "minimum": 0}}}


def event(offset=0, **changes):
    value = dict(
        stream="orders",
        partition=0,
        offset=offset,
        event_id=f"evt-{offset}",
        key="key-1",
        revision=offset + 1,
        effective_at=f"2026-01-{offset + 1:02}T00:00:00Z",
        schema_version=1,
        op="upsert",
        data={"value": offset},
    )
    value.update(changes)
    return value


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = Ledger(self.path)
        self.addCleanup(self.ledger.close)
        self.ledger.register_contract("orders", 1, SPEC)

    def test_initial_ingest_and_restart(self):
        self.assertEqual(self.ledger.ingest([event()])["accepted"], 1)
        with Ledger(self.path) as other:
            self.assertEqual(other.current("orders")[0]["data"], {"value": 0})
            self.assertTrue(other.audit()["ok"])

    def test_replay_does_not_change_state(self):
        self.ledger.ingest([event()])
        self.assertEqual(self.ledger.ingest([event()])["replays"], 1)
        self.assertEqual(self.ledger.status()["events"], 1)

    def test_new_transport_offset_same_identity_is_duplicate(self):
        first = event()
        result = self.ledger.ingest([first, {**first, "offset": 1}])
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(self.ledger.status()["checkpoints"][0]["last_offset"], 1)

    def test_gap_rolls_back_prior_record(self):
        with self.assertRaisesRegex(PipelineError, "NON_CONTIGUOUS_OFFSET"):
            self.ledger.ingest([event(), event(2)])
        self.assertEqual(self.ledger.status()["records"], 0)

    def test_invalid_outer_record_rolls_back(self):
        with self.assertRaisesRegex(PipelineError, "INVALID_SEQUENCE"):
            self.ledger.ingest([event(), {**event(1), "offset": True}])
        self.assertEqual(self.ledger.status()["records"], 0)

    def test_contract_violation_quarantines_and_advances(self):
        result = self.ledger.ingest([event(data={"value": -1}), event(1)])
        self.assertEqual(result["quarantined"], 1)
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(self.ledger.status()["checkpoints"][0]["last_offset"], 1)

    def test_missing_contract_is_quarantine(self):
        self.assertEqual(
            self.ledger.ingest([event(schema_version=2)])["quarantined"], 1
        )
        self.assertEqual(
            self.ledger.status()["quarantine_reasons"], {"UNKNOWN_SCHEMA": 1}
        )

    def test_unknown_field_missing_field_wrong_type_and_null(self):
        bad = [{"value": 1, "secret": "no"}, {}, {"value": "1"}, {"value": None}]
        result = self.ledger.ingest([event(i, data=data) for i, data in enumerate(bad)])
        self.assertEqual(result["quarantined"], 4)

    def test_additive_optional_schema(self):
        spec = copy.deepcopy(SPEC)
        spec["fields"]["tag"] = {"type": "string", "max_length": 8}
        self.ledger.register_contract("orders", 2, spec)
        self.assertEqual(self.ledger.ingest([event(schema_version=2)])["accepted"], 1)

    def test_breaking_schema_rejected(self):
        for spec in (
            {"fields": {"other": {"type": "string"}}},
            {"fields": {"value": {"type": "number"}}},
            {"fields": {**SPEC["fields"], "new": {"type": "string", "required": True}}},
        ):
            with (
                self.subTest(spec=spec),
                self.assertRaisesRegex(PipelineError, "BREAKING_SCHEMA_CHANGE"),
            ):
                self.ledger.register_contract("orders", 2, spec)

    def test_contract_version_immutable_and_contiguous(self):
        self.ledger.register_contract("orders", 1, SPEC)
        with self.assertRaisesRegex(PipelineError, "IMMUTABLE_CONTRACT"):
            self.ledger.register_contract(
                "orders", 1, {"fields": {"x": {"type": "string"}}}
            )
        with self.assertRaisesRegex(PipelineError, "NON_CONTIGUOUS_CONTRACT_VERSION"):
            self.ledger.register_contract("orders", 3, SPEC)

    def test_conflicting_event_id_quarantined(self):
        self.ledger.ingest([event(), event(1, event_id="evt-0")])
        self.assertEqual(
            self.ledger.status()["quarantine_reasons"], {"EVENT_ID_CONFLICT": 1}
        )

    def test_conflicting_revision_quarantined(self):
        self.ledger.ingest([event(), event(1, revision=1)])
        self.assertEqual(
            self.ledger.status()["quarantine_reasons"], {"REVISION_CONFLICT": 1}
        )

    def test_late_event_restores_history(self):
        self.ledger.ingest(
            [
                event(),
                event(1, effective_at="2026-01-03T00:00:00Z", revision=3),
                event(2, effective_at="2026-01-02T00:00:00Z", revision=2),
            ]
        )
        history = self.ledger.history("orders")
        self.assertEqual([row["revision"] for row in history], [1, 2, 3])
        self.assertEqual(history[0]["valid_to"], history[1]["valid_from"])
        self.assertEqual(history[1]["valid_to"], history[2]["valid_from"])
        self.assertIsNone(history[2]["valid_to"])

    def test_same_time_wins_highest_revision(self):
        self.ledger.ingest([event(), event(1, effective_at=event()["effective_at"])])
        self.assertEqual(len(self.ledger.history("orders")), 1)
        self.assertEqual(self.ledger.current("orders")[0]["revision"], 2)
        self.assertEqual(self.ledger.status()["events"], 2)

    def test_delete_and_resurrection(self):
        self.ledger.ingest([event(), event(1, op="delete", data={})])
        self.assertEqual(self.ledger.current("orders"), [])
        self.ledger.ingest([event(2)])
        self.assertEqual(len(self.ledger.current("orders")), 1)
        self.assertEqual(len(self.ledger.history("orders")), 3)

    def test_isolation_by_stream_and_key(self):
        self.ledger.register_contract("other", 1, SPEC)
        self.ledger.ingest([event(), event(stream="other"), event(1, key="key-2")])
        self.assertEqual(len(self.ledger.history("orders", "key-1")), 1)
        self.assertEqual(len(self.ledger.current("orders")), 2)
        self.assertEqual(len(self.ledger.current("other")), 1)

    def test_audit_detects_corruption(self):
        self.ledger.ingest([event()])
        self.ledger.db.execute("UPDATE events SET data='{}'")
        self.assertIn("EVENT_HASH_MISMATCH", self.ledger.audit()["issues"])
        self.ledger.db.execute("UPDATE checkpoints SET last_offset=42")
        self.assertIn("CHECKPOINT_DIVERGENCE", self.ledger.audit()["issues"])

    def test_query_parameters_prevent_injection(self):
        self.ledger.ingest([event(key="x'; DROP TABLE events; --")])
        self.assertEqual(
            len(self.ledger.history("orders", "x'; DROP TABLE events; --")), 1
        )
        self.assertEqual(self.ledger.status()["events"], 1)

    def test_timezone_normalization(self):
        self.assertEqual(
            timestamp("2026-01-01T01:00:00+01:00"), "2026-01-01T00:00:00.000000Z"
        )
        with self.assertRaisesRegex(ContractError, "INVALID_TIMESTAMP"):
            timestamp("2026-01-01T00:00:00")

    def test_inapplicable_constraints_and_incompatible_enum_rejected(self):
        for rule in (
            {"type": "string", "minimum": 1},
            {"type": "number", "max_length": 10},
            {"type": "integer", "enum": [True]},
            {"type": "string", "enum": [None]},
        ):
            with self.subTest(rule=rule), self.assertRaises(ContractError):
                self.ledger.register_contract("invalid", 1, {"fields": {"value": rule}})


if __name__ == "__main__":
    unittest.main()
