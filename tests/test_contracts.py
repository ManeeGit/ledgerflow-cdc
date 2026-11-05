import unittest

from ledgerflow.contracts import (
    ContractError,
    validate_contract,
    validate_data,
    validate_envelope,
)
from test_engine import event


class ContractsTests(unittest.TestCase):
    def test_invalid_rule_shapes(self):
        invalid = [
            None,
            {},
            {"fields": []},
            {"fields": {}},
            {"fields": {"x": []}},
            {"fields": {"bad space": {"type": "string"}}},
        ]
        invalid += [
            {"fields": {"x": rule}}
            for rule in [
                {"type": "string", "surprise": True},
                {"type": "nope"},
                {"type": "string", "required": 1},
                {"type": "string", "enum": []},
                {"type": "number", "minimum": True},
                {"type": "number", "minimum": 2, "maximum": 1},
                {"type": "string", "max_length": 0},
            ]
        ]
        for spec in invalid:
            with self.subTest(spec=spec), self.assertRaises(ContractError):
                validate_contract(spec)

    def test_valid_optional_nullable_types(self):
        for kind, value in [
            ("string", "x"),
            ("integer", 2),
            ("number", 2.5),
            ("boolean", True),
            ("timestamp", "2026-01-01T00:00:00Z"),
        ]:
            spec = {"fields": {"x": {"type": kind, "nullable": True}}}
            validate_contract(spec)
            validate_data({"x": value}, spec)
            validate_data({"x": None}, spec)
            validate_data({}, spec)

    def test_length_bounds_and_enum_enforced(self):
        for rule, value in [
            ({"type": "string", "max_length": 2}, "abc"),
            ({"type": "string", "enum": ["x"]}, "y"),
            ({"type": "number", "maximum": 2}, 3),
            ({"type": "timestamp"}, "not-a-time"),
        ]:
            with self.subTest(rule=rule), self.assertRaises(ContractError):
                validate_data({"x": value}, {"fields": {"x": rule}})

    def test_envelope_failures(self):
        malformed = [
            {},
            event(stream="bad space"),
            event(key=""),
            event(op="unknown"),
            event(op="delete"),
            event(data=[]),
            event(data={1: "x"}),
            event(data={"x": float("nan")}),
            event(effective_at="2026-01-01"),
            event(data={"x": "a" * 1048576}),
        ]
        for row in malformed:
            with self.subTest(fields=list(row)), self.assertRaises(ContractError):
                validate_envelope(row)
