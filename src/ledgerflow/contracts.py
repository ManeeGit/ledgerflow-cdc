"""Strict data contracts and transport envelope validation, without coercion."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Any

TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
MAX_INTEGER = 2**63 - 1


class ContractError(ValueError):
    """Contains a safe machine code, never an input value."""


def canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def finite(value: Any) -> bool:
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False


def integer(value: Any, minimum: int = 0) -> bool:
    return type(value) is int and minimum <= value <= MAX_INTEGER


def timestamp(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 40:
        raise ContractError("INVALID_TIMESTAMP")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or "T" not in value:
            raise ValueError
        return (
            parsed.astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
    except (ValueError, OverflowError):
        raise ContractError("INVALID_TIMESTAMP") from None


def validate_contract(spec: Any) -> None:
    if not isinstance(spec, dict) or set(spec) != {"fields"}:
        raise ContractError("INVALID_CONTRACT")
    fields = spec["fields"]
    if not isinstance(fields, dict) or not fields or len(fields) > 100:
        raise ContractError("INVALID_CONTRACT")
    for name, rule in fields.items():
        if (
            not isinstance(name, str)
            or not TOKEN.fullmatch(name)
            or not isinstance(rule, dict)
        ):
            raise ContractError("INVALID_FIELD_RULE")
        if set(rule) - {
            "type",
            "required",
            "nullable",
            "enum",
            "minimum",
            "maximum",
            "max_length",
        }:
            raise ContractError("UNKNOWN_FIELD_RULE")
        if not isinstance(rule.get("type"), str) or rule.get("type") not in {
            "string",
            "integer",
            "number",
            "boolean",
            "timestamp",
        }:
            raise ContractError("INVALID_FIELD_TYPE")
        for flag in ("required", "nullable"):
            if flag in rule and type(rule[flag]) is not bool:
                raise ContractError("INVALID_FIELD_RULE")
        if "enum" in rule and (not isinstance(rule["enum"], list) or not rule["enum"]):
            raise ContractError("INVALID_FIELD_RULE")
        for bound in ("minimum", "maximum"):
            if bound in rule and (
                type(rule[bound]) not in (int, float) or not finite(rule[bound])
            ):
                raise ContractError("INVALID_FIELD_RULE")
        if (
            "minimum" in rule
            and "maximum" in rule
            and rule["minimum"] > rule["maximum"]
        ):
            raise ContractError("INVALID_FIELD_RULE")
        if "max_length" in rule and not integer(rule["max_length"], 1):
            raise ContractError("INVALID_FIELD_RULE")
        if any(bound in rule for bound in ("minimum", "maximum")) and rule[
            "type"
        ] not in {"integer", "number"}:
            raise ContractError("INAPPLICABLE_CONSTRAINT")
        if "max_length" in rule and rule["type"] not in {"string", "timestamp"}:
            raise ContractError("INAPPLICABLE_CONSTRAINT")
        if "enum" in rule:
            without_enum = {k: v for k, v in rule.items() if k != "enum"}
            for candidate in rule["enum"]:
                try:
                    validate_data({name: candidate}, {"fields": {name: without_enum}})
                except ContractError:
                    raise ContractError("INVALID_ENUM_CONTRACT") from None
    try:
        canonical(spec)
    except (ValueError, TypeError, RecursionError):
        raise ContractError("INVALID_CONTRACT") from None


def validate_data(data: dict, spec: dict) -> None:
    fields = spec["fields"]
    if set(data) - set(fields):
        raise ContractError("UNKNOWN_FIELD")
    for name, rule in fields.items():
        if name not in data:
            if rule.get("required", False):
                raise ContractError("MISSING_REQUIRED_FIELD")
            continue
        value = data[name]
        if value is None:
            if not rule.get("nullable", False):
                raise ContractError("NULL_NOT_ALLOWED")
            continue
        kind = rule["type"]
        valid = {
            "string": isinstance(value, str),
            "integer": type(value) is int and -MAX_INTEGER <= value <= MAX_INTEGER,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
            "timestamp": isinstance(value, str),
        }[kind]
        if not valid:
            raise ContractError("TYPE_MISMATCH")
        if kind == "timestamp":
            timestamp(value)
        if kind in {"number", "integer"}:
            if not finite(value):
                raise ContractError("NON_FINITE_NUMBER")
            if ("minimum" in rule and value < rule["minimum"]) or (
                "maximum" in rule and value > rule["maximum"]
            ):
                raise ContractError("OUT_OF_RANGE")
        if kind in {"string", "timestamp"} and len(value) > rule.get(
            "max_length", 4096
        ):
            raise ContractError("VALUE_TOO_LONG")
        if "enum" in rule and not any(
            type(value) is type(v) and value == v for v in rule["enum"]
        ):
            raise ContractError("INVALID_ENUM")


def validate_envelope(event: Any) -> dict:
    required = {
        "stream",
        "partition",
        "offset",
        "event_id",
        "key",
        "revision",
        "effective_at",
        "schema_version",
        "op",
        "data",
    }
    if not isinstance(event, dict) or set(event) != required:
        raise ContractError("INVALID_ENVELOPE")
    for field in ("stream", "event_id"):
        if not isinstance(event[field], str) or not TOKEN.fullmatch(event[field]):
            raise ContractError("INVALID_IDENTIFIER")
    if not isinstance(event["key"], str) or not event["key"] or len(event["key"]) > 256:
        raise ContractError("INVALID_KEY")
    for field in ("partition", "offset", "revision", "schema_version"):
        if not integer(
            event[field], 1 if field in {"revision", "schema_version"} else 0
        ):
            raise ContractError("INVALID_SEQUENCE")
    if event["op"] not in ("upsert", "delete") or not isinstance(event["data"], dict):
        raise ContractError("INVALID_OPERATION")
    if any(not isinstance(key, str) for key in event["data"]):
        raise ContractError("INVALID_DATA_KEYS")
    if event["op"] == "delete" and event["data"]:
        raise ContractError("DELETE_HAS_DATA")
    normalized = dict(event)
    normalized["effective_at"] = timestamp(event["effective_at"])
    try:
        if len(canonical(event).encode()) > 1_048_576:
            raise ContractError("EVENT_TOO_LARGE")
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ContractError("INVALID_JSON") from None
    return normalized
