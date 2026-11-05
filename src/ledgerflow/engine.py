"""Transactional ingestion, immutable source ledger and event-time SCD2 views."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .contracts import (
    ContractError,
    TOKEN,
    canonical,
    digest,
    integer,
    validate_contract,
    validate_data,
    validate_envelope,
)


class PipelineError(RuntimeError):
    """A safe machine-readable operational error."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS contracts (
 stream TEXT NOT NULL, version INTEGER NOT NULL, spec TEXT NOT NULL, hash TEXT NOT NULL,
 PRIMARY KEY(stream,version));
CREATE TABLE IF NOT EXISTS checkpoints (
 stream TEXT NOT NULL, partition INTEGER NOT NULL, last_offset INTEGER NOT NULL,
 PRIMARY KEY(stream,partition));
CREATE TABLE IF NOT EXISTS records (
 stream TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL,
 raw_hash TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('accepted','quarantined','duplicate')),
 reason TEXT, PRIMARY KEY(stream,partition,offset));
CREATE TABLE IF NOT EXISTS events (
 stream TEXT NOT NULL, event_id TEXT NOT NULL, entity_key TEXT NOT NULL,
 revision INTEGER NOT NULL, effective_at TEXT NOT NULL, op TEXT NOT NULL,
 schema_version INTEGER NOT NULL, data TEXT NOT NULL, identity_hash TEXT NOT NULL,
 PRIMARY KEY(stream,event_id), UNIQUE(stream,entity_key,revision));
CREATE INDEX IF NOT EXISTS events_history ON events(stream,entity_key,effective_at,revision);
CREATE TABLE IF NOT EXISTS runs (
 run_id INTEGER PRIMARY KEY, committed_at TEXT NOT NULL,
 accepted INTEGER NOT NULL, quarantined INTEGER NOT NULL, duplicates INTEGER NOT NULL,
 replays INTEGER NOT NULL);
CREATE VIEW IF NOT EXISTS history AS
 WITH ranked AS (
   SELECT *, ROW_NUMBER() OVER(PARTITION BY stream,entity_key,effective_at ORDER BY revision DESC) AS rank
   FROM events
 ), intervals AS (
   SELECT stream,entity_key,revision,effective_at AS valid_from,
     LEAD(effective_at) OVER(PARTITION BY stream,entity_key ORDER BY effective_at) AS valid_to,
     op, schema_version,data,event_id FROM ranked WHERE rank=1
 ) SELECT * FROM intervals;
CREATE VIEW IF NOT EXISTS current_entities AS
 SELECT * FROM history WHERE valid_to IS NULL AND op='upsert';
"""


class Ledger:
    """One connection per worker. SQLite serializes writes, with a bounded busy timeout."""

    def __init__(self, path: str | Path, *, timeout: float = 5.0):
        self.path = str(path)
        if self.path != ":memory:":
            # SQLite derives WAL/SHM permissions from the database file.
            descriptor = os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(descriptor)
        self.db = sqlite3.connect(self.path, timeout=timeout, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=" + str(max(0, int(timeout * 1000))))
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            self.close()
            raise PipelineError("UNSUPPORTED_DATABASE_VERSION")
        self.db.executescript(
            "BEGIN IMMEDIATE;" + SCHEMA + "PRAGMA user_version=1; COMMIT;"
        )

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> Ledger:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def transaction(self, *, write: bool = True):
        self.db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def register_contract(self, stream: str, version: int, spec: dict) -> str:
        if (
            not isinstance(stream, str)
            or not TOKEN.fullmatch(stream)
            or not integer(version, 1)
        ):
            raise PipelineError("INVALID_CONTRACT_ID")
        validate_contract(spec)
        sha = digest(spec)
        with self.transaction():
            old = self.db.execute(
                "SELECT * FROM contracts WHERE stream=? AND version=?",
                (stream, version),
            ).fetchone()
            if old:
                if old["hash"] != sha:
                    raise PipelineError("IMMUTABLE_CONTRACT")
                return sha
            latest = self.db.execute(
                "SELECT * FROM contracts WHERE stream=? ORDER BY version DESC LIMIT 1",
                (stream,),
            ).fetchone()
            if version != (latest["version"] + 1 if latest else 1):
                raise PipelineError("NON_CONTIGUOUS_CONTRACT_VERSION")
            if latest:
                previous = json.loads(latest["spec"])["fields"]
                new = spec["fields"]
                if any(new.get(name) != rule for name, rule in previous.items()):
                    raise PipelineError("BREAKING_SCHEMA_CHANGE")
                if any(
                    rule.get("required", False)
                    for name, rule in new.items()
                    if name not in previous
                ):
                    raise PipelineError("BREAKING_SCHEMA_CHANGE")
            self.db.execute(
                "INSERT INTO contracts VALUES(?,?,?,?)",
                (stream, version, canonical(spec), sha),
            )
        return sha

    def ingest(self, events: Iterable[dict], *, max_batch: int = 10_000) -> dict:
        if not integer(max_batch, 1):
            raise PipelineError("INVALID_BATCH_LIMIT")
        result = {"accepted": 0, "quarantined": 0, "duplicates": 0, "replays": 0}
        with self.transaction():
            for index, raw in enumerate(events):
                if index >= max_batch:
                    raise PipelineError("BATCH_TOO_LARGE")
                try:
                    event = validate_envelope(raw)
                    raw_hash = digest(raw)
                except ContractError as exc:
                    raise PipelineError(str(exc)) from None
                stream, partition, offset = (
                    event["stream"],
                    event["partition"],
                    event["offset"],
                )
                previous = self.db.execute(
                    "SELECT raw_hash FROM records WHERE stream=? AND partition=? AND offset=?",
                    (stream, partition, offset),
                ).fetchone()
                if previous:
                    if previous["raw_hash"] != raw_hash:
                        raise PipelineError("OFFSET_CONTENT_CONFLICT")
                    result["replays"] += 1
                    continue
                checkpoint = self.db.execute(
                    "SELECT last_offset FROM checkpoints WHERE stream=? AND partition=?",
                    (stream, partition),
                ).fetchone()
                expected = checkpoint[0] + 1 if checkpoint else 0
                if offset != expected:
                    raise PipelineError("NON_CONTIGUOUS_OFFSET")
                reason, duplicate = self._accept(event)
                status = (
                    "quarantined"
                    if reason
                    else ("duplicate" if duplicate else "accepted")
                )
                result[{"duplicate": "duplicates"}.get(status, status)] += 1
                self.db.execute(
                    "INSERT INTO records VALUES(?,?,?,?,?,?)",
                    (stream, partition, offset, raw_hash, status, reason),
                )
                self.db.execute(
                    "INSERT INTO checkpoints VALUES(?,?,?) ON CONFLICT(stream,partition) DO UPDATE SET last_offset=excluded.last_offset",
                    (stream, partition, offset),
                )
            self.db.execute(
                "INSERT INTO runs(committed_at,accepted,quarantined,duplicates,replays) VALUES(?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), *result.values()),
            )
            result["run_id"] = self.db.execute("SELECT last_insert_rowid()").fetchone()[
                0
            ]
        return result

    def _accept(self, event: dict) -> tuple[str | None, bool]:
        identity = {k: v for k, v in event.items() if k not in {"partition", "offset"}}
        identity_hash = digest(identity)
        existing = self.db.execute(
            "SELECT identity_hash FROM events WHERE stream=? AND event_id=?",
            (event["stream"], event["event_id"]),
        ).fetchone()
        if existing:
            return (
                (None, True)
                if existing[0] == identity_hash
                else ("EVENT_ID_CONFLICT", False)
            )
        same_revision = self.db.execute(
            "SELECT 1 FROM events WHERE stream=? AND entity_key=? AND revision=?",
            (event["stream"], event["key"], event["revision"]),
        ).fetchone()
        if same_revision:
            return "REVISION_CONFLICT", False
        contract = self.db.execute(
            "SELECT spec FROM contracts WHERE stream=? AND version=?",
            (event["stream"], event["schema_version"]),
        ).fetchone()
        if not contract:
            return "UNKNOWN_SCHEMA", False
        if event["op"] == "upsert":
            try:
                validate_data(event["data"], json.loads(contract[0]))
            except ContractError as exc:
                return str(exc), False
        self.db.execute(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?)",
            (
                event["stream"],
                event["event_id"],
                event["key"],
                event["revision"],
                event["effective_at"],
                event["op"],
                event["schema_version"],
                canonical(event["data"]),
                identity_hash,
            ),
        )
        return None, False

    def history(self, stream: str, key: str | None = None) -> list[dict]:
        query = "SELECT * FROM history WHERE stream=?"
        args = [stream]
        if key is not None:
            query += " AND entity_key=?"
            args.append(key)
        return self._rows(query + " ORDER BY entity_key,valid_from", args)

    def current(self, stream: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM current_entities WHERE stream=? ORDER BY entity_key",
            [stream],
        )

    def _rows(self, sql: str, params: list) -> list[dict]:
        values = [dict(row) for row in self.db.execute(sql, params)]
        for row in values:
            if "data" in row:
                row["data"] = json.loads(row["data"])
        return values

    def status(self) -> dict:
        with self.transaction(write=False):
            counts = {
                row[0]: row[1]
                for row in self.db.execute(
                    "SELECT status,COUNT(*) FROM records GROUP BY status"
                )
            }
            return {
                "records": sum(counts.values()),
                "outcomes": counts,
                "events": self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                "checkpoints": self._rows(
                    "SELECT * FROM checkpoints ORDER BY stream,partition", []
                ),
                "quarantine_reasons": {
                    row[0]: row[1]
                    for row in self.db.execute(
                        "SELECT reason,COUNT(*) FROM records WHERE status='quarantined' GROUP BY reason"
                    )
                },
            }

    def audit(self) -> dict:
        """Read-only integrity checks against durable invariants; no repair side effects."""
        issues = []
        with self.transaction(write=False):
            if self.db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                issues.append("SQLITE_INTEGRITY")
            partitions = self.db.execute(
                "SELECT stream,partition,MIN(offset),MAX(offset),COUNT(*) FROM records GROUP BY stream,partition"
            ).fetchall()
            for stream, partition, low, high, count in partitions:
                checkpoint = self.db.execute(
                    "SELECT last_offset FROM checkpoints WHERE stream=? AND partition=?",
                    (stream, partition),
                ).fetchone()
                if (
                    low != 0
                    or count != high + 1
                    or checkpoint is None
                    or checkpoint[0] != high
                ):
                    issues.append("CHECKPOINT_DIVERGENCE")
            if self.db.execute(
                "SELECT COUNT(*) FROM checkpoints c WHERE NOT EXISTS(SELECT 1 FROM records r WHERE r.stream=c.stream AND r.partition=c.partition)"
            ).fetchone()[0]:
                issues.append("ORPHAN_CHECKPOINT")
            for row in self.db.execute("SELECT * FROM contracts"):
                if digest(json.loads(row["spec"])) != row["hash"]:
                    issues.append("CONTRACT_HASH_MISMATCH")
            for row in self.db.execute("SELECT * FROM events"):
                identity = {
                    "stream": row["stream"],
                    "event_id": row["event_id"],
                    "key": row["entity_key"],
                    "revision": row["revision"],
                    "effective_at": row["effective_at"],
                    "op": row["op"],
                    "schema_version": row["schema_version"],
                    "data": json.loads(row["data"]),
                }
                if digest(identity) != row["identity_hash"]:
                    issues.append("EVENT_HASH_MISMATCH")
        return {"ok": not issues, "issues": sorted(set(issues))}
