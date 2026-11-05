"""Consistent, atomically published JSONL snapshots with verifiable manifests."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical
from .engine import Ledger, PipelineError

FILES = {"history.jsonl", "current.jsonl", "contracts.jsonl", "checkpoints.jsonl"}


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def export_snapshot(ledger: Ledger, stream: str, destination: str | Path) -> dict:
    """Destination must be new and under a trusted, writable local directory."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise PipelineError("SNAPSHOT_ALREADY_EXISTS")
    lock = target.parent / (target.name + ".lock")
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise PipelineError("SNAPSHOT_BUSY") from None
    os.close(descriptor)
    temporary = None
    try:
        temporary = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=target.parent))
        manifest = {
            "format_version": 1,
            "stream": stream,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "files": {},
        }
        with ledger.transaction(write=False):
            if not ledger.db.execute(
                "SELECT 1 FROM contracts WHERE stream=? LIMIT 1", (stream,)
            ).fetchone():
                raise PipelineError("STREAM_NOT_FOUND")
            queries = {
                "history.jsonl": (
                    "SELECT * FROM history WHERE stream=? ORDER BY entity_key,valid_from",
                    True,
                ),
                "current.jsonl": (
                    "SELECT * FROM current_entities WHERE stream=? ORDER BY entity_key",
                    True,
                ),
                "contracts.jsonl": (
                    "SELECT * FROM contracts WHERE stream=? ORDER BY version",
                    False,
                ),
                "checkpoints.jsonl": (
                    "SELECT * FROM checkpoints WHERE stream=? ORDER BY partition",
                    False,
                ),
            }
            for name, (query, has_data) in queries.items():
                sha = hashlib.sha256()
                count = 0
                with (temporary / name).open("xb") as handle:
                    for source in ledger.db.execute(query, (stream,)):
                        row = dict(source)
                        if has_data:
                            row["data"] = json.loads(row["data"])
                        if name == "contracts.jsonl":
                            row["spec"] = json.loads(row["spec"])
                        content = (canonical(row) + "\n").encode()
                        handle.write(content)
                        sha.update(content)
                        count += 1
                    handle.flush()
                    os.fsync(handle.fileno())
                manifest["files"][name] = {"sha256": sha.hexdigest(), "rows": count}
        with (temporary / "manifest.json").open("x", encoding="utf-8") as handle:
            handle.write(canonical(manifest) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        _sync_directory(temporary)
        if target.exists() or target.is_symlink():
            raise PipelineError("SNAPSHOT_ALREADY_EXISTS")
        os.rename(temporary, target)
        _sync_directory(target.parent)
        return manifest
    finally:
        if temporary is not None and temporary.exists():
            shutil.rmtree(temporary)
        lock.unlink(missing_ok=True)


def verify_snapshot(destination: str | Path) -> dict:
    """Verify integrity, not authenticity. Treat snapshots as confidential data."""
    root = Path(destination)
    issues = []
    if root.is_symlink() or not root.is_dir():
        raise PipelineError("INVALID_SNAPSHOT")
    manifest_path = root / "manifest.json"
    if (
        manifest_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.stat().st_size > 65_536
    ):
        raise PipelineError("INVALID_MANIFEST")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            not isinstance(manifest, dict)
            or type(manifest.get("format_version")) is not int
            or manifest["format_version"] != 1
            or set(manifest["files"]) != FILES
        ):
            raise ValueError
        if set(path.name for path in root.iterdir()) != FILES | {"manifest.json"}:
            issues.append("UNEXPECTED_FILES")
        for name in sorted(FILES):
            rule = manifest["files"][name]
            path = root / name
            if path.is_symlink() or not path.is_file():
                issues.append("MISSING_OR_UNSAFE_FILE:" + name)
                continue
            if (
                not isinstance(rule, dict)
                or type(rule.get("rows")) is not int
                or rule["rows"] < 0
            ):
                raise ValueError
            if not isinstance(rule.get("sha256"), str) or len(rule["sha256"]) != 64:
                raise ValueError
            sha = hashlib.sha256()
            count = 0
            with path.open("rb") as handle:
                for line in handle:
                    sha.update(line)
                    count += 1
            if sha.hexdigest() != rule["sha256"] or count != rule["rows"]:
                issues.append("CHECKSUM_MISMATCH:" + name)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise PipelineError("INVALID_MANIFEST") from None
    return {"ok": not issues, "issues": issues}
