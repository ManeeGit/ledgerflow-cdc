"""Machine-readable CLI; errors never print source payloads."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import TextIO

from .contracts import ContractError, canonical
from .engine import Ledger, PipelineError
from .snapshot import export_snapshot, verify_snapshot


def _unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _bad_constant(_: str) -> None:
    raise PipelineError("NON_FINITE_NUMBER")


def read_json(text: str):
    try:
        return json.loads(
            text, object_pairs_hook=_unique_object, parse_constant=_bad_constant
        )
    except (ValueError, RecursionError):
        raise PipelineError("INVALID_JSON") from None


def read_events(handle: TextIO):
    while True:
        line = handle.readline(1_048_577)
        if not line:
            return
        if len(line.encode("utf-8")) > 1_048_576:
            raise PipelineError("EVENT_TOO_LARGE")
        if not line.strip():
            raise PipelineError("EMPTY_RECORD")
        yield read_json(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LedgerFlow CDC reliability reference")
    parser.add_argument(
        "--db", default="ledger.db", help="SQLite ledger file (default: ledger.db)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser(
        "register", help="Register an immutable, compatible contract"
    )
    register.add_argument("--stream", required=True)
    register.add_argument("--version", type=int, required=True)
    register.add_argument("contract", type=Path)
    ingest = commands.add_parser(
        "ingest", help="Atomically ingest at most 10,000 JSONL records"
    )
    ingest.add_argument("source", help="JSONL path, or - for standard input")
    for name in ("status", "audit", "metrics"):
        commands.add_parser(name)
    for name in ("current", "history", "export"):
        child = commands.add_parser(name)
        child.add_argument("--stream", required=True)
        if name == "history":
            child.add_argument("--key")
        if name == "export":
            child.add_argument("destination", type=Path)
    verify = commands.add_parser(
        "verify", help="Verify a snapshot without opening a database"
    )
    verify.add_argument("destination", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "verify":
            result = verify_snapshot(args.destination)
        else:
            if (
                args.command not in {"register", "ingest"}
                and not Path(args.db).is_file()
            ):
                raise PipelineError("DATABASE_NOT_FOUND")
            with Ledger(args.db) as ledger:
                if args.command == "register":
                    if args.contract.stat().st_size > 65_536:
                        raise PipelineError("CONTRACT_TOO_LARGE")
                    result = {
                        "contract_hash": ledger.register_contract(
                            args.stream,
                            args.version,
                            read_json(args.contract.read_text(encoding="utf-8")),
                        )
                    }
                elif args.command == "ingest":
                    if args.source == "-":
                        result = ledger.ingest(read_events(sys.stdin))
                    else:
                        with open(args.source, encoding="utf-8") as handle:
                            result = ledger.ingest(read_events(handle))
                elif args.command == "export":
                    result = export_snapshot(ledger, args.stream, args.destination)
                elif args.command == "current":
                    result = ledger.current(args.stream)
                elif args.command == "history":
                    result = ledger.history(args.stream, args.key)
                elif args.command == "audit":
                    result = ledger.audit()
                else:
                    result = ledger.status()
                    if args.command == "metrics":
                        print(
                            "# HELP ledgerflow_records_total Durable source records by outcome"
                        )
                        print("# TYPE ledgerflow_records_total counter")
                        for status in ("accepted", "quarantined", "duplicate"):
                            print(
                                f'ledgerflow_records_total{{outcome="{status}"}} {result["outcomes"].get(status, 0)}'
                            )
                        print(
                            "# HELP ledgerflow_events Unique accepted business events"
                        )
                        print("# TYPE ledgerflow_events gauge")
                        print("ledgerflow_events " + str(result["events"]))
                        return 0
        print(canonical(result))
        return 3 if isinstance(result, dict) and result.get("ok") is False else 0
    except (PipelineError, ContractError) as exc:
        print(canonical({"error": str(exc)}), file=sys.stderr)
        return 2
    except sqlite3.Error:
        print('{"error":"STORAGE_ERROR"}', file=sys.stderr)
        return 2
    except (OSError, UnicodeError):
        print('{"error":"IO_ERROR"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
