# Operating runbook

## Before processing

1. Use an access-controlled local directory. New database files are mode 0600 on POSIX. Existing permissions are not rewritten.
2. Register contracts, retain synthetic/source input separately, and define source-offset mapping.
3. Use one connection per worker and batch at most 10,000 records. A source acknowledgement occurs only after successful commit.
4. Scrape `ledgerflow --db <path> metrics` via your job runner/Prometheus textfile collector. Alert on quarantine growth, job failures, stale externally tracked source lag and disk pressure. This CLI does not provide an HTTP metrics server or alert rules for an unconfigured deployment.

## Error response

| Code | Meaning | Action |
|---|---|---|
| `NON_CONTIGUOUS_OFFSET` | Missing/out-of-order transport position | Restore exact missing records. Never advance the checkpoint manually. |
| `OFFSET_CONTENT_CONFLICT` | A committed offset changed | Stop the producer, compare the retained source, investigate corruption. |
| `UNKNOWN_SCHEMA` | Schema not registered | Register it, then repair/re-publish affected records with new IDs/offsets. |
| `EVENT_ID_CONFLICT` / `REVISION_CONFLICT` | Producer identity reuse | Investigate producer; do not silently rewrite history. |
| `STORAGE_ERROR` | Lock, disk, schema or SQLite failure | Check capacity and process logs outside payloads; retry exact batch only after resolving cause. |
| `SNAPSHOT_BUSY` | Export lock exists | Establish whether exporter is alive; inspect destination before removing a stale lock. |
| `SNAPSHOT_ALREADY_EXISTS` | Immutable destination exists | Verify it; use a new destination for another snapshot. |
| `DATABASE_NOT_FOUND` | Read command points at missing file | Correct path; do not treat an empty ledger as successful validation. |

## Crash recovery

SQLite recovers committed state when reopening. Run `audit`, inspect `status`, then replay the last unacknowledged source batch unchanged. Crash tests deliberately kill a subprocess in the middle of ingestion and verify the next process observes no partial effects.

A crash during export may leave `.snapshot-*` directories or `<destination>.lock`. First establish that no exporter owns them. If the destination exists, run `verify` before taking any action. Only then remove known abandoned artifacts under change control. The library cleans failures it catches; it cannot clean after SIGKILL/power loss.

## Backup and restore

Use SQLite's online backup API or the SQLite CLI `.backup` operation under an approved backup policy. **Do not copy only the live `.db` file while WAL writes continue.** An exported snapshot is a derived read model, not a full source-ledger backup; it cannot recreate transport receipts or all superseded events.

Restore into an isolated local path, run `audit`, inspect retained source/checkpoints, and rehearse replay before switching writers. Define RPO/RTO and test them in the real deployment; this repository does not claim a measured recovery objective.

## Data retention

Valid payloads, history and source receipts persist indefinitely. The reference has no deletion/compaction API. Implement and test retention, legal holds, deletion propagation, backups and snapshot removal before processing regulated data. Never put real credentials, production records or exports in Git.
