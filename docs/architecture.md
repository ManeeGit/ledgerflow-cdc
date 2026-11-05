# Architecture and design decisions

## Transaction boundary

A batch begins `BEGIN IMMEDIATE`. For every event, the engine validates its envelope, checks the previously committed record digest, and demands `offset == last_offset + 1` for new records. First offset is zero per `(stream, partition)`.

The event ledger, receipt, checkpoint and run summary live in one SQLite transaction. A generator/parser error, content conflict, offset gap, oversize batch or storage failure rolls back everything in that batch. A data-contract violation is different: its transport identity is trusted, so a payload-free quarantine receipt is committed and that offset is acknowledged. External source acknowledgement must happen **after** `ingest` returns successfully.

SQLite WAL + FULL synchronous mode provides local durability subject to the filesystem/device honoring sync. One connection belongs to one thread. Multiple independent connections serialize writes and use a five-second busy timeout. No retry loop silently changes ordering; callers may retry the exact batch after a lock failure.

## Identity and replay

Transport identity is `(stream, partition, offset)` and its SHA-256 of canonical input JSON. Canonical JSON sorts object keys, ignores whitespace and rejects NaN. A changed record at a committed position is never silently overwritten.

Business identity is `(stream, event_id)` and a SHA-256 of all fields except transport partition/offset, with UTC-normalized effective time. Business revisions are unique at `(stream, entity_key, revision)`. Exact business duplicates can move across partitions/offsets and still apply once. ID/revision conflicts become quarantine receipts, preserving forward progress without fabricating a new business state.

No digest is treated as encryption. If values have low entropy, an attacker with access to digests can guess them. The database and exports require the same access controls as source data.

## Why SQLite instead of decorative cloud infrastructure?

The executable reference keeps the correctness boundary inspectable, affordable and reproducible on a laptop or CI runner. Replacing the store with PostgreSQL or an object-store table format requires new implementations of atomic checkpoint/state commits, concurrency, snapshot consistency and recovery. A YAML file naming Kafka/Spark would not establish those properties.

The dependency-free Python package uses standard `sqlite3`, strict contract validation and window-function SQL. On promotion, select a real source, throughput/SLO target, storage and IAM boundary, then run the contract test suite against the adapter.

## SCD2 semantics

All accepted business events remain immutable. The `history` view first selects the highest revision at each `(stream, key, effective_at)`. It then uses `LEAD(effective_at)` to derive half-open intervals `[valid_from, valid_to)`. The final interval ends at infinity (`NULL`).

Deletes own an interval too. `current_entities` excludes a terminal delete; an earlier late event cannot resurrect a later delete. A later upsert can explicitly resurrect the entity. At identical effective times, a higher revision supersedes the visible interval while both events remain in the audit ledger.

Revision is an explicit tie-breaker, not the global time axis. Producers are responsible for meaningful revision and event-time values. If a business system instead needs transaction-time history or bitemporal corrections, add that model rather than describing this event-time view as bitemporal.

## Snapshot consistency and publication

Export obtains one read transaction before querying history, current entities, contracts and checkpoints. WAL readers retain a consistent snapshot even if another writer commits between file writes. Each file is streamed to a private staging directory, hashed and fsynced. A manifest records row counts, SHA-256, stream, format version and creation time. After the files and staging directory are synced, a same-filesystem directory rename publishes the complete snapshot.

An exclusive adjacent lock prevents cooperating exporters from publishing the same destination. Interrupted processes can leave locks/staging directories; see the runbook. A returned post-rename fsync error has an uncertain publication outcome: inspect and verify the destination before retrying. Destination parents must be trusted local directories.

## Audit scope

`audit` runs SQLite integrity checking, verifies checkpoint contiguity/counts, finds orphan checkpoints, and checks stored event/contract hashes. It detects common accidental drift; it is not a tamper-proof audit log and cannot recover original rejected payloads or prove source completeness beyond the supplied offset protocol.
