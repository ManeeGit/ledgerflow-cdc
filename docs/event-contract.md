# Source contract

Each line is one JSON object with exactly these fields:

```json
{"stream":"orders","partition":0,"offset":0,"event_id":"order-001-v1","key":"order-001","revision":1,"effective_at":"2026-01-01T12:00:00Z","schema_version":1,"op":"upsert","data":{"status":"pending","amount_cents":12900,"currency":"USD"}}
```

- `stream` and `event_id`: 1–128 ASCII letters/digits, `.`, `_`, `:`, or `-`, starting with a letter/digit.
- `partition` and `offset`: nonnegative integers up to signed 64-bit maximum. Booleans are not integers.
- `key`: nonempty string up to 256 characters. Use non-sensitive stable surrogate identifiers.
- `revision` and `schema_version`: positive signed 64-bit integers.
- `effective_at`: timezone-aware ISO 8601 timestamp containing `T`; normalized to UTC microsecond precision. Producers should not use finer precision because the reference normalizes to microseconds.
- `op`: `upsert` or `delete`. A delete must have `data: {}`.
- `data`: flat object conforming to the registered schema. Unknown fields are rejected, not dropped.

Records are capped at 1 MiB canonical size and CLI line size; a batch is capped at 10,000 records. CLI contract files are capped at 64 KiB. Duplicate JSON keys, non-finite numbers, unknown envelope fields and malformed envelopes abort the batch. Blank JSONL lines are errors.

Contract version 1 must be registered first. Versions are immutable and consecutive. A new version may only add optional fields; existing rule definitions must remain equal. Breaking changes should get a new stream plus a documented migration.

Supported field types: `string`, `integer`, `number`, `boolean`, `timestamp`. Rules support `required`, `nullable`, `enum`, numeric `minimum`/`maximum`, and string/timestamp `max_length`. Default string length maximum is 4096. No implicit coercion occurs. Numeric contract bounds must be finite; enum candidates must themselves satisfy the rule. Timestamp payloads are validated but stored in their original spelling. The envelope effective timestamp is normalized.

## Remediation

A quarantined offset is already consumed and immutable. Do not edit its original record and replay at the same offset. Retrieve the original payload from your authorized source, repair its data or register the missing schema, then publish a fresh event ID at the next offset. A corrected event may reuse the rejected revision because it was not accepted into the event ledger.

The built-in JSONL adapter is a finite ordered batch. It does not subscribe to Kafka, Debezium, databases or object storage. A connector must preserve per-partition sequence, durable mapping to external offsets and source retention until this ledger acknowledges commit.
