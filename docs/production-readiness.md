# Production-readiness gate

This repository demonstrates implemented reliability controls. It is not evidence of deployment at an employer, operation at enterprise traffic or compliance approval.

## Implemented and locally testable

- Transactional state/checkpoint commit and replay fences.
- Strict versioned contracts and safe poison-record receipts.
- Late-event, deletion and deterministic SCD2 interval behavior.
- Bounded batches/records, safe error messages and parameterized SQL.
- Multi-connection serialization and subprocess crash recovery tests.
- Consistent atomic snapshots with checksum verification.
- Integrity checks, metrics text, non-root container definition and automated CI.

## Required before real production use

- [ ] Define source adapter, actual source-offset semantics and acknowledgement failure recovery.
- [ ] Measure ingest/query/export latency under realistic key skew, disk, volume and concurrent readers/writers.
- [ ] Define SLOs, backpressure, on-call alerts, runbook ownership and retention budgets.
- [ ] Evaluate whether single-writer SQLite can meet capacity/HA requirements; otherwise implement and test another storage boundary.
- [ ] Set IAM/service authorization, encrypted disks, secret management and restricted filesystem/backup permissions.
- [ ] Establish protected branches, reviewed releases, signed build artifacts/SBOM, container digest pinning and vulnerability response.
- [ ] Rehearse backup/restore, disk-full failure, machine loss, actual power-loss semantics and RPO/RTO.
- [ ] Add data classification, deletion/retention, PII controls and organization-specific compliance review.
- [ ] Verify deployment-platform behavior for directory fsync/rename and lock recovery. Do not use SQLite WAL on NFS.
- [ ] Load-test the source connector and source outages. No live Kafka/cloud database integration has been validated here.

## Validation honesty

A local unit-test pass does not prove Docker execution or remote CI success. The repository's Actions runs show independent remote results for exact commits. Container execution is part of CI because some development environments do not have Docker. Benchmarks are local observations with reproducible scripts, not service-level guarantees.
