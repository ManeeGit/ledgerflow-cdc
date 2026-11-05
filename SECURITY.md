# Security policy

This is a reference project, not a hosted service. Use synthetic data until your organization's security controls are implemented.

## Threat model

The CLI runs as a trusted local operator against a trusted local directory. Inputs may be malformed, duplicated or accidentally changed. The project validates shapes/types/bounds, parameterizes SQL, rejects duplicate JSON keys, caps input/batch size, avoids logging payloads and keeps rejected business payloads out of the ledger. Accepted data and identifiers remain confidential material.

The project does not defend against a privileged local attacker, hostile filesystem races, someone modifying both data and checksums, malicious producers with authorization, or an untrusted embedded Python caller. Export manifests provide integrity checks, not signatures. SHA-256 of sensitive values is not anonymization. SQLite encryption, service authentication, TLS, PII detection, tenant access control and secrets management are not included.

Use restrictive directories and encrypted storage; new POSIX database files are created 0600 and snapshot staging directories are private. Containers use UID 10001 and expose no port. Run with read-only rootfs and no capabilities where possible. Never commit `.env`, credentials, real source records, SQLite files, snapshots or backups.

## Reporting

Use GitHub private vulnerability reporting if enabled on the repository. Otherwise contact the repository maintainer privately using their GitHub profile. Do not open a public issue containing exploit details, credentials or real data. No response-time guarantee is currently offered.

GitHub Actions have read-only repository permissions and pinned action revisions. Dependabot proposes updates. Container base-image digest pinning, signed releases and a formal vulnerability scanning/promotion gate remain production requirements.
