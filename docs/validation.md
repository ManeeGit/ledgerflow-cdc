# Initial local validation record

Executed in a Linux cloud development container on 2026-11-05, Python 3.12.14. All inputs were synthetic. This report is evidence of local behavior, not deployment certification.

| Check | Result |
|---|---|
| Standard-library unittest suite | 60 tests passed |
| Coverage 7.10.6, source package | 96% statements; 487 statements, 20 missed |
| Independent adversarial suite | 25 tests, included in total |
| Actual child-process death during transaction | Recovery regression passed |
| 20 shuffled source-arrival permutations | Identical event-time history |
| Concurrent ingestion | 8 jobs across 4 worker connections passed |
| Snapshot with concurrent writer | Consistent files/checkpoints verified |
| Ruff 0.14.4 lint and format | Passed |
| Bandit 1.8.6, `src` | Passed; not a comprehensive security audit |
| Bytecode compilation | Passed |
| Wheel build and installation | Passed |
| Installed package demo/test | Passed |
| 10,000-event synthetic local benchmark | Approximately 24,800 events/sec; audit passed |
| Docker image execution | Not run locally: Docker unavailable |
| Python 3.11/3.13 matrix | Configured in CI; not run locally |
| Remote GitHub Actions | Consult exact commit's Actions results; configuration is not a pass |
| Live source/cloud deployment | Not performed |

The independent review identified five malformed-input or snapshot-cleanup defects, which were corrected and covered by regressions before this record. Production failure modes still requiring real-platform validation are listed in [production-readiness.md](production-readiness.md).

Reproduce from the repo root with `python -m pip install '.[dev]'`, `coverage run -m unittest discover -s tests -v`, `coverage report`, `ruff check src tests scripts`, `ruff format --check src tests scripts`, `bandit -r src -q`, `python scripts/demo.py` and `python scripts/benchmark.py --events 10000`.
