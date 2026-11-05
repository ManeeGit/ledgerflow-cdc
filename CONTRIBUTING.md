# Contributing

Use Python 3.11 or newer. This package has no runtime dependencies.

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
PYTHONPATH=src python scripts/demo.py
python -m compileall -q src tests scripts
```

Optional development tools:

```bash
python -m pip install '.[dev]'
ruff check src tests scripts
ruff format --check src tests scripts
bandit -r src -q
coverage run -m unittest discover -s tests -v
coverage report
```

CI requires at least 90% statement coverage.

Add a failing regression test before fixing a correctness issue. Keep core transaction invariants, failure codes, privacy boundaries and docs synchronized. Use synthetic fixtures only. For adapters, include crash/replay/acknowledgement tests and document actual source-offset semantics. Do not add placeholder enterprise components or unverified scale claims.

Open a draft pull request with the problem, solution, validation and limitations. Changes to history semantics, contracts or persistence require an explicit migration/compatibility discussion. Never force an existing database version forward silently.
