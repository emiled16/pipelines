# Repository Guidelines

## Project Structure & Module Organization
The repository is a small Python workspace managed with Poetry. The root contains `main.py`, a runnable RSS ingestion example that writes to SQLite, plus the top-level `pyproject.toml`. The active library lives in `libs/ingestion/`, with source code under `libs/ingestion/src/ingestion/` and tests under `libs/ingestion/tests/`. Package areas are split by responsibility: `abstractions/`, `models/`, `providers/rss/`, `stores/`, `database/`, and `utils/`. `libs/feature-store/` currently contains no implementation, so treat it as a placeholder.

## Build, Test, and Development Commands
Use Python `3.13.0` and Poetry.

- `poetry install`: install root dependencies for the demo runner.
- `poetry run python main.py --help`: inspect CLI options for the RSS example.
- `poetry run python main.py --feed-url https://planetpython.org/rss20.xml`: run the demo and create/update `rss_ingestion.db`.
- `cd libs/ingestion && poetry install`: install library and dev dependencies.
- `cd libs/ingestion && poetry run pytest -q`: run the ingestion test suite.
- `cd libs/ingestion && poetry run ruff check src tests`: run linting and import-order checks.

## Coding Style & Naming Conventions
Follow the existing style in `libs/ingestion/src`: 4-space indentation, type hints throughout, `from __future__ import annotations`, and small focused modules. Keep line length at `100` characters to match Ruff. Use `snake_case` for modules, functions, variables, and test names; use `PascalCase` for classes and dataclasses. Prefer explicit package paths such as `ingestion.providers.rss.provider`.

## Testing Guidelines
Tests use `pytest` and live in `libs/ingestion/tests/unit/` and `libs/ingestion/tests/integration/`. Name files `test_*.py` and keep test functions in the same pattern. Add unit coverage for new provider, store, and serialization logic; use integration tests when persistence or async boundaries matter. Some tests skip when optional dependencies like SQLAlchemy are not installed, so install the library extras or dev environment before relying on full coverage.

## Commit & Pull Request Guidelines
Recent history uses short Conventional Commit subjects such as `feat: upload RSS provider` and `feat: add first version of ingestion`. Keep the same format: `feat: ...`, `fix: ...`, `chore: ...`. PRs should describe the behavior change, note affected paths, list verification commands, and include sample CLI output when `main.py` behavior changes.
