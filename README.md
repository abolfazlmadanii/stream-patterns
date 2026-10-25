# stream-patterns

A catalog of streaming data modeling patterns for Apache Flink and Kafka, under development. Each pattern is planned to ship with a written explanation, the data model, an executable reference model, a Flink SQL implementation, tests, and its known failure modes.

**Status:** early. The first executable pattern (late data) is available under `streampatterns.patterns.late_data`.

## Build and test

Requires [uv](https://docs.astral.sh/uv/) and Python **3.12**. PyFlink local tests need a **JDK 17+** on `PATH` (the PyFlink runtime).

```bash
uv sync
uv run ruff check
uv run pytest
```

GitHub Actions installs Temurin JDK 17, then runs `ruff` and `pytest` (no Docker).

Pinned in `pyproject.toml`: **apache-flink 2.2.1**, **pytest**, **hypothesis**, **ruff**, **testcontainers** 4.15 (optional local Kafka).

Tests will use [streamproof](https://github.com/abolfazlmadanii/streamproof), a companion correctness harness.

## Pattern: late data (P05)

Bounded-out-of-orderness event-time watermarks, tumbling windows, and a **side output** for late records.

- Job: `streampatterns.patterns.late_data.job.wire_late_data`
- Reference oracle: `streampatterns.patterns.late_data.reference.LateDataOracle`
- Smoke tests: `tests/patterns/test_late_data_job.py` (`from_collection`, no Kafka)

```bash
uv run pytest tests/patterns/test_late_data_job.py -v
```

## Local platform (optional)

Kafka 4.3.1 and Flink with PyFlink for manual runs — not used in CI. See [platform/README.md](platform/README.md).

```bash
make -f platform/Makefile up
make -f platform/Makefile down
make -f platform/Makefile test
```

## Layout

| Package | Role |
|---------|------|
| `streampatterns.patterns_common` | `StreamEvent` schema and struct codec |
| `streampatterns.testkit` | Seeded generator, `Oracle` protocol, pytest fixtures |
| `streampatterns.patterns` | PyFlink pattern jobs |

See [docs/ROADMAP.md](docs/ROADMAP.md) for architecture notes.

Licensed under the [Apache License 2.0](LICENSE).
