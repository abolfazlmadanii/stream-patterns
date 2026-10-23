# stream-patterns

A catalog of streaming data modeling patterns for Apache Flink and Kafka, under development. Each pattern is planned to ship with a written explanation, the data model, an executable reference model, a Flink SQL implementation, tests, and its known failure modes.

**Status:** early. No patterns are published yet.

## Build and test

Requires [uv](https://docs.astral.sh/uv/) and Python **3.12**.

```bash
uv sync
uv run ruff check
uv run pytest
```

GitHub Actions runs `ruff` and `pytest` on every push (no Docker).

Pinned in `pyproject.toml`: **pytest**, **hypothesis**, **ruff**, **testcontainers** 4.15 (optional local Kafka). Flink jobs target **PyFlink 2.2.1**. CI does not start containers.

Tests will use [streamproof](https://github.com/abolfazlmadanii/streamproof), a companion correctness harness.

## Layout

| Package | Role |
|---------|------|
| `streampatterns.testkit` | Seeded event generator, `Oracle` protocol, pytest fixtures for local PyFlink and optional Docker |

See [docs/ROADMAP.md](docs/ROADMAP.md) for architecture notes.

Licensed under the [Apache License 2.0](LICENSE).
