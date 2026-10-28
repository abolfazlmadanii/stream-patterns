# stream-patterns

A catalog of streaming data modeling patterns for Apache Flink and Kafka, under development. Each pattern is planned to ship with a written explanation, the data model, an executable reference model, a Flink SQL implementation, tests, and its known failure modes.

**Status:** early. Patterns under `streampatterns.patterns` include late data, keyed deduplication, and CDC changelog (reference models).

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

Bounded-out-of-orderness event-time watermarks and a **side output** (`LATE_EVENTS_TAG`) for very-late records.

- Job: `streampatterns.patterns.late_data.job.wire_late_data`
- Reference oracle: `streampatterns.patterns.late_data.reference.LateDataOracle`
- Smoke tests: `tests/patterns/test_late_data_job.py` (`from_collection`, no Kafka)
- Optional Kafka I/O: `wire_late_data_from_kafka` / `wire_late_data_to_kafka` with connector jar `5.0.0-2.2` (`make -f platform/Makefile download-kafka-connector` or `STREAM_PATTERNS_KAFKA_CONNECTOR_JAR`)

```bash
uv run pytest tests/patterns/test_late_data_job.py -v
```

## Pattern: keyed deduplication (TTL)

Per-key event-time deduplication: the first record for a key wins until `event_time + TTL`, then the key accepts a new winner.

- Reference: `streampatterns.patterns.keyed_dedup.reference.KeyedDedupReference`
- Tests: `tests/patterns/test_keyed_dedup_reference.py` (hypothesis properties; no PyFlink job yet)

```bash
uv run pytest tests/patterns/test_keyed_dedup_reference.py -v
```

## Pattern: CDC changelog (P03)

Debezium-style envelopes (`c` / `u` / `r` / `d`, key, before/after, source LSN) folded into a table. Per key, events apply in LSN order; `c`, `u`, and `r` upsert the after-image, `d` removes the row. Positions at or below the key’s last applied LSN are ignored so replays and duplicates are idempotent.

Properties checked in tests: duplicate events change nothing; interleaving keys preserves the outcome when each key’s order is kept; the fold matches a direct dict simulation; delete then create leaves the new row.

- Reference: `streampatterns.patterns.cdc_changelog.reference.CdcChangelogReference`
- Tests: `tests/patterns/test_cdc_changelog_reference.py`

```bash
uv run pytest tests/patterns/test_cdc_changelog_reference.py -v
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
