from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

Row = dict[str, Any]
Table = dict[str, Row]

CdcOp = Literal["c", "u", "r", "d"]


@dataclass(frozen=True, slots=True)
class CdcEnvelope:
    """Debezium-style change event (key, before/after images, source position)."""

    op: CdcOp
    key: str
    before: Row | None
    after: Row | None
    lsn: int


class CdcChangelogReference:
    """Fold an ordered changelog into a materialized table (per-key LSN idempotency)."""

    def apply(self, events: list[CdcEnvelope]) -> Table:
        table: Table = {}
        last_applied_lsn: dict[str, int] = {}

        for event in sorted(events, key=lambda item: item.lsn):
            last_lsn = last_applied_lsn.get(event.key, -1)
            if event.lsn <= last_lsn:
                continue

            if event.op in ("c", "u", "r"):
                if event.after is None:
                    raise ValueError(f"op {event.op!r} requires an after image")
                table[event.key] = dict(event.after)
            elif event.op == "d":
                table.pop(event.key, None)
            else:
                raise ValueError(f"unsupported op {event.op!r}")

            last_applied_lsn[event.key] = event.lsn

        return table


def simulate_table(events: list[CdcEnvelope]) -> Table:
    """Apply each event in LSN order without idempotency filtering (unique positions only)."""
    table: Table = {}
    for event in sorted(events, key=lambda item: item.lsn):
        if event.op in ("c", "u", "r"):
            if event.after is None:
                raise ValueError(f"op {event.op!r} requires an after image")
            table[event.key] = dict(event.after)
        elif event.op == "d":
            table.pop(event.key, None)
    return table
