from __future__ import annotations

import random
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis import strategies as st

from streampatterns.patterns.cdc_changelog.reference import (
    CdcChangelogReference,
    CdcEnvelope,
    simulate_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

_row_values = st.one_of(st.integers(min_value=-10_000, max_value=10_000), st.text(max_size=12))
_rows = st.dictionaries(st.text(min_size=1, max_size=8), _row_values, max_size=4)
_ops = st.sampled_from(("c", "u", "r", "d"))


@st.composite
def envelope_lists(draw: st.DrawFn) -> list[CdcEnvelope]:
    key_count = draw(st.integers(min_value=1, max_value=4))
    keys = [f"k{i}" for i in range(key_count)]
    events: list[CdcEnvelope] = []
    lsn = 0
    for _ in range(draw(st.integers(min_value=1, max_value=16))):
        key = draw(st.sampled_from(keys))
        op = draw(_ops)
        lsn += draw(st.integers(min_value=1, max_value=20))
        before = draw(_rows) if op in ("u", "d") else None
        after = None if op == "d" else draw(_rows)
        events.append(CdcEnvelope(op=op, key=key, before=before, after=after, lsn=lsn))
    return events


def _interleave_preserving_order(
    left: list[CdcEnvelope],
    right: list[CdcEnvelope],
    rng: random.Random,
) -> list[CdcEnvelope]:
    merged: list[CdcEnvelope] = []
    left_index = 0
    right_index = 0
    while left_index < len(left) or right_index < len(right):
        take_left = right_index >= len(right) or (
            left_index < len(left) and rng.random() < 0.5
        )
        if take_left:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    return merged


def _split_by_key(events: Iterable[CdcEnvelope]) -> dict[str, list[CdcEnvelope]]:
    by_key: dict[str, list[CdcEnvelope]] = {}
    for event in events:
        by_key.setdefault(event.key, []).append(event)
    return by_key


def test_delete_then_recreate() -> None:
    reference = CdcChangelogReference()
    events = [
        CdcEnvelope("c", "user-1", None, {"name": "ada"}, 1),
        CdcEnvelope("d", "user-1", {"name": "ada"}, None, 2),
        CdcEnvelope("c", "user-1", None, {"name": "grace"}, 3),
    ]
    assert reference.apply(events) == {"user-1": {"name": "grace"}}


@given(envelope_lists())
def test_duplicating_events_is_idempotent(events: list[CdcEnvelope]) -> None:
    reference = CdcChangelogReference()
    once = reference.apply(events)
    twice = reference.apply(events + events)
    assert once == twice


@given(envelope_lists())
def test_interleaved_keys_same_result(events: list[CdcEnvelope]) -> None:
    reference = CdcChangelogReference()
    baseline = reference.apply(events)

    by_key = _split_by_key(events)
    keys = sorted(by_key)
    if len(keys) < 2:
        return

    left_keys = keys[: len(keys) // 2]
    right_keys = keys[len(keys) // 2 :]
    left = [event for key in left_keys for event in by_key[key]]
    right = [event for key in right_keys for event in by_key[key]]

    rng_a = random.Random(0)
    rng_b = random.Random(1)
    merged_a = _interleave_preserving_order(left, right, rng_a)
    merged_b = _interleave_preserving_order(left, right, rng_b)

    assert reference.apply(merged_a) == baseline
    assert reference.apply(merged_b) == baseline


@given(envelope_lists())
def test_matches_direct_simulation(events: list[CdcEnvelope]) -> None:
    reference = CdcChangelogReference()
    positions = [(event.key, event.lsn) for event in events]
    assert len(positions) == len(set(positions))
    assert reference.apply(events) == simulate_table(events)
