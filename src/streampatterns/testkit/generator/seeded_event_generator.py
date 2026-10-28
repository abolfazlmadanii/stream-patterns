from __future__ import annotations

import random

from streampatterns.patterns_common.events import StreamEvent

_CATEGORIES = ("click", "purchase", "refund", "signup")


class SeededEventGenerator:
    """Produces repeatable :class:`StreamEvent` sequences for a fixed seed."""

    def __init__(self, seed: int) -> None:
        self._random = random.Random(seed)
        self._sequence = 0

    def next(self) -> StreamEvent:
        index = self._sequence
        self._sequence += 1
        event_id = f"evt-{index}"
        event_time_millis = 1_700_000_000_000 + index * 1_000
        category = self._random.choice(_CATEGORIES)
        amount_cents = self._random.randrange(1, 10_000)
        return StreamEvent(
            event_id=event_id,
            event_time_millis=event_time_millis,
            category=category,
            amount_cents=amount_cents,
        )

    def next_n(self, count: int) -> list[StreamEvent]:
        if count < 0:
            raise ValueError("count must be non-negative")
        return [self.next() for _ in range(count)]
