from __future__ import annotations

from collections.abc import Callable

from streampatterns.patterns_common.events import StreamEvent
from streampatterns.testkit.oracle import Oracle


class KeyedDedupReference(Oracle[StreamEvent, str]):
    """Event-time keyed deduplication with per-key TTL (ingestion order)."""

    def __init__(
        self,
        ttl_millis: int,
        key_fn: Callable[[StreamEvent], str] | None = None,
    ) -> None:
        if ttl_millis < 0:
            raise ValueError("ttl_millis must be non-negative")
        self._ttl_millis = ttl_millis
        self._key_fn = key_fn or (lambda event: event.category)

    def expected(self, inputs: list[StreamEvent]) -> list[str]:
        return self.apply(inputs)

    def apply(self, inputs: list[StreamEvent]) -> list[str]:
        emitted: list[str] = []
        key_expires_at: dict[str, int] = {}

        for event in inputs:
            key = self._key_fn(event)
            event_time = event.event_time_millis
            expires_at = key_expires_at.get(key)
            if expires_at is None or event_time >= expires_at:
                emitted.append(event.event_id)
                key_expires_at[key] = event_time + self._ttl_millis

        return emitted
