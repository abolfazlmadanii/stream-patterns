from __future__ import annotations

from streampatterns.patterns_common.events import StreamEvent
from streampatterns.testkit.oracle import Oracle

_TIMESTAMP_MIN = -(2**63)


class LateDataOracle(Oracle[StreamEvent, str]):
    """Reference split of on-time vs late ids for ingestion-ordered event-time streams."""

    def __init__(self, max_out_of_orderness_millis: int) -> None:
        self._max_out_of_orderness_millis = max_out_of_orderness_millis

    def expected(self, inputs: list[StreamEvent]) -> list[str]:
        on_time, _late = self.partition(inputs)
        return on_time

    def partition(self, inputs: list[StreamEvent]) -> tuple[list[str], list[str]]:
        on_time: list[str] = []
        late: list[str] = []
        max_seen = _TIMESTAMP_MIN

        for event in inputs:
            watermark = (
                max_seen - self._max_out_of_orderness_millis
                if max_seen > _TIMESTAMP_MIN
                else _TIMESTAMP_MIN
            )
            if watermark > _TIMESTAMP_MIN and event.event_time_millis < watermark:
                late.append(event.event_id)
            else:
                on_time.append(event.event_id)
            max_seen = max(max_seen, event.event_time_millis)

        return on_time, late
