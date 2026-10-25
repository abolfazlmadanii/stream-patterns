from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """Canonical stream record schema for patterns and tests."""

    event_id: str
    event_time_millis: int
    category: str
    amount_cents: int
