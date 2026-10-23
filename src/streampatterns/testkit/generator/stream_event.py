from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """Synthetic stream record used by the seeded event generator."""

    event_id: str
    event_time_millis: int
    category: str
    amount_cents: int
