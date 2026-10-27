from hypothesis import given
from hypothesis import strategies as st

from streampatterns.patterns.keyed_dedup.reference import KeyedDedupReference
from streampatterns.patterns_common.events import StreamEvent

_categories = st.sampled_from(("click", "purchase", "refund", "signup"))


@st.composite
def event_lists(draw: st.DrawFn) -> list[StreamEvent]:
    count = draw(st.integers(min_value=0, max_value=24))
    events: list[StreamEvent] = []
    for index in range(count):
        events.append(
            StreamEvent(
                event_id=f"evt-{index}",
                event_time_millis=draw(st.integers(min_value=0, max_value=500_000)),
                category=draw(_categories),
                amount_cents=draw(st.integers(min_value=0, max_value=10_000)),
            )
        )
    return events


def test_keyed_dedup_ttl_expiry_example() -> None:
    reference = KeyedDedupReference(ttl_millis=1_000)
    events = [
        StreamEvent("first", 100, "click", 1),
        StreamEvent("dup", 500, "click", 2),
        StreamEvent("second", 1_100, "click", 3),
    ]
    assert reference.apply(events) == ["first", "second"]


@given(event_lists(), st.integers(min_value=1, max_value=120_000))
def test_keyed_dedup_idempotent_on_replay(
    events: list[StreamEvent],
    ttl_millis: int,
) -> None:
    reference = KeyedDedupReference(ttl_millis=ttl_millis)
    once = reference.apply(events)
    twice = reference.apply(events)
    assert once == twice


@given(event_lists(), st.integers(min_value=1, max_value=120_000))
def test_keyed_dedup_first_event_per_key_within_ttl_wins(
    events: list[StreamEvent],
    ttl_millis: int,
) -> None:
    reference = KeyedDedupReference(ttl_millis=ttl_millis)
    emitted_ids = reference.apply(events)
    key_expires_at: dict[str, int] = {}

    for event in events:
        key = event.category
        event_time = event.event_time_millis
        expires_at = key_expires_at.get(key)
        if expires_at is None or event_time >= expires_at:
            assert event.event_id in emitted_ids
            key_expires_at[key] = event_time + ttl_millis
        else:
            assert event.event_id not in emitted_ids


@given(
    st.integers(min_value=1, max_value=50_000),
    st.integers(min_value=1, max_value=120_000),
)
def test_keyed_dedup_key_expires_after_ttl(
    gap_before_expiry: int,
    ttl_millis: int,
) -> None:
    first_time = 10_000
    second_time = first_time + ttl_millis + gap_before_expiry
    events = [
        StreamEvent("first", first_time, "signup", 1),
        StreamEvent("second", second_time, "signup", 2),
    ]
    reference = KeyedDedupReference(ttl_millis=ttl_millis)
    assert reference.apply(events) == ["first", "second"]
