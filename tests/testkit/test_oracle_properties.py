from hypothesis import given
from hypothesis import strategies as st

from streampatterns.patterns.late_data.reference import LateDataOracle
from streampatterns.patterns_common.events import StreamEvent

_categories = st.sampled_from(("click", "purchase", "refund", "signup"))


@st.composite
def event_lists(draw: st.DrawFn) -> list[StreamEvent]:
    count = draw(st.integers(min_value=0, max_value=20))
    events: list[StreamEvent] = []
    for index in range(count):
        events.append(
            StreamEvent(
                event_id=f"evt-{index}",
                event_time_millis=draw(st.integers(min_value=0, max_value=2_000_000)),
                category=draw(_categories),
                amount_cents=draw(st.integers(min_value=0, max_value=10_000)),
            )
        )
    return events


@given(event_lists(), st.integers(min_value=0, max_value=60_000))
def test_oracle_partition_is_partition(events: list[StreamEvent], bound_ms: int) -> None:
    oracle = LateDataOracle(max_out_of_orderness_millis=bound_ms)
    on_time, late = oracle.partition(events)
    combined = on_time + late
    assert len(combined) == len(events)
    assert len(combined) == len(set(combined))
    assert sorted(combined) == sorted(event.event_id for event in events)
