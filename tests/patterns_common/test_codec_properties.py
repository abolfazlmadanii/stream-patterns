from hypothesis import given
from hypothesis import strategies as st

from streampatterns.patterns_common.codec import decode_event, encode_event
from streampatterns.patterns_common.events import StreamEvent

_categories = st.sampled_from(("click", "purchase", "refund", "signup"))


@st.composite
def stream_events(draw: st.DrawFn) -> StreamEvent:
    event_id = draw(st.from_regex(r"[a-z0-9-]{1,32}", fullmatch=True))
    category = draw(_categories)
    return StreamEvent(
        event_id=event_id,
        event_time_millis=draw(st.integers(min_value=0, max_value=2**62 - 1)),
        category=category,
        amount_cents=draw(st.integers(min_value=0, max_value=10_000_000)),
    )


@given(stream_events())
def test_codec_roundtrip(event: StreamEvent) -> None:
    assert decode_event(encode_event(event)) == event
