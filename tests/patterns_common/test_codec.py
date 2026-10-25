from streampatterns.patterns_common.codec import decode_event, encode_event
from streampatterns.patterns_common.events import StreamEvent


def test_roundtrip_codec() -> None:
    event = StreamEvent("evt-1", 1_700_000_000_000, "click", 42)
    assert decode_event(encode_event(event)) == event
