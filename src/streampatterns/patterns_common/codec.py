"""Versioned binary codec for :class:`StreamEvent` (no pickle)."""

from __future__ import annotations

import struct

from streampatterns.patterns_common.events import StreamEvent

_CODEC_VERSION = 1
_HEADER = struct.Struct("!B H")


def encode_event(event: StreamEvent) -> bytes:
    event_id = event.event_id.encode("utf-8")
    category = event.category.encode("utf-8")
    if len(event_id) > 0xFFFF or len(category) > 0xFFFF:
        raise ValueError("event_id and category must fit in 65535 UTF-8 bytes")
    body = struct.pack(
        "!qHq",
        event.event_time_millis,
        len(category),
        event.amount_cents,
    )
    return _HEADER.pack(_CODEC_VERSION, len(event_id)) + event_id + body + category


def decode_event(payload: bytes) -> StreamEvent:
    if len(payload) < _HEADER.size:
        raise ValueError("payload too short for event header")
    version, event_id_len = _HEADER.unpack_from(payload)
    if version != _CODEC_VERSION:
        raise ValueError(f"unsupported codec version: {version}")
    offset = _HEADER.size
    end_id = offset + event_id_len
    if end_id > len(payload):
        raise ValueError("truncated event_id")
    event_id = payload[offset:end_id].decode("utf-8")
    offset = end_id
    if offset + struct.calcsize("!qHq") > len(payload):
        raise ValueError("truncated event body")
    event_time_millis, category_len, amount_cents = struct.unpack_from("!qHq", payload, offset)
    offset += struct.calcsize("!qHq")
    end_cat = offset + category_len
    if end_cat > len(payload):
        raise ValueError("truncated category")
    category = payload[offset:end_cat].decode("utf-8")
    return StreamEvent(
        event_id=event_id,
        event_time_millis=event_time_millis,
        category=category,
        amount_cents=amount_cents,
    )
