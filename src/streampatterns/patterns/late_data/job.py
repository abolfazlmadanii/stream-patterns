from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from pyflink.common import Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor

from streampatterns.patterns_common.events import StreamEvent

if TYPE_CHECKING:
    from pyflink.datastream import CloseableIterator

LATE_EVENTS_TAG = OutputTag("late-events", Types.STRING())

DEFAULT_MAX_OUT_OF_ORDERNESS_MS = 5_000
_TIMESTAMP_MIN = -(2**63)


class _StreamEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: StreamEvent, record_timestamp: int) -> int:
        return value.event_time_millis


class _LateDataSplitter(KeyedProcessFunction):
    """Routes records past the bounded-out-of-orderness watermark to the late side output."""

    has_side_output = True

    def __init__(self, max_out_of_orderness_millis: int) -> None:
        self._max_out_of_orderness_millis = max_out_of_orderness_millis
        self._max_event_time_state = None

    def open(self, runtime_context) -> None:
        descriptor = ValueStateDescriptor("max-event-time", Types.LONG())
        self._max_event_time_state = runtime_context.get_state(descriptor)

    def process_element(self, value: StreamEvent, ctx: KeyedProcessFunction.Context):
        timestamp = value.event_time_millis
        max_seen = self._max_event_time_state.value()
        if max_seen is None:
            max_seen = _TIMESTAMP_MIN

        watermark = (
            max_seen - self._max_out_of_orderness_millis
            if max_seen > _TIMESTAMP_MIN
            else _TIMESTAMP_MIN
        )
        if watermark > _TIMESTAMP_MIN and timestamp < watermark:
            yield LATE_EVENTS_TAG, value.event_id
        else:
            yield value.event_id

        self._max_event_time_state.update(max(max_seen, timestamp))


def wire_late_data(
    env: StreamExecutionEnvironment,
    events: list[StreamEvent],
    *,
    max_out_of_orderness_millis: int = DEFAULT_MAX_OUT_OF_ORDERNESS_MS,
) -> tuple[DataStream, DataStream]:
    """Assign event-time watermarks and split very-late records to :data:`LATE_EVENTS_TAG`."""
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_millis(max_out_of_orderness_millis)
        )
        .with_timestamp_assigner(_StreamEventTimestampAssigner())
    )
    timed = env.from_collection(events).assign_timestamps_and_watermarks(watermark_strategy)
    on_time = timed.key_by(lambda _: "all").process(
        _LateDataSplitter(max_out_of_orderness_millis),
        Types.STRING(),
    )
    return on_time, on_time.get_side_output(LATE_EVENTS_TAG)


def collect_late_data(
    on_time_stream: DataStream,
    late_stream: DataStream,
) -> tuple[list[str], list[str]]:
    """Collect on-time and late side outputs in one local execution."""
    tagged = on_time_stream.map(lambda event_id: ("on-time", event_id)).union(
        late_stream.map(lambda event_id: ("late", event_id))
    )
    on_time: list[str] = []
    late: list[str] = []
    for kind, event_id in _materialize(tagged):
        if kind == "on-time":
            on_time.append(event_id)
        else:
            late.append(event_id)
    return on_time, late


def _materialize(stream: DataStream) -> Iterable[tuple[str, str]]:
    iterator: CloseableIterator = stream.execute_and_collect()
    return list(iterator)
