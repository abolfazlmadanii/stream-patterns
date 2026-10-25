from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows

from streampatterns.patterns_common.events import StreamEvent

if TYPE_CHECKING:
    from pyflink.datastream import CloseableIterator

LATE_EVENTS_TAG = OutputTag("late-events", Types.STRING())

DEFAULT_MAX_OUT_OF_ORDERNESS_MS = 5_000
DEFAULT_WINDOW_SIZE_MS = 10_000


class _StreamEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: StreamEvent, record_timestamp: int) -> int:
        return value.event_time_millis


def wire_late_data(
    env: StreamExecutionEnvironment,
    events: list[StreamEvent],
    *,
    max_out_of_orderness_millis: int = DEFAULT_MAX_OUT_OF_ORDERNESS_MS,
    window_size_millis: int = DEFAULT_WINDOW_SIZE_MS,
) -> tuple[DataStream, DataStream]:
    """Event-time windows with bounded lateness; late records go to :data:`LATE_EVENTS_TAG`."""
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_millis(max_out_of_orderness_millis)
        )
        .with_timestamp_assigner(_StreamEventTimestampAssigner())
    )
    timed = env.from_collection(events).assign_timestamps_and_watermarks(watermark_strategy)
    windowed = (
        timed.map(lambda event: event.event_id)
        .key_by(lambda _: "all")
        .window(TumblingEventTimeWindows.of(Time.milliseconds(window_size_millis)))
        .allowed_lateness(0)
        .side_output_late_data(LATE_EVENTS_TAG)
        .reduce(lambda left, right: left)
    )
    return windowed, windowed.get_side_output(LATE_EVENTS_TAG)


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
