from pyflink.datastream import StreamExecutionEnvironment

from streampatterns.patterns.late_data.job import collect_late_data, wire_late_data
from streampatterns.patterns.late_data.reference import LateDataOracle
from streampatterns.patterns_common.events import StreamEvent


def test_late_data_smoke_on_time_events(require_jdk: None) -> None:
    events = [
        StreamEvent("on-1", 9_000, "click", 1),
        StreamEvent("on-2", 20_000, "purchase", 2),
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    on_time_stream, late_stream = wire_late_data(env, events)
    on_time, late = collect_late_data(on_time_stream, late_stream)
    assert on_time == ["on-1", "on-2"]
    assert late == []


def test_late_data_routes_very_late_record_to_side_output(require_jdk: None) -> None:
    events = [
        StreamEvent("on-1", 9_000, "click", 1),
        StreamEvent("on-2", 20_000, "purchase", 2),
        StreamEvent("late-1", 1_000, "refund", 3),
    ]
    oracle = LateDataOracle(max_out_of_orderness_millis=5_000)
    expected_on_time, expected_late = oracle.partition(events)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    on_time_stream, late_stream = wire_late_data(env, events)
    on_time, late = collect_late_data(on_time_stream, late_stream)

    assert expected_on_time == ["on-1", "on-2"]
    assert expected_late == ["late-1"]
    assert on_time == expected_on_time
    assert late == expected_late
