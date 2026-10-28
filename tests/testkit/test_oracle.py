from streampatterns.patterns.late_data.reference import LateDataOracle
from streampatterns.patterns_common.events import StreamEvent


def test_late_data_oracle_partitions_late_event() -> None:
    events = [
        StreamEvent("on-1", 9_000, "click", 1),
        StreamEvent("on-2", 20_000, "click", 2),
        StreamEvent("late-1", 1_000, "click", 3),
    ]
    oracle = LateDataOracle(max_out_of_orderness_millis=5_000)
    on_time, late = oracle.partition(events)
    assert on_time == ["on-1", "on-2"]
    assert late == ["late-1"]
