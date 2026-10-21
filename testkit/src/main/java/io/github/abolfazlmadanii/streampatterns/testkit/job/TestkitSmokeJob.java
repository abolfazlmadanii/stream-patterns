package io.github.abolfazlmadanii.streampatterns.testkit.job;

import io.github.abolfazlmadanii.streampatterns.testkit.generator.StreamEvent;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Minimal pipeline that tags each {@link StreamEvent} to prove testkit wiring. */
public final class TestkitSmokeJob {

  private TestkitSmokeJob() {}

  public static DataStream<String> wire(StreamExecutionEnvironment env, List<StreamEvent> events) {
    return env.fromCollection(events).map(event -> event.eventId() + ":" + event.category());
  }
}
