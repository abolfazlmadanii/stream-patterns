package io.github.abolfazlmadanii.streampatterns.common;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Minimal pipeline used to prove the Flink toolchain and tests run. */
public final class TrivialJob {

  private TrivialJob() {}

  public static DataStream<String> wire(StreamExecutionEnvironment env) {
    return env.fromElements("stream-patterns").map(value -> value);
  }
}
