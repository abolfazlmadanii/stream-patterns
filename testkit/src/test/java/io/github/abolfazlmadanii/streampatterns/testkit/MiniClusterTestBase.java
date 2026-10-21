package io.github.abolfazlmadanii.streampatterns.testkit;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Shared MiniCluster setup for Flink integration tests (no Docker). */
public abstract class MiniClusterTestBase {

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  protected static StreamExecutionEnvironment executionEnvironment() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    return env;
  }
}
