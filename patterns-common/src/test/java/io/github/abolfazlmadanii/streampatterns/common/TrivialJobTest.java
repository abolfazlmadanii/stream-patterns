package io.github.abolfazlmadanii.streampatterns.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TrivialJobTest {

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  @Test
  void runsOnMiniCluster() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    assertThat(TrivialJob.wire(env).executeAndCollect(1)).containsExactly("stream-patterns");
  }
}
