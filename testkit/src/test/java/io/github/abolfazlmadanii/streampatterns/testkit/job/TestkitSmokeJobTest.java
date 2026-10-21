package io.github.abolfazlmadanii.streampatterns.testkit.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abolfazlmadanii.streampatterns.testkit.MiniClusterTestBase;
import io.github.abolfazlmadanii.streampatterns.testkit.generator.SeededEventGenerator;
import io.github.abolfazlmadanii.streampatterns.testkit.generator.StreamEvent;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class TestkitSmokeJobTest extends MiniClusterTestBase {

  @Test
  void tagsEventsOnMiniCluster() throws Exception {
    List<StreamEvent> events = new SeededEventGenerator(7L).next(4);
    List<String> expected =
        events.stream().map(e -> e.eventId() + ":" + e.category()).toList();

    StreamExecutionEnvironment env = executionEnvironment();
    List<String> result = TestkitSmokeJob.wire(env, events).executeAndCollect(expected.size());

    assertThat(result).containsExactlyElementsOf(expected);
  }
}
