package io.github.abolfazlmadanii.streampatterns.testkit;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;

/**
 * Base for integration tests that need Docker (local only; CI uses MiniCluster instead).
 */
public abstract class ContainersTestBase {

  @BeforeAll
  static void requireDocker() {
    Assumptions.assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is required for this test");
  }
}
