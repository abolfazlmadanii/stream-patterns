package io.github.abolfazlmadanii.streampatterns.testkit.generator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SeededEventGeneratorTest {

  @Test
  void sameSeedProducesIdenticalSequence() {
    long seed = 42L;

    assertThat(new SeededEventGenerator(seed).next(5))
        .isEqualTo(new SeededEventGenerator(seed).next(5));
  }

  @Test
  void differentSeedsDiverge() {
    assertThat(new SeededEventGenerator(1L).next(3))
        .isNotEqualTo(new SeededEventGenerator(2L).next(3));
  }
}
