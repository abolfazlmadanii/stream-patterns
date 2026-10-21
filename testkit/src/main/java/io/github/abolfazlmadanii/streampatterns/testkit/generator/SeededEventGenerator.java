package io.github.abolfazlmadanii.streampatterns.testkit.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/** Produces repeatable {@link StreamEvent} sequences for a fixed seed. */
public final class SeededEventGenerator {

  private static final String[] CATEGORIES = {"click", "purchase", "refund", "signup"};

  private final SplittableRandom random;
  private long sequence;

  public SeededEventGenerator(long seed) {
    this.random = new SplittableRandom(seed);
    this.sequence = 0L;
  }

  public StreamEvent next() {
    long index = sequence++;
    String eventId = "evt-" + index;
    long eventTimeMillis = 1_700_000_000_000L + index * 1_000L;
    String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
    long amountCents = random.nextLong(1L, 10_000L);
    return new StreamEvent(eventId, eventTimeMillis, category, amountCents);
  }

  public List<StreamEvent> next(int count) {
    if (count < 0) {
      throw new IllegalArgumentException("count must be non-negative");
    }
    List<StreamEvent> events = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      events.add(next());
    }
    return events;
  }
}
