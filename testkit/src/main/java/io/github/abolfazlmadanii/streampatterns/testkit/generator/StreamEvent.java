package io.github.abolfazlmadanii.streampatterns.testkit.generator;

import java.util.Objects;

/** Synthetic stream record used by the seeded event generator. */
public final class StreamEvent {

  private final String eventId;
  private final long eventTimeMillis;
  private final String category;
  private final long amountCents;

  public StreamEvent(String eventId, long eventTimeMillis, String category, long amountCents) {
    this.eventId = Objects.requireNonNull(eventId, "eventId");
    this.eventTimeMillis = eventTimeMillis;
    this.category = Objects.requireNonNull(category, "category");
    this.amountCents = amountCents;
  }

  public String eventId() {
    return eventId;
  }

  public long eventTimeMillis() {
    return eventTimeMillis;
  }

  public String category() {
    return category;
  }

  public long amountCents() {
    return amountCents;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamEvent that)) {
      return false;
    }
    return eventTimeMillis == that.eventTimeMillis
        && amountCents == that.amountCents
        && eventId.equals(that.eventId)
        && category.equals(that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventId, eventTimeMillis, category, amountCents);
  }

  @Override
  public String toString() {
    return "StreamEvent{"
        + "eventId='"
        + eventId
        + '\''
        + ", eventTimeMillis="
        + eventTimeMillis
        + ", category='"
        + category
        + '\''
        + ", amountCents="
        + amountCents
        + '}';
  }
}
