package run.ratchet.tck.api;

import java.time.Instant;
import java.util.Objects;
import run.ratchet.api.JobHandle;

/**
 * Immutable record of a single observation made by a {@link RatchetTckProbe} for a particular
 * {@link JobHandle}.
 *
 * <p>Probes translate scheduler events into {@code ProbeEvent}s so contracts can assert on
 * lifecycle ordering without coupling to {@code ratchet-api}'s event hierarchy. The event types
 * here are intentionally narrower than the full set in {@code run.ratchet.api.event}; a contract
 * that needs a richer signal can use the dedicated {@code await*} or {@code invocationCount}
 * methods on the probe.
 */
public record ProbeEvent(Type type, Instant timestamp) {

  public ProbeEvent(Type type, Instant timestamp) {
    this.type = Objects.requireNonNull(type, "type");
    this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ProbeEvent that)) return false;
    return type == that.type && timestamp.equals(that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, timestamp);
  }

  @Override
  public String toString() {
    return "ProbeEvent{" + type + " @ " + timestamp + '}';
  }

  /** Event types observable through the probe. */
  public enum Type {
    /** Job execution started — task body invoked. Maps to {@code JobStartedEvent}. */
    STARTED,
    /** Job completed successfully. Maps to {@code JobCompletedEvent}. */
    COMPLETED,
    /** Job threw an exception. Maps to {@code JobFailedEvent}. */
    FAILED,
    /** Job was cancelled. Maps to {@code JobCancelledEvent}. */
    CANCELLED,
    /** Job is being retried. Maps to {@code JobRetryingEvent}. */
    RETRYING
  }
}
