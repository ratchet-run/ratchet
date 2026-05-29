package run.ratchet.tck.coordinator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.JobWakeupHint;

/**
 * Wakeup listener that records every callback for later assertion and supports signal-driven
 * waiting for a minimum number of deliveries.
 *
 * <p>Listeners are invoked from transport-owned threads. Waiting threads in tests use {@link
 * #awaitOne}, {@link #awaitCount}, or {@link #awaitAtLeast} to block until the expected number of
 * deliveries arrives; a missed condition raises {@link AssertionError} so the failing test stops at
 * the unmet expectation rather than a downstream consequence.
 */
public final class RecordingWakeupListener implements Consumer<JobWakeupHint> {

  private final List<Record> received = new CopyOnWriteArrayList<>();
  private final Object signal = new Object();

  /** A single recorded delivery. */
  public record Record(
      JobPriority priority, NodeIdentity source, String executionTarget, Instant at) {}

  @Override
  public void accept(JobWakeupHint hint) {
    received.add(new Record(hint.priority(), hint.source(), hint.executionTarget(), Instant.now()));
    synchronized (signal) {
      signal.notifyAll();
    }
  }

  /** Snapshot of deliveries observed so far. The returned list is independent of the recorder. */
  public List<Record> received() {
    return List.copyOf(received);
  }

  /** Block until at least one delivery has been observed or the timeout elapses. */
  public void awaitOne(Duration timeout) {
    awaitAtLeast(1, timeout);
  }

  /**
   * Block until exactly {@code n} deliveries have been observed.
   *
   * <p>Waits up to {@code timeout} for at least {@code n} deliveries, then settles for {@link
   * #OVERSHOOT_SETTLE} so an in-flight extra delivery has time to land. Throws {@link
   * AssertionError} on either undershoot (fewer than {@code n} within the timeout) or overshoot
   * (more than {@code n} once the settle window has elapsed). The overshoot check is what makes
   * this distinct from {@link #awaitAtLeast}: a coordinator that double-delivers must fail here.
   */
  public void awaitCount(int n, Duration timeout) {
    awaitAtLeast(n, timeout);
    try {
      Thread.sleep(OVERSHOOT_SETTLE.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted awaiting overshoot settle", e);
    }
    int observed = received.size();
    if (observed != n) {
      throw new AssertionError(
          "expected exactly "
              + n
              + " deliveries (settled "
              + OVERSHOOT_SETTLE
              + " after reaching the target) but observed "
              + observed);
    }
  }

  /** Block until at least {@code n} deliveries have been observed or the timeout elapses. */
  public void awaitAtLeast(int n, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    synchronized (signal) {
      while (received.size() < n) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
          throw new AssertionError(
              "expected at least "
                  + n
                  + " deliveries within "
                  + timeout
                  + " but observed "
                  + received.size());
        }
        try {
          signal.wait(Math.max(1L, remaining / 1_000_000L));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("interrupted awaiting deliveries", e);
        }
      }
    }
  }

  /**
   * Window awaited after the target count is reached, used by {@link #awaitCount} to detect a
   * trailing duplicate delivery. Short enough not to slow happy-path TCK runs; long enough to catch
   * a double-publish on a typical in-process transport.
   */
  private static final Duration OVERSHOOT_SETTLE = Duration.ofMillis(100);
}
