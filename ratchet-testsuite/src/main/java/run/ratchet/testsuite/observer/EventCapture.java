package run.ratchet.testsuite.observer;

import run.ratchet.api.event.AbstractJobSchedulerEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * CDI event observer that captures all scheduler events for test verification.
 *
 * <p>Thread-safe — events can arrive from any thread. Provides {@link #awaitEvent(Class, Duration)}
 * for Awaitility-style waiting.
 */
@ApplicationScoped
public class EventCapture {

  private final CopyOnWriteArrayList<AbstractJobSchedulerEvent> events =
      new CopyOnWriteArrayList<>();

  private volatile CountDownLatch latch = new CountDownLatch(1);
  private volatile Class<? extends AbstractJobSchedulerEvent> expectedType;

  public void onEvent(@Observes AbstractJobSchedulerEvent event) {
    events.add(event);
    if (expectedType != null && expectedType.isInstance(event)) {
      latch.countDown();
    }
  }

  /** Returns all captured events. */
  public List<AbstractJobSchedulerEvent> getEvents() {
    return List.copyOf(events);
  }

  /** Returns captured events of a specific type. */
  @SuppressWarnings("unchecked")
  public <T extends AbstractJobSchedulerEvent> List<T> getEvents(Class<T> type) {
    return events.stream().filter(type::isInstance).map(e -> (T) e).toList();
  }

  /**
   * Waits for an event of the specified type to be captured.
   *
   * @param type the event type to wait for
   * @param timeout maximum wait duration
   * @return true if the event was captured within the timeout
   * @throws InterruptedException if the wait is interrupted
   */
  public boolean awaitEvent(Class<? extends AbstractJobSchedulerEvent> type, Duration timeout)
      throws InterruptedException {
    // Check if already received
    if (events.stream().anyMatch(type::isInstance)) {
      return true;
    }

    // Set up latch for future events
    this.expectedType = type;
    this.latch = new CountDownLatch(1);

    // Double-check after setting up latch (event may have arrived between check and setup)
    if (events.stream().anyMatch(type::isInstance)) {
      return true;
    }

    return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Clears all captured events. Call in test setup for isolation. */
  public void clear() {
    events.clear();
    expectedType = null;
    latch = new CountDownLatch(1);
  }
}
