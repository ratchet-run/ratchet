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

  private CountDownLatch latch = new CountDownLatch(1);
  private Class<? extends AbstractJobSchedulerEvent> expectedType;

  /** Lock object for synchronizing the check-and-setup sequence in awaitEvent. */
  private final Object awaitLock = new Object();

  public void onEvent(@Observes AbstractJobSchedulerEvent event) {
    events.add(event);
    synchronized (awaitLock) {
      if (expectedType != null && expectedType.isInstance(event)) {
        latch.countDown();
      }
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
    CountDownLatch awaitLatch;
    synchronized (awaitLock) {
      // Check if already received while holding the lock
      if (events.stream().anyMatch(type::isInstance)) {
        return true;
      }

      // Set up latch for future events — atomically with the check above,
      // so no event can slip between the check and the setup
      this.expectedType = type;
      this.latch = new CountDownLatch(1);
      awaitLatch = this.latch;
    }

    return awaitLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Clears all captured events. Call in test setup for isolation. */
  public void clear() {
    events.clear();
    synchronized (awaitLock) {
      expectedType = null;
      latch = new CountDownLatch(1);
    }
  }
}
