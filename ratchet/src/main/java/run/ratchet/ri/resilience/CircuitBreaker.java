package run.ratchet.ri.resilience;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lightweight circuit breaker state machine.
 *
 * <p>States: CLOSED → OPEN → HALF_OPEN → CLOSED. Thread-safe via {@link ReentrantLock} for
 * sliding-window operations and {@link AtomicReference} for state transitions.
 *
 * <pre>
 * CLOSED (default)
 *   → Track success/failure in sliding window (ring buffer of last N calls)
 *   → When failure rate >= threshold AND calls >= minimumCalls → OPEN
 *
 * OPEN
 *   → All calls throw ServiceUnavailableException immediately
 *   → After waitDuration expires → HALF_OPEN
 *
 * HALF_OPEN
 *   → Allow permittedCallsInHalfOpen calls through
 *   → If all succeed → CLOSED
 *   → If any fail → OPEN
 * </pre>
 */
public class CircuitBreaker {

  private static final int UNINITIALIZED = -1;

  private final String name;
  private final CircuitBreakerConfiguration config;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private final ReentrantLock lock = new ReentrantLock();
  // Sliding window: ring buffer of outcomes (1 = success, 0 = failure, -1 = uninitialized)
  private final int[] window;
  private int windowIndex;
  private int totalCalls;
  private int failureCount;
  // HALF_OPEN state tracking
  private int halfOpenSuccesses;
  private int halfOpenAttempts;
  // OPEN state timing
  private volatile long openedAtMs;

  public CircuitBreaker(String name, CircuitBreakerConfiguration config) {
    this.name = name;
    this.config = config;
    this.window = new int[config.slidingWindowSize()];
    Arrays.fill(this.window, UNINITIALIZED);
  }

  public String getName() {
    return name;
  }

  public State getState() {
    State current = state.get();
    // Auto-transition from OPEN to HALF_OPEN if wait duration has elapsed
    if (current == State.OPEN
        && System.currentTimeMillis() - openedAtMs >= config.waitDurationMs()) {
      lock.lock();
      try {
        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
          halfOpenSuccesses = 0;
          halfOpenAttempts = 0;
        }
      } finally {
        lock.unlock();
      }
      return state.get();
    }
    return current;
  }

  /**
   * Executes the task with circuit breaker protection.
   *
   * @throws ServiceUnavailableException if the circuit is OPEN
   * @throws Exception if the task throws
   */
  public <T> T execute(Callable<T> task) throws Exception {
    State current = getState();

    if (current == State.OPEN) {
      throw new ServiceUnavailableException(
          "Circuit breaker '" + name + "' is OPEN — service unavailable");
    }

    if (current == State.HALF_OPEN) {
      return executeInHalfOpen(task);
    }

    // CLOSED
    return executeInClosed(task);
  }

  public void transitionToOpen() {
    lock.lock();
    try {
      State current = state.get();
      if (current == State.OPEN) {
        return;
      }
      // Write openedAtMs BEFORE the CAS so any thread that sees state==OPEN via getState()
      // also sees a valid timestamp. The lock prevents a concurrent reset() from clearing
      // openedAtMs between the write and the CAS.
      openedAtMs = System.currentTimeMillis();
      state.compareAndSet(current, State.OPEN);
    } finally {
      lock.unlock();
    }
  }

  public void reset() {
    lock.lock();
    try {
      openedAtMs = 0L;
      totalCalls = 0;
      failureCount = 0;
      windowIndex = 0;
      halfOpenSuccesses = 0;
      halfOpenAttempts = 0;
      Arrays.fill(window, UNINITIALIZED);
      state.set(State.CLOSED);
    } finally {
      lock.unlock();
    }
  }

  private <T> T executeInClosed(Callable<T> task) throws Exception {
    try {
      T result = task.call();
      recordSuccess();
      return result;
    } catch (Exception e) {
      recordFailure();
      throw e;
    }
  }

  public long getWaitDurationMs() {
    return config.waitDurationMs();
  }

  private <T> T executeInHalfOpen(Callable<T> task) throws Exception {
    lock.lock();
    try {
      int attempt = ++halfOpenAttempts;
      if (attempt > config.permittedCallsInHalfOpen()) {
        throw new ServiceUnavailableException(
            "Circuit breaker '" + name + "' is HALF_OPEN — trial calls exhausted");
      }
    } finally {
      lock.unlock();
    }

    try {
      T result = task.call();
      lock.lock();
      try {
        int successes = ++halfOpenSuccesses;
        if (successes >= config.permittedCallsInHalfOpen()) {
          if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            totalCalls = 0;
            failureCount = 0;
            windowIndex = 0;
            Arrays.fill(window, UNINITIALIZED);
          }
        }
      } finally {
        lock.unlock();
      }
      return result;
    } catch (Exception e) {
      transitionToOpen();
      throw e;
    }
  }

  private void recordSuccess() {
    lock.lock();
    try {
      recordOutcome(1);
    } finally {
      lock.unlock();
    }
  }

  private void recordFailure() {
    int snapshotTotal;
    int snapshotFailures;
    lock.lock();
    try {
      recordOutcome(0);
      snapshotTotal = Math.min(totalCalls, window.length);
      snapshotFailures = failureCount;
    } finally {
      lock.unlock();
    }
    evaluateThreshold(snapshotTotal, snapshotFailures);
  }

  // Must be called with lock held.
  private void recordOutcome(int outcome) {
    int len = window.length;
    int idx = windowIndex;
    windowIndex = (idx + 1) % len;
    int previous = window[idx];
    window[idx] = outcome;
    totalCalls++;

    if (outcome == 1) {
      // success: evicting a failure shrinks failure count
      if (totalCalls > len && previous == 0) {
        failureCount--;
      }
    } else {
      // failure: filling new slot or evicting a success grows failure count
      if (totalCalls <= len && previous == UNINITIALIZED) {
        failureCount++;
      } else if (totalCalls > len && previous == 1) {
        failureCount++;
      }
    }
  }

  private void evaluateThreshold(int total, int failures) {
    if (total < config.minimumCalls()) {
      return;
    }

    float failureRate = (failures * 100.0f) / total;
    if (failureRate >= config.failureRateThreshold()) {
      transitionToOpen();
    }
  }

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }
}
