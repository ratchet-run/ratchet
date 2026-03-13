package run.ratchet.ri.resilience;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lightweight circuit breaker state machine.
 *
 * <p>States: CLOSED → OPEN → HALF_OPEN → CLOSED. Thread-safe via {@link AtomicReference} for state
 * and a simple ring buffer for failure tracking.
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

  private final String name;
  private final CircuitBreakerConfiguration config;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  // Sliding window: ring buffer of outcomes (1 = success, 0 = failure)
  private final AtomicIntegerArray window;
  private final AtomicInteger windowIndex = new AtomicInteger(0);
  private final AtomicInteger totalCalls = new AtomicInteger(0);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  // HALF_OPEN state tracking
  private final AtomicInteger halfOpenSuccesses = new AtomicInteger(0);
  private final AtomicInteger halfOpenAttempts = new AtomicInteger(0);
  // OPEN state timing
  private volatile long openedAtMs;

  public CircuitBreaker(String name, CircuitBreakerConfiguration config) {
    this.name = name;
    this.config = config;
    this.window = new AtomicIntegerArray(config.slidingWindowSize());
  }

  /** Returns the circuit breaker name. */
  public String getName() {
    return name;
  }

  /** Returns the current state. */
  public State getState() {
    State current = state.get();
    // Auto-transition from OPEN to HALF_OPEN if wait duration has elapsed
    if (current == State.OPEN
        && System.currentTimeMillis() - openedAtMs >= config.waitDurationMs()) {
      if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
        halfOpenSuccesses.set(0);
        halfOpenAttempts.set(0);
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

  /** Manually transitions to OPEN state. */
  public void transitionToOpen() {
    openedAtMs = System.currentTimeMillis();
    state.set(State.OPEN);
  }

  /** Resets to CLOSED state, clearing all counters. */
  public void reset() {
    state.set(State.CLOSED);
    openedAtMs = 0L;
    totalCalls.set(0);
    failureCount.set(0);
    windowIndex.set(0);
    halfOpenSuccesses.set(0);
    halfOpenAttempts.set(0);
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

  /** Returns the configured OPEN-state wait duration in milliseconds. */
  public long getWaitDurationMs() {
    return config.waitDurationMs();
  }

  private <T> T executeInHalfOpen(Callable<T> task) throws Exception {
    int attempt = halfOpenAttempts.incrementAndGet();
    if (attempt > config.permittedCallsInHalfOpen()) {
      throw new ServiceUnavailableException(
          "Circuit breaker '" + name + "' is HALF_OPEN — trial calls exhausted");
    }

    try {
      T result = task.call();
      int successes = halfOpenSuccesses.incrementAndGet();
      if (successes >= config.permittedCallsInHalfOpen()) {
        // All trial calls succeeded — transition to CLOSED
        if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
          totalCalls.set(0);
          failureCount.set(0);
          windowIndex.set(0);
        }
      }
      return result;
    } catch (Exception e) {
      // Any failure in HALF_OPEN → back to OPEN
      transitionToOpen();
      throw e;
    }
  }

  private void recordSuccess() {
    int len = window.length();
    int idx = windowIndex.getAndUpdate(i -> (i + 1) % len);
    int previous = window.getAndSet(idx, 1);

    int total = totalCalls.incrementAndGet();
    if (total > len && previous == 0) {
      failureCount.decrementAndGet();
    }
  }

  private void recordFailure() {
    int len = window.length();
    int idx = windowIndex.getAndUpdate(i -> (i + 1) % len);
    int previous = window.getAndSet(idx, 0);

    int total = totalCalls.incrementAndGet();
    if (total <= len || previous == 1) {
      failureCount.incrementAndGet();
    }

    evaluateThreshold();
  }

  private void evaluateThreshold() {
    int total = Math.min(totalCalls.get(), window.length());
    if (total < config.minimumCalls()) {
      return;
    }

    float failureRate = (failureCount.get() * 100.0f) / total;
    if (failureRate >= config.failureRateThreshold()) {
      transitionToOpen();
    }
  }

  /** Circuit breaker states. */
  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }
}
