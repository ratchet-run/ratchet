/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.resilience;

import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.CircuitBreakerExceptionFilter;

/**
 * Lightweight circuit breaker state machine.
 *
 * <p>States: CLOSED → OPEN → HALF_OPEN → CLOSED. Thread-safe via {@link ReentrantLock} for
 * sliding-window operations and {@link AtomicReference} for state transitions.
 *
 * <pre>
 * CLOSED (default)
 *   → Track success/failure in sliding window (ring buffer of last N calls)
 *   → Ignored and non-recorded exceptions leave the window untouched
 *   → When failure rate >= threshold AND calls >= minimumCalls → OPEN
 *
 * OPEN
 *   → All calls throw CircuitBreakerOpenException immediately
 *   → After waitDuration expires → HALF_OPEN
 *
 * HALF_OPEN
 *   → Allow up to permittedCallsInHalfOpen calls through concurrently
 *   → If all succeed → CLOSED
 *   → If any recorded exception occurs → OPEN
 *   → Ignored and non-recorded exceptions release their trial permit
 * </pre>
 *
 * <p>Once the breaker is OPEN, normal successes cannot close it because calls are rejected.
 * Recovery happens only after the wait duration permits HALF_OPEN probes, or through an explicit
 * {@link #reset()}.
 *
 * <p>Every state change starts a new period. A call records its outcome only if the breaker is
 * still in the period that admitted it, so a late completion from an earlier period is dropped.
 */
public class CircuitBreaker {

  private static final Logger log = Logger.getLogger(CircuitBreaker.class);
  private static final int UNINITIALIZED = -1;
  private static final long NOT_ADMITTED = -1L;

  private final String name;
  private final CircuitBreakerConfiguration config;
  private final Clock clock;
  private final Consumer<State> stateListener;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private final ReentrantLock lock = new ReentrantLock();
  private final ConcurrentLinkedQueue<State> pendingStateNotifications =
      new ConcurrentLinkedQueue<>();
  private final AtomicBoolean publishingStateNotifications = new AtomicBoolean();
  // Sliding window: ring buffer of outcomes (1 = success, 0 = failure, -1 = uninitialized)
  private final int[] window;
  private int windowIndex;
  private int totalCalls;
  private int failureCount;
  // HALF_OPEN state tracking
  private int halfOpenSuccesses;
  private int halfOpenAttempts;
  // Identifies the current state period. It changes on every transition (and on reset), so a late
  // completion from an earlier CLOSED period or HALF_OPEN round cannot update a newer one.
  private long periodGeneration;
  // OPEN state timing
  private volatile long openedAtMs;

  public CircuitBreaker(String name, CircuitBreakerConfiguration config) {
    this(name, config, Clock.systemUTC(), ignored -> {});
  }

  public CircuitBreaker(String name, CircuitBreakerConfiguration config, Clock clock) {
    this(name, config, clock, ignored -> {});
  }

  CircuitBreaker(String name, CircuitBreakerConfiguration config, Consumer<State> stateListener) {
    this(name, config, Clock.systemUTC(), stateListener);
  }

  CircuitBreaker(
      String name, CircuitBreakerConfiguration config, Clock clock, Consumer<State> stateListener) {
    this.name = name;
    this.config = config;
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.stateListener = stateListener != null ? stateListener : ignored -> {};
    this.window = new int[config.slidingWindowSize()];
    Arrays.fill(this.window, UNINITIALIZED);
  }

  public String getName() {
    return name;
  }

  public State getState() {
    State current = state.get();
    // Auto-transition from OPEN to HALF_OPEN if wait duration has elapsed
    if (current == State.OPEN && clock.millis() - openedAtMs >= config.waitDurationMs()) {
      boolean transitioned = false;
      lock.lock();
      try {
        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
          periodGeneration++;
          halfOpenSuccesses = 0;
          halfOpenAttempts = 0;
          queueStateNotification(State.HALF_OPEN);
          transitioned = true;
        }
      } finally {
        lock.unlock();
      }
      if (transitioned) {
        publishPendingStateNotifications();
      }
      return state.get();
    }
    return current;
  }

  /**
   * Executes the task with circuit breaker protection.
   *
   * @throws CircuitBreakerOpenException if the circuit is OPEN
   * @throws Exception if the task throws
   */
  public <T> T execute(Callable<T> task) throws Exception {
    return execute(task, null);
  }

  /**
   * Executes a task with the union of configuration and per-call exception filters.
   *
   * @param task the task to execute
   * @param callFilter the per-call filter, or null
   * @param <T> the result type
   * @return the task result
   * @throws CircuitBreakerOpenException if the call is rejected
   * @throws Exception the unchanged task exception
   */
  public <T> T execute(Callable<T> task, CircuitBreakerExceptionFilter callFilter)
      throws Exception {
    CircuitBreakerExceptionFilter filter = config.exceptionFilter().merge(callFilter);
    State current = getState();

    if (current == State.OPEN) {
      throw new CircuitBreakerOpenException(
          "Circuit breaker '" + name + "' is OPEN — service unavailable");
    }

    if (current == State.HALF_OPEN) {
      return executeInHalfOpen(task, filter);
    }

    long admittedGeneration = admitClosedCall();
    if (admittedGeneration == NOT_ADMITTED) {
      // The breaker left CLOSED after the state read; dispatch again against the new state.
      return execute(task, callFilter);
    }
    return executeInClosed(task, filter, admittedGeneration);
  }

  public void transitionToOpen() {
    boolean transitioned;
    lock.lock();
    try {
      transitioned = transitionToOpenUnderLock();
    } finally {
      lock.unlock();
    }
    if (transitioned) {
      publishPendingStateNotifications();
    }
  }

  public void reset() {
    boolean transitioned = false;
    lock.lock();
    try {
      openedAtMs = 0L;
      totalCalls = 0;
      failureCount = 0;
      windowIndex = 0;
      halfOpenSuccesses = 0;
      halfOpenAttempts = 0;
      periodGeneration++;
      Arrays.fill(window, UNINITIALIZED);
      State previous = state.getAndSet(State.CLOSED);
      if (previous != State.CLOSED) {
        queueStateNotification(State.CLOSED);
        transitioned = true;
      }
    } finally {
      lock.unlock();
    }
    if (transitioned) {
      publishPendingStateNotifications();
    }
  }

  public long getWaitDurationMs() {
    return config.waitDurationMs();
  }

  public long getRemainingWaitDurationMs() {
    if (getState() != State.OPEN) {
      return 0L;
    }
    return Math.max(0L, openedAtMs + config.waitDurationMs() - clock.millis());
  }

  // Captures the CLOSED period under the lock so the state check and the capture are atomic.
  private long admitClosedCall() {
    lock.lock();
    try {
      return state.get() == State.CLOSED ? periodGeneration : NOT_ADMITTED;
    } finally {
      lock.unlock();
    }
  }

  private <T> T executeInClosed(
      Callable<T> task, CircuitBreakerExceptionFilter filter, long admittedGeneration)
      throws Exception {
    T result;
    try {
      result = task.call();
    } catch (Throwable e) {
      // Errors are classified too; precise rethrow keeps the same instance and the Exception
      // signature.
      switch (filter.classify(e)) {
        case RECORDED -> recordOutcome(false, admittedGeneration);
        case NOT_RECORDED, IGNORED -> {}
      }
      throw e;
    }
    // Accounting and listener calls run outside the try so only the task's outcome is classified.
    recordOutcome(true, admittedGeneration);
    return result;
  }

  private <T> T executeInHalfOpen(Callable<T> task, CircuitBreakerExceptionFilter filter)
      throws Exception {
    long admittedGeneration;
    lock.lock();
    try {
      if (state.get() != State.HALF_OPEN) {
        throw new CircuitBreakerOpenException(
            "Circuit breaker '" + name + "' is no longer accepting this HALF_OPEN trial call");
      }
      if (halfOpenAttempts >= config.permittedCallsInHalfOpen()) {
        throw new CircuitBreakerOpenException(
            "Circuit breaker '" + name + "' is HALF_OPEN — trial calls exhausted");
      }
      halfOpenAttempts++;
      admittedGeneration = periodGeneration;
    } finally {
      lock.unlock();
    }

    T result;
    try {
      result = task.call();
    } catch (Throwable e) {
      // Every throwable releases or settles its trial permit before the same instance is rethrown.
      boolean transitioned = false;
      boolean recorded = filter.classify(e) == CircuitBreakerExceptionFilter.Outcome.RECORDED;
      lock.lock();
      try {
        if (isCurrentPeriod(State.HALF_OPEN, admittedGeneration)) {
          if (recorded) {
            transitioned = transitionToOpenUnderLock();
          } else {
            halfOpenAttempts--;
          }
        }
      } finally {
        lock.unlock();
      }
      if (transitioned) {
        publishPendingStateNotifications();
      }
      throw e;
    }
    // Accounting and listener calls run outside the try so only the task's outcome is classified.
    if (onHalfOpenSuccess(admittedGeneration)) {
      publishPendingStateNotifications();
    }
    return result;
  }

  private boolean onHalfOpenSuccess(long admittedGeneration) {
    boolean transitioned = false;
    lock.lock();
    try {
      if (isCurrentPeriod(State.HALF_OPEN, admittedGeneration)) {
        int successes = ++halfOpenSuccesses;
        if (successes >= config.permittedCallsInHalfOpen()) {
          if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            periodGeneration++;
            totalCalls = 0;
            failureCount = 0;
            windowIndex = 0;
            Arrays.fill(window, UNINITIALIZED);
            queueStateNotification(State.CLOSED);
            transitioned = true;
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return transitioned;
  }

  // Must be called with lock held.
  private boolean isCurrentPeriod(State expected, long admittedGeneration) {
    return state.get() == expected && periodGeneration == admittedGeneration;
  }

  // Records one CLOSED-state outcome and evaluates the failure-rate threshold after every outcome,
  // so the call that reaches minimumCalls can open the breaker whether it succeeded or failed.
  // An outcome from a call admitted in an earlier period is dropped without touching the window.
  private void recordOutcome(boolean success, long admittedGeneration) {
    boolean transitioned;
    lock.lock();
    try {
      if (!isCurrentPeriod(State.CLOSED, admittedGeneration)) {
        return;
      }
      recordInWindow(success ? 1 : 0);
      int snapshotTotal = Math.min(totalCalls, window.length);
      int snapshotFailures = failureCount;
      transitioned = evaluateThreshold(snapshotTotal, snapshotFailures);
    } finally {
      lock.unlock();
    }
    if (transitioned) {
      publishPendingStateNotifications();
    }
  }

  // Must be called with lock held.
  private void recordInWindow(int outcome) {
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

  private boolean evaluateThreshold(int total, int failures) {
    if (total < config.minimumCalls()) {
      return false;
    }

    float failureRate = (failures * 100.0f) / total;
    if (failureRate >= config.failureRateThreshold()) {
      return transitionToOpenUnderLock();
    }
    return false;
  }

  // Must be called with lock held.
  private boolean transitionToOpenUnderLock() {
    State current = state.get();
    if (current == State.OPEN) {
      return false;
    }
    // Write openedAtMs BEFORE the CAS so any thread that sees state==OPEN via getState()
    // also sees a valid timestamp. The lock prevents a concurrent reset() from clearing
    // openedAtMs between the write and the CAS.
    openedAtMs = clock.millis();
    if (state.compareAndSet(current, State.OPEN)) {
      periodGeneration++;
      queueStateNotification(State.OPEN);
      return true;
    }
    return false;
  }

  // Must be called with lock held so queue order matches state-transition order.
  private void queueStateNotification(State newState) {
    pendingStateNotifications.add(newState);
  }

  private void publishPendingStateNotifications() {
    if (!publishingStateNotifications.compareAndSet(false, true)) {
      return;
    }

    boolean continuePublishing;
    do {
      try {
        State next;
        while ((next = pendingStateNotifications.poll()) != null) {
          notifyState(next);
        }
      } finally {
        publishingStateNotifications.set(false);
      }
      continuePublishing =
          !pendingStateNotifications.isEmpty()
              && publishingStateNotifications.compareAndSet(false, true);
    } while (continuePublishing);
  }

  private void notifyState(State newState) {
    try {
      stateListener.accept(newState);
    } catch (Throwable listenerFailure) {
      // Observability must not change circuit-breaker behavior, so even an Error is contained.
      log.warnf(
          listenerFailure,
          "Circuit breaker '%s' state listener failed for transition to %s",
          name,
          newState);
    }
  }

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }
}
