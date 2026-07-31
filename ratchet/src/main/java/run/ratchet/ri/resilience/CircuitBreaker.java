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
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.CircuitBreakerManager;

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
 *   → All calls throw CircuitBreakerOpenException immediately
 *   → After waitDuration expires → HALF_OPEN
 *
 * HALF_OPEN
 *   → Allow up to permittedCallsInHalfOpen calls through concurrently
 *   → If all succeed → CLOSED
 *   → If any fail → OPEN
 * </pre>
 *
 * <p>Once the breaker is OPEN, normal successes cannot close it because calls are rejected.
 * Recovery happens only after the wait duration permits HALF_OPEN probes, or through an explicit
 * {@link #reset()}.
 */
public class CircuitBreaker implements CircuitBreakerManager.Breaker {

  private static final int UNINITIALIZED = -1;

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
  // Identifies the probe round so late completions cannot update a newer round.
  private long halfOpenGeneration;
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
          halfOpenGeneration++;
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

  @Override
  public String stateName() {
    return getState().name();
  }

  /**
   * Executes the task with circuit breaker protection.
   *
   * @throws CircuitBreakerOpenException if the circuit is OPEN
   * @throws Exception if the task throws
   */
  public <T> T execute(Callable<T> task) throws Exception {
    State current = getState();

    if (current == State.OPEN) {
      throw new CircuitBreakerOpenException(
          "Circuit breaker '" + name + "' is OPEN — service unavailable");
    }

    if (current == State.HALF_OPEN) {
      return executeInHalfOpen(task);
    }

    // CLOSED
    return executeInClosed(task);
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
      halfOpenGeneration++;
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

  private <T> T executeInHalfOpen(Callable<T> task) throws Exception {
    long admittedGeneration;
    lock.lock();
    try {
      if (state.get() != State.HALF_OPEN) {
        throw new CircuitBreakerOpenException(
            "Circuit breaker '" + name + "' is no longer accepting this HALF_OPEN trial call");
      }
      int attempt = ++halfOpenAttempts;
      if (attempt > config.permittedCallsInHalfOpen()) {
        throw new CircuitBreakerOpenException(
            "Circuit breaker '" + name + "' is HALF_OPEN — trial calls exhausted");
      }
      admittedGeneration = halfOpenGeneration;
    } finally {
      lock.unlock();
    }

    try {
      T result = task.call();
      boolean transitioned = false;
      lock.lock();
      try {
        if (isCurrentHalfOpenGeneration(admittedGeneration)) {
          int successes = ++halfOpenSuccesses;
          if (successes >= config.permittedCallsInHalfOpen()) {
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
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
      if (transitioned) {
        publishPendingStateNotifications();
      }
      return result;
    } catch (Exception e) {
      boolean transitioned = false;
      lock.lock();
      try {
        if (isCurrentHalfOpenGeneration(admittedGeneration)) {
          transitioned = transitionToOpenUnderLock();
        }
      } finally {
        lock.unlock();
      }
      if (transitioned) {
        publishPendingStateNotifications();
      }
      throw e;
    }
  }

  // Must be called with lock held.
  private boolean isCurrentHalfOpenGeneration(long admittedGeneration) {
    return state.get() == State.HALF_OPEN && halfOpenGeneration == admittedGeneration;
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
    boolean transitioned;
    lock.lock();
    try {
      recordOutcome(0);
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
    } catch (RuntimeException ignored) {
      // Observability must not change circuit-breaker behavior.
    }
  }

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }
}
