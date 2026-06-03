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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.CircuitBreakerOpenException;

class CircuitBreakerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");

  private MutableClock clock;
  private CircuitBreaker breaker;

  @BeforeEach
  void setUp() {
    clock = new MutableClock(FIXED_NOW);
    breaker =
        new CircuitBreaker(
            "test-service", new CircuitBreakerConfiguration(50.0f, 4, 100L, 2, 2), clock);
  }

  @Test
  void startsInClosedState() {
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void successfulCallsKeepClosed() throws Exception {
    for (int i = 0; i < 10; i++) {
      String result = breaker.execute(() -> "ok");
      assertEquals("ok", result);
    }
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void failuresBelowThresholdKeepClosed() {
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("fail");
                }));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void agedOutFailuresDoNotReopenWhenSlidingWindowWraps() throws Exception {
    // Window=4, threshold=50%, minimumCalls=2. Drive only through execute(), never opening,
    // so the ring buffer wraps in steady state and the failure-evicted-by-success eviction
    // path runs. A broken decrement would leave failureCount stale and spuriously reopen.

    // Phase 1: fill the window just below threshold -> [F,S,S,S] = 25%. The lone failure is
    // recorded first: at totalCalls=1 the minimumCalls=2 gate skips evaluation, so a window
    // that is momentarily 100%-of-one cannot open the breaker.
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("warmup-failure");
                }));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Phase 2: four successes wrap the buffer and evict the original failure -> [S,S,S,S].
    // failureCount must be decremented to 0 by the eviction math.
    for (int i = 0; i < 4; i++) {
      assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    }
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Phase 3: one late failure -> live window [F,S,S,S] = 25% < 50%. The breaker must stay
    // CLOSED. If the eviction decrement had been dropped, failureCount would read high here and
    // the breaker would open on a healthy service.
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("late-failure");
                }));
    assertEquals(
        CircuitBreaker.State.CLOSED,
        breaker.getState(),
        "aged-out failures must not reopen a breaker whose live window is below threshold");
  }

  @Test
  void freshFailuresOpenWhenTheyEvictSuccessesAcrossTheWindowBoundary() throws Exception {
    // Mirror of the eviction logic: fill with successes, then age them out with failures until
    // the LIVE window crosses threshold. Exercises the failure-evicts-success increment path,
    // which only runs once totalCalls > slidingWindowSize.

    // Fill the window: [S,S,S,S], failureCount=0.
    for (int i = 0; i < 4; i++) {
      assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    }
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // First wrapping failure evicts a success -> [F,S,S,S] = 25% < 50%, still CLOSED.
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("fail-1");
                }));
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Second wrapping failure evicts another success -> [F,F,S,S] = 50% >= 50%, opens (>=).
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("fail-2");
                }));
    assertEquals(
        CircuitBreaker.State.OPEN,
        breaker.getState(),
        "the breaker must open exactly when the live window reaches the failure threshold");
  }

  @Test
  void failuresAboveThresholdOpenCircuit() {
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("fail1");
                }));
    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("fail2");
                }));

    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  void openCircuitRejectsCalls() {
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    assertThrows(CircuitBreakerOpenException.class, () -> breaker.execute(() -> "should not run"));
  }

  @Test
  void openTransitionsToHalfOpenAfterWaitDuration() {
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
  }

  @Test
  void halfOpenSuccessTransitionsToClosed() throws Exception {
    breaker.transitionToOpen();
    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    breaker.execute(() -> "ok1");
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
    breaker.execute(() -> "ok2");

    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void halfOpenExhaustionRejectsExtraConcurrentTrialCalls() throws Exception {
    breaker.transitionToOpen();
    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first = executor.submit(() -> blockingHalfOpenCall(started, release));
      Future<String> second = executor.submit(() -> blockingHalfOpenCall(started, release));
      assertTrue(started.await(1, java.util.concurrent.TimeUnit.SECONDS));

      CircuitBreakerOpenException thrown =
          assertThrows(CircuitBreakerOpenException.class, () -> breaker.execute(() -> "extra"));
      assertTrue(thrown.getMessage().contains("HALF_OPEN"));

      release.countDown();
      assertEquals("ok", first.get());
      assertEquals("ok", second.get());
      assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void halfOpenSuccessTransitionsToClosedAndResetsSlidingWindow() throws Exception {
    breaker.transitionToOpen();
    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    breaker.execute(() -> "ok1");
    breaker.execute(() -> "ok2");
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("new failure");
                }));
    assertEquals(
        CircuitBreaker.State.CLOSED,
        breaker.getState(),
        "a reset sliding window should not open after one new failure");
  }

  @Test
  void halfOpenFailureTransitionsBackToOpen() {
    breaker.transitionToOpen();
    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("still broken");
                }));

    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  void halfOpenFailureAfterPartialSuccessTransitionsBackToOpen() throws Exception {
    breaker.transitionToOpen();
    clock.advance(Duration.ofMillis(100));
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    breaker.execute(() -> "ok");
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    assertThrows(
        RuntimeException.class,
        () ->
            breaker.execute(
                () -> {
                  throw new RuntimeException("still broken");
                }));

    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  void resetClearsStateToClose() {
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    breaker.reset();
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void nameIsPreserved() {
    assertEquals("test-service", breaker.getName());
  }

  @Test
  void configurationRejectsInvalidValues() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerConfiguration(-1.0f, 4, 100L, 2, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerConfiguration(101.0f, 4, 100L, 2, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerConfiguration(50.0f, 0, 100L, 2, 2));
    assertThrows(
        IllegalArgumentException.class, () -> new CircuitBreakerConfiguration(50.0f, 4, -1L, 2, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerConfiguration(50.0f, 4, 100L, 0, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerConfiguration(50.0f, 4, 100L, 2, 0));
  }

  private String blockingHalfOpenCall(CountDownLatch started, CountDownLatch release)
      throws Exception {
    return breaker.execute(
        () -> {
          started.countDown();
          release.await();
          return "ok";
        });
  }

  private static final class MutableClock extends Clock {

    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    private void advance(Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(instant, zone);
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
