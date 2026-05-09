package run.ratchet.ri.resilience;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

  private CircuitBreaker breaker;

  @BeforeEach
  void setUp() {
    breaker =
        new CircuitBreaker("test-service", new CircuitBreakerConfiguration(50.0f, 4, 100L, 2, 2));
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

    assertThrows(ServiceUnavailableException.class, () -> breaker.execute(() -> "should not run"));
  }

  @Test
  void openTransitionsToHalfOpenAfterWaitDuration() {
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));
  }

  @Test
  void halfOpenSuccessTransitionsToClosed() throws Exception {
    breaker.transitionToOpen();
    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    breaker.execute(() -> "ok1");
    breaker.execute(() -> "ok2");

    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void halfOpenExhaustionRejectsExtraConcurrentTrialCalls() throws Exception {
    breaker.transitionToOpen();
    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first = executor.submit(() -> blockingHalfOpenCall(started, release));
      Future<String> second = executor.submit(() -> blockingHalfOpenCall(started, release));
      assertTrue(started.await(1, java.util.concurrent.TimeUnit.SECONDS));

      ServiceUnavailableException thrown =
          assertThrows(ServiceUnavailableException.class, () -> breaker.execute(() -> "extra"));
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
    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

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
    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

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
}
