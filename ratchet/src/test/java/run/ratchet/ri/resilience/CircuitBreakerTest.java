package run.ratchet.ri.resilience;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
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
}
