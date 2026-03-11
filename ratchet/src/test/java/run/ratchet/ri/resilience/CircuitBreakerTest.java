package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

  private CircuitBreaker breaker;

  @BeforeEach
  void setUp() {
    // Low thresholds for easy testing: 50% failure rate, window of 4, min 2 calls, 100ms wait
    breaker =
        new CircuitBreaker(
            "test-service", new CircuitBreakerConfiguration(50.0f, 4, 100L, 5000L, 2, 2));
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
    // 1 failure out of 3 calls = 33% < 50% threshold, and min calls = 2
    // Sequence: success, success, failure → 1/3 = 33%
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
    // 2 failures out of 2 calls = 100% >= 50% threshold
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
    // Force open
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    assertThrows(ServiceUnavailableException.class, () -> breaker.execute(() -> "should not run"));
  }

  @Test
  void openTransitionsToHalfOpenAfterWaitDuration() throws Exception {
    breaker.transitionToOpen();
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    // Wait for the configured wait duration (100ms)
    Thread.sleep(150);

    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
  }

  @Test
  void halfOpenSuccessTransitionsToClosed() throws Exception {
    breaker.transitionToOpen();
    Thread.sleep(150);
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    // permittedCallsInHalfOpen = 2, all succeed → CLOSED
    breaker.execute(() -> "ok1");
    breaker.execute(() -> "ok2");

    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void halfOpenFailureTransitionsBackToOpen() throws Exception {
    breaker.transitionToOpen();
    Thread.sleep(150);
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    // First trial call fails → back to OPEN
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
}
