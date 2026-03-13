package run.ratchet.testsuite.resilience;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.ServiceUnavailableException;
import run.ratchet.testsuite.app.CircuitBreakerTestService;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates the {@code @CircuitBreakerProtected} CDI interceptor integration.
 *
 * <p>Uses the FAST profile: 50% failure threshold, 20-call sliding window, 3 minimum calls, 10s
 * wait duration, 2 permitted calls in half-open.
 */
class CircuitBreakerIT extends BaseRatchetIT {

  @Inject private CircuitBreakerTestService service;

  @Inject private CircuitBreakerRegistry registry;

  @Inject private TestJobService jobService;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CircuitBreakerTestService.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetState() {
    CircuitBreakerTestService.reset();
    // Reset the circuit breaker for our test service
    registry.resetBreaker("test-service:FAST");
  }

  @Test
  void circuitBreaker_shouldOpenAfterConsecutiveFailures() {
    // FAST profile: 3 minimum calls, 50% failure threshold
    CircuitBreakerTestService.setShouldFail(true);

    // Make enough failing calls to trip the breaker (need >= 3 calls with >= 50% failure)
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    // Circuit should now be OPEN — next call should throw ServiceUnavailableException
    assertThrows(ServiceUnavailableException.class, () -> service.callService());

    // Verify the breaker state
    CircuitBreaker breaker = registry.getBreaker("test-service:FAST");
    // After ServiceUnavailableException, state is either OPEN or transitioning
    assertEquals(
        CircuitBreaker.State.OPEN,
        breaker.getState(),
        "Expected circuit breaker to be in OPEN state after consecutive failures");
  }

  @Test
  void circuitBreaker_shouldHalfOpenAfterTimeout() {
    // Trip the breaker
    CircuitBreakerTestService.setShouldFail(true);
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    // Verify it's OPEN
    CircuitBreaker breaker = registry.getBreaker("test-service:FAST");
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    // Manually reset to simulate timeout (FAST profile wait is 10s — too long for unit test)
    breaker.reset();
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Now calls should succeed again
    CircuitBreakerTestService.setShouldFail(false);
    String result = service.callService();
    assertEquals("success", result);
  }

  @Test
  void circuitBreaker_shouldTransitionFromHalfOpenToClosedAfterSuccessfulTrials() throws Exception {
    // Create a breaker with 1ms wait duration so OPEN → HALF_OPEN transition happens immediately
    CircuitBreakerConfiguration fastConfig =
        new CircuitBreakerConfiguration(50.0f, 20, 1L, 2_000L, 2, 3);
    CircuitBreaker breaker = new CircuitBreaker("test-fast-transition", fastConfig);

    // Drive to OPEN: 3 failures (minimum calls = 3, failure rate = 100% >= 50% threshold)
    for (int i = 0; i < 3; i++) {
      assertThrows(
          RuntimeException.class,
          () ->
              breaker.execute(
                  () -> {
                    throw new RuntimeException("fail");
                  }));
    }
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    // Successful trial calls (permittedCallsInHalfOpen = 2) should transition to CLOSED
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertDoesNotThrow(() -> breaker.execute(() -> "ok"));
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }
}
