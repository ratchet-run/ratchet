package run.ratchet.testsuite.resilience;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Vetoed;
import jakarta.enterprise.inject.spi.CDI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.cdi.CircuitBreakerInterceptor;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.ServiceUnavailableException;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.testsuite.app.CircuitBreakerTestService;

/**
 * Validates the {@code @CircuitBreakerProtected} CDI interceptor integration.
 *
 * <p>Uses the FAST profile: 50% failure threshold, 20-call sliding window, 3 minimum calls, 10s
 * wait duration, 2 permitted calls in half-open.
 */
@ExtendWith(ArquillianExtension.class)
@Vetoed
public class CircuitBreakerIT {

  private static final String TEST_SERVICE = "test-service";
  private static final String BEANS_XML =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
                 https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
             version="4.0"
             bean-discovery-mode="annotated"/>
      """;
  private static final CircuitBreakerConfiguration TEST_FAST_CONFIG =
      new CircuitBreakerConfiguration(50.0f, 20, 100L, 2, 3);

  private CircuitBreakerTestService service;

  private CircuitBreakerRegistry registry;

  @Deployment
  public static WebArchive createDeployment() {
    return ShrinkWrap.create(WebArchive.class, "circuit-breaker-it.war")
        .addPackage(CircuitBreakerProtected.class.getPackage())
        .addClasses(
            CircuitBreakerIT.class,
            CircuitBreakerTestService.class,
            CircuitBreakerTestService.TestCircuitBreakerConfigProvider.class,
            CircuitBreakerConfig.class,
            CircuitBreakerConfigProvider.class,
            CircuitBreakerInterceptor.class,
            CircuitBreaker.class,
            CircuitBreakerConfiguration.class,
            CircuitBreakerRegistry.class,
            ServiceUnavailableException.class)
        .addAsWebInfResource(new StringAsset(BEANS_XML), "beans.xml");
  }

  @BeforeEach
  void resetState() {
    service = CDI.current().select(CircuitBreakerTestService.class).get();
    registry = CDI.current().select(CircuitBreakerRegistry.class).get();
    CircuitBreakerTestService.reset();
    registry.registerConfig("fast", TEST_FAST_CONFIG);
    // Reset the circuit breaker for our test service (must match interceptor key)
    registry.resetBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
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
    assertEquals(
        3,
        CircuitBreakerTestService.getCallCount(),
        "OPEN circuit should reject before invoking the CDI target method");

    // Verify the breaker state
    CircuitBreaker breaker = registry.getBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
    // After ServiceUnavailableException, state is either OPEN or transitioning
    assertEquals(
        CircuitBreaker.State.OPEN,
        breaker.getState(),
        "Expected circuit breaker to be in OPEN state after consecutive failures");
  }

  @Test
  void circuitBreaker_shouldHalfOpenAfterTimeout() throws Exception {
    // Trip the breaker
    CircuitBreakerTestService.setShouldFail(true);
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    // Verify it's OPEN
    CircuitBreaker breaker = registry.getBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    CircuitBreakerTestService.setShouldFail(false);
    String result = service.callService();
    assertEquals("success", result);
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());
  }

  @Test
  void circuitBreaker_shouldTransitionFromHalfOpenToClosedAfterSuccessfulTrials() throws Exception {
    // Drive to OPEN: 3 failures (minimum calls = 3, failure rate = 100% >= 50% threshold)
    CircuitBreakerTestService.setShouldFail(true);
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    CircuitBreaker breaker = registry.getBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    // Successful trial calls (permittedCallsInHalfOpen = 2) should transition to CLOSED
    CircuitBreakerTestService.setShouldFail(false);
    assertEquals("success", service.callService());
    assertEquals("success", service.callService());
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  void circuitBreaker_shouldRejectCallsExceedingHalfOpenTrials() throws Exception {
    CircuitBreakerTestService.setShouldFail(true);
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    CircuitBreaker breaker = registry.getBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    CircuitBreakerTestService.setShouldFail(false);
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    CircuitBreakerTestService.blockCalls(started, release);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first = executor.submit(() -> service.callService());
      Future<String> second = executor.submit(() -> service.callService());
      assertTrue(started.await(1, TimeUnit.SECONDS), "Expected both trial calls to start");

      ServiceUnavailableException thrown =
          assertThrows(ServiceUnavailableException.class, () -> service.callService());
      assertTrue(thrown.getMessage().contains("HALF_OPEN"));

      release.countDown();
      assertEquals("success", first.get());
      assertEquals("success", second.get());
      assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    } finally {
      release.countDown();
      executor.shutdownNow();
      CircuitBreakerTestService.reset();
    }
  }

  @Test
  void circuitBreaker_shouldReopenOnHalfOpenFailure() {
    CircuitBreakerTestService.setShouldFail(true);
    for (int i = 0; i < 3; i++) {
      assertThrows(RuntimeException.class, () -> service.callService());
    }

    CircuitBreaker breaker = registry.getBreaker(TEST_SERVICE, CircuitBreakerProfile.FAST);
    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState()));

    assertThrows(RuntimeException.class, () -> service.callService());
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }
}
