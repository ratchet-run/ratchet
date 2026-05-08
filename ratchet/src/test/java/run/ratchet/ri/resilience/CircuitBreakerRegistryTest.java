package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;

class CircuitBreakerRegistryTest {

  @Test
  void serviceNamesContainingProfileSeparatorsDoNotUseEncodedStringKeys() throws Exception {
    CircuitBreakerRegistry registry = new CircuitBreakerRegistry();

    CircuitBreaker breaker = registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    assertSame(breaker, registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST));
    assertFalse(registryKeyMap(registry).containsKey("payments:DEFAULT:FAST"));
  }

  @Test
  void serviceNamesContainingSeparatorsRemainIndependentlyManageable() {
    CircuitBreakerRegistry registry = new CircuitBreakerRegistry();

    CircuitBreaker plain = registry.getBreaker("payments", CircuitBreakerProfile.DEFAULT);
    CircuitBreaker colon = registry.getBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    registry.openBreaker("payments:DEFAULT", CircuitBreakerProfile.FAST);

    assertEquals(CircuitBreaker.State.CLOSED, plain.getState());
    assertEquals(CircuitBreaker.State.OPEN, colon.getState());
    assertEquals(
        CircuitBreaker.State.OPEN,
        registry.getBreakerState("payments:DEFAULT", CircuitBreakerProfile.FAST));
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, CircuitBreaker> registryKeyMap(CircuitBreakerRegistry registry)
      throws NoSuchFieldException, IllegalAccessException {
    Field breakers = CircuitBreakerRegistry.class.getDeclaredField("breakers");
    breakers.setAccessible(true);
    return (Map<Object, CircuitBreaker>) breakers.get(registry);
  }
}
