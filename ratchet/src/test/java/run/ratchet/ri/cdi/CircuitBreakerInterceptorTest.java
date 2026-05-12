package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerInterceptorTest {

  @Mock private CircuitBreakerRegistry registry;
  @Mock private CircuitBreakerConfigProvider configProvider;
  @Mock private InvocationContext context;

  private CircuitBreakerInterceptor interceptor;

  @BeforeEach
  void setUp() {
    interceptor = new CircuitBreakerInterceptor(registry, configProvider);
  }

  @Test
  void intercept_annotationOnImplementationMethod_resolvesTargetMethod() throws Exception {
    Method interfaceMethod = CircuitProtectedService.class.getMethod("call");
    CircuitProtectedService target = new CircuitProtectedServiceImpl();
    CircuitBreaker breaker =
        new CircuitBreaker(
            "impl-service", CircuitBreakerConfiguration.forProfile(CircuitBreakerProfile.FAST));

    when(context.getMethod()).thenReturn(interfaceMethod);
    when(context.getTarget()).thenReturn(target);
    when(context.proceed()).thenReturn("ok");
    when(configProvider.isEnabled()).thenReturn(true);
    when(registry.getBreaker("impl-service", CircuitBreakerProfile.FAST)).thenReturn(breaker);

    Object result = interceptor.intercept(context);

    assertEquals("ok", result);
    verify(registry).getBreaker("impl-service", CircuitBreakerProfile.FAST);
  }

  interface CircuitProtectedService {
    String call() throws Exception;
  }

  static final class CircuitProtectedServiceImpl implements CircuitProtectedService {
    @Override
    @CircuitBreakerProtected(service = "impl-service", profile = CircuitBreakerProfile.FAST)
    public String call() {
      return "ok";
    }
  }
}
