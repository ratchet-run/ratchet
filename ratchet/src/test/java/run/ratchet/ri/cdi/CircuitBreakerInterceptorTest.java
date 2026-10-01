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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
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

  @Test
  void methodFilterOverridesClassAnnotationAndIgnoresFailures() throws Exception {
    assertAnnotationFilters("methodCall");
  }

  @Test
  void classFilterAppliesWhenMethodHasNoAnnotation() throws Exception {
    assertAnnotationFilters("classCall");
  }

  private void assertAnnotationFilters(String methodName) throws Exception {
    CircuitBreaker breaker =
        new CircuitBreaker("filtered", new CircuitBreakerConfiguration(50.0f, 20, 30_000L, 2, 3));
    when(context.getMethod()).thenReturn(FilteredService.class.getMethod(methodName));
    when(configProvider.isEnabled()).thenReturn(true);
    when(registry.getBreaker("filtered", CircuitBreakerProfile.DEFAULT)).thenReturn(breaker);
    IllegalArgumentException ignored = new IllegalArgumentException();
    doThrow(ignored).when(context).proceed();
    for (int i = 0; i < 10; i++) {
      assertSame(
          ignored,
          assertThrows(IllegalArgumentException.class, () -> interceptor.intercept(context)));
    }
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    doThrow(new UnsupportedOperationException()).when(context).proceed();
    for (int i = 0; i < 3; i++) {
      assertThrows(UnsupportedOperationException.class, () -> interceptor.intercept(context));
    }
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    doThrow(new IllegalStateException()).when(context).proceed();
    for (int i = 0; i < 3; i++) {
      assertThrows(IllegalStateException.class, () -> interceptor.intercept(context));
    }
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  /** Class-level exception filter fixture. */
  @CircuitBreakerProtected(
      service = "filtered",
      recordExceptions = IllegalStateException.class,
      ignoreExceptions = IllegalArgumentException.class)
  public static class FilteredService {
    /** Uses the class-level filter. */
    public void classCall() {}

    /** Uses a method-level filter; ignoring wins even for a recorded superclass. */
    @CircuitBreakerProtected(
        service = "filtered",
        recordExceptions = RuntimeException.class,
        ignoreExceptions = {IllegalArgumentException.class, UnsupportedOperationException.class})
    public void methodCall() {}
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
