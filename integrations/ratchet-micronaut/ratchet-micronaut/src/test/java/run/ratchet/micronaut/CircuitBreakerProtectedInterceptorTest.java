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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanProvider;
import io.micronaut.inject.ExecutableMethod;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.micronaut.internal.CircuitBreakerProtectedInterceptor;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.ri.resilience.CircuitBreakerConfiguration;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.CircuitBreakerConfigProvider;

class CircuitBreakerProtectedInterceptorTest {

  private final CircuitBreakerRegistry registry = mock(CircuitBreakerRegistry.class);
  private final CircuitBreakerConfigProvider configProvider =
      mock(CircuitBreakerConfigProvider.class);
  private final MethodInvocationContext<Object, Object> context =
      mock(MethodInvocationContext.class);
  private final ExecutableMethod<Object, Object> executable = mock(ExecutableMethod.class);
  private final BeanProvider<CircuitBreakerRegistry> registries = mock(BeanProvider.class);
  private final BeanProvider<CircuitBreakerConfigProvider> configurations =
      mock(BeanProvider.class);

  private CircuitBreakerProtectedInterceptor interceptor;

  @BeforeEach
  void setUp() {
    interceptor = new CircuitBreakerProtectedInterceptor(registries, configurations);
    when(context.getExecutableMethod()).thenReturn(executable);
    when(registries.isPresent()).thenReturn(true);
    when(registries.get()).thenReturn(registry);
    when(configurations.isPresent()).thenReturn(true);
    when(configurations.get()).thenReturn(configProvider);
  }

  @Test
  void intercept_annotationOnImplementationMethod_resolvesTargetMethod() throws Exception {
    Method interfaceMethod = CircuitProtectedService.class.getMethod("call");
    CircuitProtectedService target = new CircuitProtectedServiceImpl();
    CircuitBreaker breaker =
        new CircuitBreaker(
            "impl-service", CircuitBreakerConfiguration.forProfile(CircuitBreakerProfile.FAST));

    when(executable.getTargetMethod()).thenReturn(interfaceMethod);
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

  @Test
  void recordedErrorsPassThroughUnchangedAndOpenTheBreaker() throws Exception {
    CircuitBreaker breaker =
        new CircuitBreaker("filtered", new CircuitBreakerConfiguration(50.0f, 20, 30_000L, 2, 3));
    when(executable.getTargetMethod()).thenReturn(FilteredService.class.getMethod("errorCall"));
    when(configProvider.isEnabled()).thenReturn(true);
    when(registry.getBreaker("filtered", CircuitBreakerProfile.DEFAULT)).thenReturn(breaker);
    AssertionError error = new AssertionError("recorded");
    doThrow(error).when(context).proceed();
    for (int i = 0; i < 3; i++) {
      assertSame(error, assertThrows(AssertionError.class, () -> interceptor.intercept(context)));
    }
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  void disabledPassesThroughWithoutLookingUpBreaker() throws Exception {
    when(executable.getTargetMethod()).thenReturn(FilteredService.class.getMethod("classCall"));
    when(context.proceed()).thenReturn("ok");
    assertEquals("ok", interceptor.intercept(context));
    verifyNoInteractions(registry);
  }

  @Test
  void absentPolicyBeansPassThrough() throws Exception {
    when(executable.getTargetMethod()).thenReturn(FilteredService.class.getMethod("classCall"));
    when(context.proceed()).thenReturn("ok");
    when(configurations.isPresent()).thenReturn(false);
    assertEquals("ok", interceptor.intercept(context));
    when(configurations.isPresent()).thenReturn(true);
    when(configProvider.isEnabled()).thenReturn(true);
    when(registries.isPresent()).thenReturn(false);
    assertEquals("ok", interceptor.intercept(context));
    verifyNoInteractions(registry);
  }

  @Test
  void checkedFailureKeepsIdentity() throws Exception {
    when(executable.getTargetMethod()).thenReturn(FilteredService.class.getMethod("classCall"));
    when(configProvider.isEnabled()).thenReturn(true);
    when(registry.getBreaker("filtered", CircuitBreakerProfile.DEFAULT))
        .thenReturn(
            new CircuitBreaker("filtered", new CircuitBreakerConfiguration(50, 20, 30000, 2, 3)));
    Exception failure = new Exception("checked");
    when(context.proceed())
        .thenAnswer(
            invocation -> {
              throw failure;
            });
    assertSame(failure, assertThrows(Exception.class, () -> interceptor.intercept(context)));
  }

  private void assertAnnotationFilters(String methodName) throws Exception {
    CircuitBreaker breaker =
        new CircuitBreaker("filtered", new CircuitBreakerConfiguration(50.0f, 20, 30_000L, 2, 3));
    when(executable.getTargetMethod()).thenReturn(FilteredService.class.getMethod(methodName));
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

    /** Records errors through a method-level filter. */
    @CircuitBreakerProtected(service = "filtered", recordExceptions = AssertionError.class)
    public void errorCall() {}
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
