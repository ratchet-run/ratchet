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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.ri.core.internal.JobPayloadInvoker.ResilienceTarget;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.CircuitBreakerExceptionFilter;
import run.ratchet.store.entity.JobPayload;

class JobPayloadInvokerTest {

  @Test
  void sameBinaryNameLoadedByTwoClassLoadersUsesTheCurrentContextLoader() throws Exception {
    byte[] targetBytes = classBytes(LoaderTarget.class);
    ClassLoader parent = JobPayloadInvokerTest.class.getClassLoader();
    ClassLoader first =
        new IsolatedTargetClassLoader(parent, LoaderTarget.class.getName(), targetBytes);
    ClassLoader second =
        new IsolatedTargetClassLoader(parent, LoaderTarget.class.getName(), targetBytes);
    JobPayloadInvoker invoker =
        new JobPayloadInvoker(
            unusedBeanResolver(), name -> name.equals(LoaderTarget.class.getName()));
    JobPayload payload =
        payload(
            LoaderTarget.class,
            "definingLoader",
            Type.getMethodDescriptor(Type.getType(ClassLoader.class)),
            true,
            List.of());

    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(first);
      assertSame(first, invoker.invoke(payload));

      Thread.currentThread().setContextClassLoader(second);
      assertSame(second, invoker.invoke(payload));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void cachedTargetIsRejectedAfterPolicyChanges() throws Exception {
    AtomicBoolean allowed = new AtomicBoolean(true);
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> allowed.get());
    JobPayload payload =
        payload(InvocationTarget.class, "staticValue", "()Ljava/lang/String;", true, List.of());

    assertEquals("static", invoker.invoke(payload));
    allowed.set(false);

    SecurityException failure =
        assertThrows(SecurityException.class, () -> invoker.invoke(payload));
    assertEquals(
        "Class " + InvocationTarget.class.getName() + " is not allowed for job execution.",
        failure.getMessage());
  }

  @Test
  void instanceInvocationResolvesTheBeanAndCoercesArguments() throws Exception {
    BeanResolver beanResolver = mock(BeanResolver.class);
    InvocationTarget target = new InvocationTarget();
    when(beanResolver.acquire(InvocationTarget.class)).thenCallRealMethod();
    when(beanResolver.resolve(InvocationTarget.class)).thenReturn(target);
    JobPayloadInvoker invoker = new JobPayloadInvoker(beanResolver, name -> true);
    JobPayload payload =
        payload(
            InvocationTarget.class, "instanceValue", "(J)Ljava/lang/String;", false, List.of(7));

    assertEquals("instance-7", invoker.invoke(payload));
    verify(beanResolver).resolve(InvocationTarget.class);
  }

  @Test
  void privateMethodProducesTheExistingVisibilityDiagnostic() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);
    JobPayload payload = payload(InvocationTarget.class, "hidden", "()V", true, List.of());

    NoSuchMethodException failure =
        assertThrows(NoSuchMethodException.class, () -> invoker.invoke(payload));

    assertEquals(
        "hidden in "
            + InvocationTarget.class.getName()
            + " is private — only public methods can be scheduled as jobs. Change the method"
            + " visibility to public.",
        failure.getMessage());
  }

  @Test
  void targetExceptionIsUnwrapped() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);
    JobPayload payload = payload(InvocationTarget.class, "fail", "()V", true, List.of());

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> invoker.invoke(payload));

    assertEquals("target failure", failure.getMessage());
  }

  @Test
  void serviceNameUsesAnnotationAndFallsBackToClassAndMethod() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);

    assertEquals(
        "payments",
        invoker
            .resilienceTarget(
                payload(InvocationTarget.class, "protectedValue", "()V", true, List.of()))
            .serviceName());
    assertEquals(
        "InvocationTarget.staticValue",
        invoker
            .resilienceTarget(
                payload(
                    InvocationTarget.class, "staticValue", "()Ljava/lang/String;", true, List.of()))
            .serviceName());
  }

  @Test
  void resilienceTargetUsesMethodFilterBeforeClassFilterAndCachesIt() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);
    JobPayload payload = payload(FilteredTarget.class, "methodCall", "()V", true, List.of());
    ResilienceTarget target = invoker.resilienceTarget(payload);
    assertEquals("method-filter", target.serviceName());
    assertEquals(List.of(IllegalStateException.class), target.exceptionFilter().recordExceptions());
    assertEquals(
        List.of(IllegalArgumentException.class), target.exceptionFilter().ignoreExceptions());
    assertSame(target, invoker.resilienceTarget(payload));
  }

  @Test
  void resilienceTargetFallsBackToClassFilter() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);
    ResilienceTarget target =
        invoker.resilienceTarget(
            payload(FilteredTarget.class, "classCall", "()V", true, List.of()));
    assertEquals("class-filter", target.serviceName());
    assertEquals(List.of(RuntimeException.class), target.exceptionFilter().recordExceptions());
    assertEquals(
        List.of(UnsupportedOperationException.class), target.exceptionFilter().ignoreExceptions());
  }

  @Test
  void resilienceTargetWithoutAnnotationOrOnResolutionFailureRecordsAll() {
    JobPayloadInvoker invoker = new JobPayloadInvoker(unusedBeanResolver(), name -> true);
    assertEquals(
        CircuitBreakerExceptionFilter.RECORD_ALL,
        invoker
            .resilienceTarget(
                payload(
                    InvocationTarget.class, "staticValue", "()Ljava/lang/String;", true, List.of()))
            .exceptionFilter());
    ResilienceTarget fallback =
        invoker.resilienceTarget(payload(FilteredTarget.class, "missing", "()V", true, List.of()));
    assertEquals("JobPayloadInvokerTest$FilteredTarget.missing", fallback.serviceName());
    assertSame(CircuitBreakerExceptionFilter.RECORD_ALL, fallback.exceptionFilter());
  }

  /** Fixture for method and class annotation resolution. */
  @CircuitBreakerProtected(
      service = "class-filter",
      recordExceptions = RuntimeException.class,
      ignoreExceptions = UnsupportedOperationException.class)
  public static class FilteredTarget {
    /** Uses class metadata. */
    public static void classCall() {}

    /** Overrides class metadata. */
    @CircuitBreakerProtected(
        service = "method-filter",
        recordExceptions = IllegalStateException.class,
        ignoreExceptions = IllegalArgumentException.class)
    public static void methodCall() {}
  }

  private static JobPayload payload(
      Class<?> target, String method, String descriptor, boolean isStatic, List<Object> args) {
    return new JobPayload(target.getName(), method, descriptor, isStatic, args, null);
  }

  private static BeanResolver unusedBeanResolver() {
    return mock(BeanResolver.class);
  }

  private static byte[] classBytes(Class<?> type) throws IOException {
    String resource = "/" + type.getName().replace('.', '/') + ".class";
    try (InputStream input = type.getResourceAsStream(resource)) {
      if (input == null) {
        throw new IOException("Class bytes not found: " + resource);
      }
      return input.readAllBytes();
    }
  }

  public static final class LoaderTarget {
    public static ClassLoader definingLoader() {
      return LoaderTarget.class.getClassLoader();
    }
  }

  public static final class InvocationTarget {
    public static String staticValue() {
      return "static";
    }

    public String instanceValue(long value) {
      return "instance-" + value;
    }

    @SuppressWarnings("unused")
    private static void hidden() {}

    public static void fail() {
      throw new IllegalStateException("target failure");
    }

    @CircuitBreakerProtected(service = "payments")
    public static void protectedValue() {}
  }

  private static final class IsolatedTargetClassLoader extends ClassLoader {
    private final String targetName;
    private final byte[] targetBytes;

    private IsolatedTargetClassLoader(ClassLoader parent, String targetName, byte[] targetBytes) {
      super(parent);
      this.targetName = targetName;
      this.targetBytes = targetBytes;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!targetName.equals(name)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = defineClass(name, targetBytes, 0, targetBytes.length);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }
}
