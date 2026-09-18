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

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.BeanResolver;
import run.ratchet.store.entity.JobPayload;

class ManagedInvocationTest {
  public interface Job {
    String execute();
  }

  public static class ConcreteJob implements Job {
    public String execute() {
      throw new AssertionError("Unwrapped target invoked");
    }

    public void hiddenFromProxy() {}
  }

  public interface OtherJob {
    String execute();
  }

  @Test
  void cachesExposedMethodsSeparatelyForEachProxyClass() throws Exception {
    var declared = ConcreteJob.class.getMethod("execute");
    Object first =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Runnable.class, Job.class},
            (proxy, method, args) -> "first");
    Object second =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {OtherJob.class},
            (proxy, method, args) -> "second");
    var exposed = ManagedInvocation.exposedMethod(declared, first);
    assertEquals(Job.class, exposed.getDeclaringClass());
    assertSame(exposed, ManagedInvocation.exposedMethod(declared, first));
    assertEquals("first", exposed.invoke(first));
    assertEquals("second", ManagedInvocation.exposedMethod(declared, second).invoke(second));
    assertSame(declared, ManagedInvocation.exposedMethod(declared, new ConcreteJob()));
    var hidden = ConcreteJob.class.getMethod("hiddenFromProxy");
    for (int attempt = 0; attempt < 2; attempt++) {
      var failure =
          assertThrows(
              ManagedInvocation.UnexposedMethodException.class,
              () -> ManagedInvocation.exposedMethod(hidden, first));
      var policy = new DoNotRetryPolicy();
      assertTrue(policy.shouldNotRetry(failure));
      assertTrue(policy.shouldNotRetry(new RuntimeException(failure)));
      assertFalse(policy.shouldNotRetry(new NoSuchMethodException("signature drift")));
    }
  }

  @Test
  void invokesExposedInterfaceAndReleasesHandleOnSuccessAndFailure() throws Exception {
    AtomicInteger intercepted = new AtomicInteger();
    AtomicInteger released = new AtomicInteger();
    Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Job.class},
            (p, m, a) -> {
              if (intercepted.incrementAndGet() == 2) throw new IllegalArgumentException("boom");
              return "managed";
            });
    BeanResolver resolver =
        new BeanResolver() {
          public <T> T resolve(Class<T> type) {
            throw new AssertionError("Unmanaged resolve");
          }

          public ManagedBean acquire(Class<?> type) {
            return new ManagedBean() {
              public Object instance() {
                return proxy;
              }

              public void close() {
                released.incrementAndGet();
              }
            };
          }
        };
    JobPayloadInvoker invoker = new JobPayloadInvoker(resolver, n -> true);
    JobPayload payload =
        new JobPayload(
            ConcreteJob.class.getName(), "execute", "()Ljava/lang/String;", false, List.of());
    assertEquals("managed", invoker.invoke(payload));
    assertThrows(IllegalArgumentException.class, () -> invoker.invoke(payload));
    assertEquals(2, intercepted.get());
    assertEquals(2, released.get());
    assertThrows(
        NoSuchMethodException.class,
        () ->
            invoker.invoke(
                new JobPayload(
                    ConcreteJob.class.getName(), "hiddenFromProxy", "()V", false, List.of())));
    assertEquals(3, released.get());
  }
}
