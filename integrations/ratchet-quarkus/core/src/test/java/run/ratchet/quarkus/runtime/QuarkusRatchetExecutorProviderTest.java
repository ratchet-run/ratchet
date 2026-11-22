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
package run.ratchet.quarkus.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;

class QuarkusRatchetExecutorProviderTest {
  @Test
  void readsOptionsLazilyWhenCreatingReleasePool() throws Exception {
    AtomicInteger reads = new AtomicInteger();
    RatchetOptions options =
        RatchetOptions.builder().execution(execution -> execution.releaseThreads(4)).build();
    QuarkusRatchetExecutorProvider provider =
        new QuarkusRatchetExecutorProvider(instance(options, reads));
    assertEquals(0, reads.get());
    try {
      var executor = provider.getReleaseExecutor();
      Field scheduler = executor.getClass().getDeclaredField("scheduler");
      scheduler.setAccessible(true);
      assertEquals(4, ((ScheduledThreadPoolExecutor) scheduler.get(executor)).getCorePoolSize());
      assertEquals(1, reads.get());
    } finally {
      provider.shutdown();
    }
  }

  @Test
  void unresolvedOptionsUseDefaultSize() throws Exception {
    QuarkusRatchetExecutorProvider provider =
        new QuarkusRatchetExecutorProvider(instance(null, new AtomicInteger()));
    try {
      var executor = provider.getReleaseExecutor();
      Field scheduler = executor.getClass().getDeclaredField("scheduler");
      scheduler.setAccessible(true);
      assertEquals(2, ((ScheduledThreadPoolExecutor) scheduler.get(executor)).getCorePoolSize());
    } finally {
      provider.shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  private Instance<RatchetOptions> instance(RatchetOptions options, AtomicInteger reads) {
    return (Instance<RatchetOptions>)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Instance.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "isResolvable" -> options != null;
                  case "get" -> {
                    reads.incrementAndGet();
                    yield options;
                  }
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
