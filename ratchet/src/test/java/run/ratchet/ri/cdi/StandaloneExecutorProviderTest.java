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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;

class StandaloneExecutorProviderTest {

  @Test
  void shutdownStopsOwnedExecutors() {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();

    assertFalse(provider.getJobExecutor().isShutdown());
    assertFalse(provider.getScheduledExecutor().isShutdown());

    provider.shutdown();

    assertTrue(provider.getJobExecutor().isShutdown());
    assertTrue(provider.getScheduledExecutor().isShutdown());
  }

  @Test
  void shutdownLetsSubmittedJobExecutorWorkFinish() throws Exception {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<?> work =
        provider
            .getJobExecutor()
            .submit(
                () -> {
                  started.countDown();
                  try {
                    assertTrue(release.await(1, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                });

    assertTrue(started.await(1, TimeUnit.SECONDS));
    release.countDown();

    provider.shutdown();

    assertTrue(work.isDone());
    assertFalse(work.isCancelled());
  }

  @Test
  void getJobExecutorByTargetResolvesVirtualExecutor() throws Exception {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();
    ExecutorService virtualExecutor = null;

    try {
      virtualExecutor = provider.getJobExecutor(ExecutorTargets.VIRTUAL).orElseThrow();
      Future<Boolean> isVirtual = virtualExecutor.submit(StandaloneExecutorProviderTest::isVirtual);

      assertNotSame(provider.getJobExecutor(), virtualExecutor);
      assertTrue(isVirtual.get(1, TimeUnit.SECONDS));
    } finally {
      provider.shutdown();
    }

    assertTrue(virtualExecutor.isShutdown());
  }

  @Test
  void shutdownNowInterruptsWorkThatDoesNotFinishWithinGracePeriod() throws Exception {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();
    CountDownLatch started = new CountDownLatch(1);
    Future<?> work =
        provider
            .getJobExecutor()
            .submit(
                () -> {
                  started.countDown();
                  try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                });

    assertTrue(started.await(1, TimeUnit.SECONDS));

    provider.shutdown();

    ExecutionException thrown =
        assertThrows(ExecutionException.class, () -> work.get(1, TimeUnit.SECONDS));
    assertInstanceOf(AssertionError.class, thrown.getCause());
  }

  private static boolean isVirtual() {
    try {
      Method isVirtual = Thread.class.getMethod("isVirtual");
      return (Boolean) isVirtual.invoke(Thread.currentThread());
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Java 21+ is required for virtual thread assertions", e);
    }
  }
}
