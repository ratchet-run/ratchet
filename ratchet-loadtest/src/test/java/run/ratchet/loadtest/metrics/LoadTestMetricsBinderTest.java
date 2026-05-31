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
package run.ratchet.loadtest.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LoadTestMetricsBinderTest {

  @Test
  void ensureBoundFastPathDoesNotAcquireMonitor() throws Exception {
    LoadTestMetricsBinder binder = new LoadTestMetricsBinder();
    markBound(binder);
    CountDownLatch monitorHeld = new CountDownLatch(1);
    CountDownLatch releaseMonitor = new CountDownLatch(1);
    ExecutorService holderExecutor = Executors.newSingleThreadExecutor();
    ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
    try {
      holderExecutor.submit(
          () -> {
            synchronized (binder) {
              monitorHeld.countDown();
              assertDoesNotThrow(() -> releaseMonitor.await());
            }
          });
      monitorHeld.await();

      var call = callerExecutor.submit(binder::ensureBound);

      assertDoesNotThrow(() -> call.get(200, TimeUnit.MILLISECONDS));
    } finally {
      releaseMonitor.countDown();
      holderExecutor.shutdownNow();
      callerExecutor.shutdownNow();
    }
  }

  private static void markBound(LoadTestMetricsBinder binder) throws Exception {
    Field bound = LoadTestMetricsBinder.class.getDeclaredField("bound");
    bound.setAccessible(true);
    bound.setBoolean(binder, true);
  }
}
