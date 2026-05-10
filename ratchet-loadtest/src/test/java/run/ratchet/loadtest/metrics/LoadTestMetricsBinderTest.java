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
