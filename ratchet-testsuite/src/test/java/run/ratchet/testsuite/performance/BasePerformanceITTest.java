package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.PollerScheduler;

class BasePerformanceITTest {

  @Test
  void pollerSchedulerStoppedReturnsFalseDuringPollCycle() throws Exception {
    PollerScheduler scheduler = new PollerScheduler(null, null);
    setField(scheduler, "cycleRunning", true);

    assertFalse(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  @Test
  void pollerSchedulerStoppedReturnsFalseForScheduledHandle() throws Exception {
    PollerScheduler scheduler = new PollerScheduler(null, null);
    setField(scheduler, "handle", new CompletableFuture<>());

    assertFalse(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  @Test
  void pollerSchedulerStoppedReturnsTrueWhenIdle() {
    PollerScheduler scheduler = new PollerScheduler(null, null);

    assertTrue(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
