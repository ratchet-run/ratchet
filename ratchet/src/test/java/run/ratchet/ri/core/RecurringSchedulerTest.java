package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobCrudStore;

class RecurringSchedulerTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void calculateNextDelay_usesInjectedClock() throws Exception {
    JobCrudStore jobCrudStore = mock(JobCrudStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.of(NOW.plusSeconds(10)));

    RecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(9500L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_noRecurringJobsUsesMaxPoll() throws Exception {
    JobCrudStore jobCrudStore = mock(JobCrudStore.class);
    when(jobCrudStore.findEarliestRecurringNextFire()).thenReturn(Optional.empty());

    RecurringScheduler scheduler = scheduler(jobCrudStore);

    assertEquals(60000L, calculateNextDelay(scheduler, 0));
  }

  @Test
  void calculateNextDelay_processedJobsUsesMinPoll() throws Exception {
    RecurringScheduler scheduler = scheduler(mock(JobCrudStore.class));

    assertEquals(1000L, calculateNextDelay(scheduler, 1));
  }

  private static RecurringScheduler scheduler(JobCrudStore jobCrudStore) {
    RecurringScheduler scheduler =
        new RecurringScheduler(
            mock(ExecutorProvider.class),
            jobCrudStore,
            mock(SingletonLeaseService.class),
            mock(NodeIdentityProvider.class),
            mock(RecurringJobExecutor.class),
            mock(PollerScheduler.class),
            Clock.fixed(NOW, ZoneOffset.UTC));
    scheduler.configure(1000L, 60000L, 20);
    return scheduler;
  }

  private static long calculateNextDelay(RecurringScheduler scheduler, int processedCount)
      throws Exception {
    Method method = RecurringScheduler.class.getDeclaredMethod("calculateNextDelay", int.class);
    method.setAccessible(true);
    return (long) method.invoke(scheduler, processedCount);
  }
}
