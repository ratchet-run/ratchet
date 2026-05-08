package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobLogStore;

@ExtendWith(MockitoExtension.class)
class LogPurgeTimerTest {

  @Mock private JobLogStore jobLogStore;
  @Mock private SingletonLeaseService singletonLeaseService;
  @Mock private ExecutorProvider executorProvider;
  @Mock private ScheduledExecutorService scheduledExecutor;

  private LogPurgeTimer timer;

  @BeforeEach
  void setUp() {
    timer = new LogPurgeTimer(jobLogStore, singletonLeaseService, executorProvider);
  }

  @Test
  void run_beforeInit_skipsPurgeAndReschedule() {
    assertDoesNotThrow(timer::run);

    verify(singletonLeaseService, never()).tryAcquire(any(), any());
    verify(jobLogStore, never()).purgeLogsOlderThan(any());
    verify(executorProvider, never()).getScheduledExecutor();
    verify(scheduledExecutor, never())
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }
}
