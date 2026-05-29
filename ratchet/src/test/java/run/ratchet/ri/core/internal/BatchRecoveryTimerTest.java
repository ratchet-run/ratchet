package run.ratchet.ri.core.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.BatchService;

class BatchRecoveryTimerTest {

  @Test
  void startRunsFirstRecoverySoonAndThenEveryFifteenMinutes() {
    BatchService batchService = mock(BatchService.class);
    SingletonLeaseService singletonLeaseService = mock(SingletonLeaseService.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> handle = mock(ScheduledFuture.class);
    doReturn(handle)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(15L), eq(TimeUnit.MINUTES));

    new BatchRecoveryTimer(batchService, singletonLeaseService).start(executor);

    verify(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(15L), eq(TimeUnit.MINUTES));
  }
}
