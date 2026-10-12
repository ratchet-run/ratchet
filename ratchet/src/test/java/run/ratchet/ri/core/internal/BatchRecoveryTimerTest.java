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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.store.spi.LockStore;

class BatchRecoveryTimerTest {

  @Test
  void configuredIntervalCapsInitialDelayAndUsesConfiguredLeaseTtl() {
    BatchService batches = mock(BatchService.class);
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("batchRecovery", Duration.ofSeconds(1)))
        .thenReturn(Optional.of(new SingletonLease(locks, "batchRecovery", "node-1")));
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    BatchRecoveryTimer timer = new BatchRecoveryTimer(batches, leases, 3, 1);
    timer.start(executor);
    timer.recoverBatches();
    verify(executor).scheduleAtFixedRate(any(Runnable.class), eq(3L), eq(3L), eq(TimeUnit.SECONDS));
    verify(leases).tryAcquire("batchRecovery", Duration.ofSeconds(1));
    verify(batches).recoverStuckBatches();
    verify(locks).unlock("batchRecovery", "node-1");
  }

  @Test
  void startRunsFirstRecoveryAfterSixtySecondsThenAtConfiguredInterval() {
    BatchService batchService = mock(BatchService.class);
    SingletonLeaseService singletonLeaseService = mock(SingletonLeaseService.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> handle = mock(ScheduledFuture.class);
    doReturn(handle)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(60L), eq(900L), eq(TimeUnit.SECONDS));

    new BatchRecoveryTimer(batchService, singletonLeaseService, 900, 900).start(executor);

    verify(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(60L), eq(900L), eq(TimeUnit.SECONDS));
  }
}
