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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;

@ExtendWith(MockitoExtension.class)
class OrphanRecoveryTimerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobBulkStore jobBulkStore;
  @Mock private NodeStore nodeStore;
  @Mock private ResourcePermitService resourcePermitService;

  private OrphanRecoveryTimer timer;

  @BeforeEach
  void setUp() {
    timer =
        new OrphanRecoveryTimer(
            jobBulkStore, nodeStore, resourcePermitService, null, 60, 120, FIXED_CLOCK);
  }

  @Test
  void configuredLeaseTtlHasNoFloorAndIsIndependentOfScanInterval() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(1)))
        .thenReturn(Optional.of(new SingletonLease(locks, "orphanRecovery", "node-1")));
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore, nodeStore, resourcePermitService, leases, 60, 1, FIXED_CLOCK);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    configured.start(executor, 300);
    configured.recoverNow();
    verify(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(300L), eq(300L), eq(TimeUnit.SECONDS));
    verify(leases).tryAcquire("orphanRecovery", Duration.ofSeconds(1));
    verify(locks).unlock("orphanRecovery", "node-1");
  }

  @Test
  void constructor_rejectsNullJobBulkStore() {
    assertThrows(
        NullPointerException.class,
        () -> new OrphanRecoveryTimer(null, nodeStore, resourcePermitService));
  }

  @Test
  void constructor_rejectsNullNodeStore() {
    assertThrows(
        NullPointerException.class,
        () -> new OrphanRecoveryTimer(jobBulkStore, null, resourcePermitService));
  }

  @Test
  void constructor_rejectsNullResourcePermitService() {
    assertThrows(
        NullPointerException.class, () -> new OrphanRecoveryTimer(jobBulkStore, nodeStore, null));
  }

  @Test
  void recoverOrphans_withStaleNodesCleansPermitsBeforeDeletingNodes() {
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("node-1");
    when(nodeStore.findInactiveNodesSince(any(Instant.class))).thenReturn(List.of(staleNode));

    timer.recoverNow();

    Instant cutoff = FIXED_NOW.minusSeconds(60);
    verify(jobBulkStore).resetOrphanJobsBefore(cutoff);
    verify(nodeStore).findInactiveNodesSince(cutoff);
    InOrder order = inOrder(resourcePermitService, nodeStore);
    order.verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
    order.verify(nodeStore).deleteInactiveNodesByIds(List.of("node-1"));
  }

  @Test
  void recoveryOnlyResetsOrphansToPending() {
    timer.recoverNow();

    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60));
    verifyNoMoreInteractions(jobBulkStore);
  }

  @Test
  void start_cancelsExistingHandleBeforeReplacingIt() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> first = mock(ScheduledFuture.class);
    ScheduledFuture<?> second = mock(ScheduledFuture.class);
    doReturn(first, second)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));

    timer.start(executor, 1);
    timer.start(executor, 1);

    verify(first).cancel(false);
    verify(second, never()).cancel(false);
  }

  @Test
  void stop_clearsHandleBeforeCancelAndIsIdempotent() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> handle = mock(ScheduledFuture.class);
    doReturn(handle)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));

    timer.start(executor, 1);
    timer.stop();
    timer.stop();

    verify(handle, times(1)).cancel(false);
  }
}
