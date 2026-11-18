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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;

class ReleaseExecutorIsolationTest {
  @Test
  void blockedReleasesLeaveHeartbeatAndWatchdogRunning() throws Exception {
    exercise(false);
  }

  @Test
  void sharedSchedulerControlStarvesHeartbeatAndWatchdog() throws Exception {
    exercise(true);
  }

  private void exercise(boolean sharedScheduler) throws Exception {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();
    CountDownLatch unblock = new CountDownLatch(1);
    CountDownLatch claimEntered = new CountDownLatch(1);
    CountDownLatch holdEntered = new CountDownLatch(1);
    CountDownLatch heartbeats = new CountDownLatch(3);
    CountDownLatch watchdog = new CountDownLatch(1);
    JobStore store = mock(JobStore.class);
    NodeStore nodes = mock(NodeStore.class);
    ResourcePermitService permits = mock(ResourcePermitService.class);
    when(store.resetOrphanJobsForNode(anyString(), anyInt(), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    when(store.resetOrphanJobsBefore(any(), anyInt(), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    when(nodes.getDatabaseTime()).thenAnswer(invocation -> Instant.now());
    NodeLiveness liveness =
        new NodeLiveness(
            () -> "isolation",
            nodes,
            store,
            mock(DynamicHeartbeatCalculator.class),
            1,
            30,
            3,
            false,
            Clock.systemUTC());
    doAnswer(
            invocation -> {
              claimEntered.countDown();
              awaitRelease(unblock);
              return true;
            })
        .when(store)
        .releaseClaim(any(), anyString(), any(), anyInt(), anyLong());
    doAnswer(
            invocation -> {
              holdEntered.countDown();
              awaitRelease(unblock);
              return true;
            })
        .when(store)
        .releaseBusinessKeyHold(any(), anyLong());
    doAnswer(
            invocation -> {
              awaitRelease(unblock);
              return null;
            })
        .when(permits)
        .release(anyString(), any(), anyLong());
    Supplier<ScheduledExecutorService> releases =
        sharedScheduler
            ? () -> provider.getScheduledExecutor()
            : () -> provider.getReleaseExecutor();
    HeldKeyReleases holds = new HeldKeyReleases(store, releases);
    PermitReleases permitReleases = new PermitReleases(permits, releases);
    ClaimReschedules claims = new ClaimReschedules(store, releases);
    try {
      liveness.init();
      doAnswer(
              invocation -> {
                heartbeats.countDown();
                return null;
              })
          .when(nodes)
          .upsertHeartbeat(anyString(), any());
      assertTrue(
          claims.handOff(UUID.randomUUID(), 1, "release", Instant.now(), 0, () -> false, () -> {}));
      assertTrue(claimEntered.await(10, TimeUnit.SECONDS));
      try {
        Thread.currentThread().interrupt();
        holds.release(UUID.randomUUID(), 2);
        permitReleases.release("gpu", UUID.randomUUID(), 3);
        permitReleases.release("gpu", UUID.randomUUID(), 4);
      } finally {
        Thread.interrupted();
      }
      assertTrue(holdEntered.await(10, TimeUnit.SECONDS));
      liveness.startHeartbeat(provider.getScheduledExecutor());
      provider.getScheduledExecutor().schedule(watchdog::countDown, 0, TimeUnit.MILLISECONDS);
      if (sharedScheduler) {
        assertFalse(heartbeats.await(4, TimeUnit.SECONDS));
        assertFalse(watchdog.await(300, TimeUnit.MILLISECONDS));
      } else {
        assertTrue(heartbeats.await(10, TimeUnit.SECONDS));
        assertTrue(watchdog.await(10, TimeUnit.SECONDS));
      }
      assertTrue(unblock.getCount() == 1, "store calls must still be blocked");
    } finally {
      Thread.interrupted();
      unblock.countDown();
      liveness.shutdown();
      provider.shutdown();
    }
  }

  private static void awaitRelease(CountDownLatch unblock) {
    try {
      unblock.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
