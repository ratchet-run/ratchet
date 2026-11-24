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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;

class ReleaseExecutorIsolationTest {
  @Test
  void manySlowRetriesYieldToTimeoutFirstTryAndStateFlush() throws Exception {
    ScheduledThreadPoolExecutor releases = new ScheduledThreadPoolExecutor(2);
    ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
    CountDownLatch unblockPool = new CountDownLatch(1);
    CountDownLatch occupied = new CountDownLatch(2);
    CountDownLatch retriesEntered = new CountDownLatch(2);
    CountDownLatch timeoutWritten = new CountDownLatch(1);
    CountDownLatch flushed = new CountDownLatch(1);
    JobStore store = mock(JobStore.class);
    ResourcePermitService permits = mock(ResourcePermitService.class);
    doAnswer(
            invocation -> {
              retriesEntered.countDown();
              Thread.sleep(150);
              throw new IllegalStateException("store unavailable");
            })
        .when(store)
        .releaseBusinessKeyHold(any(), anyLong());
    doAnswer(
            invocation -> {
              retriesEntered.countDown();
              Thread.sleep(150);
              throw new IllegalStateException("store unavailable");
            })
        .when(permits)
        .release(anyString(), any(), anyLong());
    AtomicLong now = new AtomicLong();
    JobStateFlusher flusher = new JobStateFlusher(RatchetOptions.defaults(), now::get);
    try {
      for (int i = 0; i < 2; i++)
        releases.execute(
            () -> {
              occupied.countDown();
              awaitRelease(unblockPool);
            });
      assertTrue(occupied.await(5, TimeUnit.SECONDS));
      HeldKeyReleases holds = new HeldKeyReleases(2, store, () -> releases);
      PermitReleases permitReleases = new PermitReleases(2, permits, () -> releases, null);
      try {
        Thread.currentThread().interrupt();
        for (int i = 0; i < 40; i++) {
          UUID job = UUID.randomUUID();
          holds.release(job, 1);
          permitReleases.release("gpu", job, 1);
        }
      } finally {
        Thread.interrupted();
      }
      unblockPool.countDown();
      assertTrue(retriesEntered.await(5, TimeUnit.SECONDS));
      assertTrue(
          new OutcomeWriteRetries(2, () -> releases, null)
              .execute("timeout", timeoutWritten::countDown));
      FlushableState writer = mock(FlushableState.class);
      when(writer.flushIntervalMs()).thenReturn(1L);
      doAnswer(
              invocation -> {
                flushed.countDown();
                return null;
              })
          .when(writer)
          .flush();
      flusher.markDirty(writer);
      now.set(TimeUnit.MILLISECONDS.toNanos(1));
      flusher.start(timer, () -> releases);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
      assertTrue(
          timeoutWritten.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
      assertTrue(flushed.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
    } finally {
      unblockPool.countDown();
      flusher.stop();
      releases.shutdownNow();
      timer.shutdownNow();
      assertTrue(releases.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(timer.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void blockedReleasesLeaveHeartbeatAndWatchdogRunning() throws Exception {
    exercise(false, false);
  }

  @Test
  void sharedSchedulerControlStarvesHeartbeatAndWatchdog() throws Exception {
    exercise(true, false);
  }

  @Test
  void blockedTimeoutAndFlusherWritesLeaveHeartbeatAndWatchdogRunning() throws Exception {
    exercise(false, true);
  }

  @Test
  void sharedSchedulerControlStarvesOnTimeoutAndFlusherWrites() throws Exception {
    exercise(true, true);
  }

  private void exercise(boolean sharedScheduler, boolean timeoutWrites) throws Exception {
    StandaloneExecutorProvider provider = new StandaloneExecutorProvider();
    CountDownLatch unblock = new CountDownLatch(1);
    CountDownLatch claimEntered = new CountDownLatch(1);
    CountDownLatch holdEntered = new CountDownLatch(1);
    CountDownLatch heartbeats = new CountDownLatch(3);
    CountDownLatch watchdog = new CountDownLatch(2);
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
    AtomicLong now = new AtomicLong();
    JobStateFlusher flusher = new JobStateFlusher(RatchetOptions.defaults(), now::get);
    HeldKeyReleases holds = new HeldKeyReleases(2, store, releases);
    PermitReleases permitReleases = new PermitReleases(2, permits, releases, null);
    ClaimReschedules claims = new ClaimReschedules(2, store, releases, null);
    try {
      liveness.init();
      doAnswer(
              invocation -> {
                heartbeats.countDown();
                return null;
              })
          .when(nodes)
          .upsertHeartbeat(anyString(), any());
      if (timeoutWrites) {
        CountDownLatch flushEntered = new CountDownLatch(1);
        CountDownLatch timeoutEntered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(3);
        FlushableState writer = mock(FlushableState.class);
        when(writer.flushIntervalMs()).thenReturn(1L);
        doAnswer(
                call -> {
                  flushEntered.countDown();
                  awaitRelease(unblock);
                  return null;
                })
            .when(writer)
            .flush();
        flusher.markDirty(writer);
        now.set(TimeUnit.MILLISECONDS.toNanos(1));
        flusher.start(provider.getScheduledExecutor(), releases);
        assertTrue(flushEntered.await(10, TimeUnit.SECONDS));
        JobTimeoutHandler timeouts =
            new JobTimeoutHandler(
                store,
                store,
                null,
                mock(PostExecutionHandler.class),
                80,
                60,
                Clock.systemUTC(),
                null,
                null,
                null,
                500);
        for (int i = 0; i < 3; i++) {
          JobAttemptControl attempt =
              timeouts.newAttempt(UUID.randomUUID(), 1, Instant.now(), 0, 1L, "isolation");
          // A running body makes the watchdog interrupt the worker.
          assertTrue(attempt.markBodyStarted());
          attempt.attachStateFlush(
              () -> {
                timeoutEntered.countDown();
                awaitRelease(unblock);
              });
          Future<?> future = mock(Future.class);
          doAnswer(
                  call -> {
                    interrupted.countDown();
                    return true;
                  })
              .when(future)
              .cancel(true);
          timeouts.scheduleTimeoutMonitoring(
              attempt,
              future,
              provider.getScheduledExecutor(),
              Instant.now(),
              new OutcomeWriteRetries(2, releases, null));
        }
        assertTrue(timeoutEntered.await(10, TimeUnit.SECONDS));
        if (!sharedScheduler) assertTrue(interrupted.await(10, TimeUnit.SECONDS));
      } else {
        assertTrue(
            claims.handOff(
                UUID.randomUUID(),
                1,
                "release",
                Instant.now(),
                0,
                () -> false,
                () -> {},
                () -> {},
                () -> {}));
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
      }
      liveness.startHeartbeat(provider.getScheduledExecutor());
      // Two tasks that wait for each other finish only while both scheduler threads are free.
      for (int i = 0; i < 2; i++) {
        provider
            .getScheduledExecutor()
            .schedule(
                () -> {
                  watchdog.countDown();
                  return watchdog.await(10, TimeUnit.SECONDS);
                },
                0,
                TimeUnit.MILLISECONDS);
      }
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
      flusher.stop();
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
