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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.api.AttemptToken;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.JobClaimStore;

class JobAttemptControlTest {
  @Test
  void revocationSetsBothFlagsWhileFenceBounceOnlySupersedes() {
    JobAttemptControl attempt = newAttempt();
    assertFalse(attempt.isRevoked());
    attempt.markSuperseded();
    assertTrue(attempt.isSuperseded());
    assertFalse(attempt.isRevoked());
    attempt.revoke();
    assertTrue(attempt.isRevoked());
    assertTrue(attempt.isSuperseded());

    JobAttemptControl fresh = newAttempt();
    fresh.revoke();
    assertTrue(fresh.isRevoked());
    assertTrue(fresh.isSuperseded());
  }

  @Test
  void checkpointFlushWithoutHookIsHarmless() {
    newAttempt().flushStateQuietly();
  }

  @Test
  void checkpointFlushRunsAttachedHook() {
    JobAttemptControl attempt = newAttempt();
    Runnable hook = mock(Runnable.class);
    attempt.attachStateFlush(hook);
    attempt.flushStateQuietly();
    verify(hook).run();
  }

  @Test
  void checkpointFlushCatchesThrowable() {
    JobAttemptControl attempt = newAttempt();
    attempt.attachStateFlush(
        () -> {
          throw new AssertionError("flush failed");
        });
    attempt.flushStateQuietly();
  }

  private JobAttemptControl newAttempt() {
    return new JobAttemptControl(
        new UUID(0L, 42L), Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 0L, "node-1");
  }

  private JobAttemptControl claimedAttempt() {
    return new JobAttemptControl(
        new UUID(0L, 42L), Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7L, "node-1");
  }

  @Test
  void tokensRequirePositiveClaimSequence() {
    assertTrue(newAttempt().token().isEmpty());
    JobAttemptControl attempt = claimedAttempt();
    assertEquals(Optional.of(new AttemptToken(attempt.jobId(), 7)), attempt.token());
    JobClaimStore store = mock(JobClaimStore.class);
    assertFalse(newAttempt().isCurrent(store));
    attempt.markSuperseded();
    assertTrue(attempt.isSuperseded());
    assertFalse(attempt.isCurrent(store));
    attempt.requestCancellation();
    assertTrue(attempt.isSuperseded());
    verifyNoInteractions(store);
  }

  @Test
  void currentReadTracksRunningWithoutStickingOnPaused() {
    JobAttemptControl attempt = claimedAttempt();
    JobClaimStore store = mock(JobClaimStore.class);
    when(store.currentClaim(attempt.jobId()))
        .thenReturn(
            Optional.of(new ClaimState(JobStatus.RUNNING, 7)),
            Optional.of(new ClaimState(JobStatus.PAUSED, 7)),
            Optional.of(new ClaimState(JobStatus.RUNNING, 7)));
    assertTrue(attempt.isCurrent(store));
    assertFalse(attempt.isCurrent(store));
    assertFalse(attempt.isSuperseded());
    assertTrue(attempt.isCurrent(store));
    verify(store, times(3)).currentClaim(attempt.jobId());
  }

  @Test
  void missingAndDifferentClaimsAreSticky() {
    for (Optional<ClaimState> state :
        List.of(Optional.<ClaimState>empty(), Optional.of(new ClaimState(JobStatus.RUNNING, 8)))) {
      JobAttemptControl attempt = claimedAttempt();
      JobClaimStore store = mock(JobClaimStore.class);
      when(store.currentClaim(attempt.jobId())).thenReturn(state);
      assertFalse(attempt.isCurrent(store));
      assertTrue(attempt.isSuperseded());
      assertFalse(attempt.isCurrent(store));
      verify(store).currentClaim(attempt.jobId());
    }
  }

  @Test
  void storeErrorsPropagateWithoutSuperseding() {
    JobAttemptControl attempt = claimedAttempt();
    JobClaimStore store = mock(JobClaimStore.class);
    IllegalStateException failure = new IllegalStateException("store down");
    when(store.currentClaim(attempt.jobId()))
        .thenThrow(failure)
        .thenReturn(Optional.of(new ClaimState(JobStatus.RUNNING, 7)));
    assertSame(failure, assertThrows(IllegalStateException.class, () -> attempt.isCurrent(store)));
    assertFalse(attempt.isSuperseded());
    assertTrue(attempt.isCurrent(store));
  }

  @Test
  void supersedingDuringReadWins() {
    JobAttemptControl attempt = claimedAttempt();
    JobClaimStore store = mock(JobClaimStore.class);
    when(store.currentClaim(attempt.jobId()))
        .thenAnswer(
            invocation -> {
              attempt.markSuperseded();
              return Optional.of(new ClaimState(JobStatus.RUNNING, 7));
            });
    assertFalse(attempt.isCurrent(store));
    assertTrue(attempt.isSuperseded());
  }

  @Test
  void retainsClaimFenceAndNullableNode() {
    JobAttemptControl attempt =
        new JobAttemptControl(
            new UUID(0L, 42L), Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7L, "node-1");
    assertEquals(7L, attempt.claimSeq());
    assertEquals("node-1", attempt.nodeId());
    JobAttemptControl withoutNode =
        new JobAttemptControl(
            new UUID(0L, 42L), Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7L, null);
    assertNull(withoutNode.nodeId());
  }

  @Test
  void cancellationIdentitiesAreStableAndDistinct() {
    JobAttemptControl first = newAttempt();
    JobAttemptControl second = newAttempt();
    assertNotNull(first.cancellationIdentity());
    assertNotNull(second.cancellationIdentity());
    assertSame(first.cancellationIdentity(), first.cancellationIdentity());
    assertNotSame(first.cancellationIdentity(), second.cancellationIdentity());
  }

  @Test
  void workerCanHandTimeoutBackForWatchdogToClaim() {
    JobAttemptControl attempt = newAttempt();
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWorker());
    assertTrue(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWorker());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.claimTimeoutForWatchdog());
    assertTrue(attempt.isRevoked());
    assertTrue(attempt.isSuperseded());
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWatchdog());
  }

  @Test
  void watchdogCanClaimUnclaimedTimeoutAndExcludeWorker() {
    JobAttemptControl attempt = newAttempt();
    assertTrue(attempt.claimTimeoutForWatchdog());
    assertTrue(attempt.isRevoked());
    assertTrue(attempt.isSuperseded());
    assertFalse(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertFalse(attempt.isTimeoutHandedBack());
  }

  @Test
  void watchdogPassingWorkerPreventsHandBack() {
    JobAttemptControl attempt = newAttempt();
    assertTrue(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.isRevoked());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.isRevoked());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWorker());
  }

  @Test
  void cancellationBeforeStartPreventsBodyAndNeverInterrupts() {
    JobAttemptControl attempt = newAttempt();
    Future<?> future = mock(Future.class);
    when(future.cancel(false)).thenReturn(true, false);
    assertTrue(attempt.cancelExecution(future));
    assertFalse(attempt.cancelExecution(future));
    assertFalse(attempt.markBodyStarted());
    verify(future, times(2)).cancel(false);
  }

  @Test
  void onlyRunningBodyCanBeInterrupted() {
    JobAttemptControl attempt = newAttempt();
    Future<?> future = mock(Future.class);
    when(future.cancel(true)).thenReturn(true);
    assertTrue(attempt.markBodyStarted());
    assertTrue(attempt.cancelExecution(future));
    verify(future).cancel(true);
    attempt.markBodyFinished();
    assertFalse(attempt.markBodyStarted());
    assertFalse(attempt.cancelExecution(future));
    verify(future).cancel(false);
  }

  @Test
  void finishedBodyCanStartAgainUntilCancelledBetweenBodies() {
    JobAttemptControl attempt = newAttempt();
    Future<?> future = mock(Future.class);
    assertTrue(attempt.markBodyStarted());
    attempt.markBodyFinished();
    assertTrue(attempt.markBodyStarted());
    attempt.markBodyFinished();
    attempt.cancelExecution(future);
    assertFalse(attempt.markBodyStarted());
    verify(future).cancel(false);
  }

  @Test
  void cancellationAfterLastBodyPreventsAnyLaterStart() {
    JobAttemptControl attempt = newAttempt();
    Future<?> future = mock(Future.class);
    assertTrue(attempt.markBodyStarted());
    attempt.markBodyFinished();
    attempt.cancelExecution(future);
    attempt.markBodyFinished();
    assertFalse(attempt.markBodyStarted());
    verify(future).cancel(false);
  }

  @Test
  void racingCancellationCannotInterruptAfterGateCloses() throws Exception {
    for (int iteration = 0; iteration < 100; iteration++) {
      JobAttemptControl attempt = newAttempt();
      CountDownLatch bodyStarted = new CountDownLatch(1);
      CountDownLatch race = new CountDownLatch(1);
      CountDownLatch finished = new CountDownLatch(1);
      boolean cancelDuringBody = iteration % 3 == 0;
      boolean cancelAfterBody = iteration % 3 == 1;
      CountDownLatch cancelled = new CountDownLatch(1);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      FutureTask<Void> future =
          new FutureTask<>(
              () -> {
                try {
                  assertTrue(attempt.markBodyStarted());
                  bodyStarted.countDown();
                  if (cancelDuringBody) cancelled.await();
                  else race.await();
                } catch (InterruptedException expected) {
                  // Cancellation wins while the body is waiting.
                } catch (Throwable thrown) {
                  failure.set(thrown);
                } finally {
                  attempt.markBodyFinished();
                  Thread.interrupted();
                  finished.countDown();
                }
                try {
                  cancelled.await();
                  assertFalse(Thread.currentThread().isInterrupted());
                } catch (Throwable thrown) {
                  failure.set(thrown);
                }
                return null;
              });
      Thread worker = new Thread(future);
      Thread canceller =
          new Thread(
              () -> {
                try {
                  assertTrue(bodyStarted.await(5, TimeUnit.SECONDS));
                  race.countDown();
                  if (cancelAfterBody) assertTrue(finished.await(5, TimeUnit.SECONDS));
                  attempt.cancelExecution(future);
                } catch (Throwable thrown) {
                  failure.set(thrown);
                } finally {
                  cancelled.countDown();
                }
              });
      worker.setUncaughtExceptionHandler((thread, thrown) -> failure.set(thrown));
      worker.start();
      canceller.start();
      worker.join(5000);
      canceller.join(5000);
      assertFalse(worker.isAlive());
      assertFalse(canceller.isAlive());
      assertNull(failure.get(), () -> String.valueOf(failure.get()));
    }
  }
}
