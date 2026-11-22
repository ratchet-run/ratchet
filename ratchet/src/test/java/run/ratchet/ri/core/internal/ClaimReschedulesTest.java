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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.spi.JobRetryStore;

class ClaimReschedulesTest {
  private static final UUID JOB = UUID.randomUUID();
  private static final Instant TIME = Instant.parse("2026-11-17T00:00:00Z");
  private final JobRetryStore store = mock(JobRetryStore.class);
  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
  private final AtomicLong clock = new AtomicLong();
  private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
  private final ClaimReschedules reschedules =
      new ClaimReschedules(store, () -> scheduler, null, clock::get);

  @BeforeEach
  void captureTasks() {
    doAnswer(
            inv -> {
              tasks.add(inv.getArgument(0));
              return null;
            })
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  private boolean handOff(long seq, Runnable callback) {
    return reschedules.handOff(
        JOB, seq, "release", TIME, 2, () -> false, () -> {}, callback, () -> {});
  }

  private void drainAt(long seconds) {
    clock.set(TimeUnit.SECONDS.toNanos(seconds));
    tasks.remove().run();
  }

  private static Object drainLock(ClaimReschedules reschedules) {
    try {
      Field drainField = ClaimReschedules.class.getDeclaredField("drain");
      drainField.setAccessible(true);
      Field lockField = RetryDrain.class.getDeclaredField("lock");
      lockField.setAccessible(true);
      return lockField.get(drainField.get(reschedules));
    } catch (ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }

  @Test
  void concurrentHandoffAfterRefusalOwnsItsWriteAndCallbacks() throws Exception {
    Runnable firstAccepted = mock(Runnable.class);
    Runnable firstRejected = mock(Runnable.class);
    Runnable firstDropped = mock(Runnable.class);
    Runnable secondAccepted = mock(Runnable.class);
    Runnable secondRejected = mock(Runnable.class);
    Runnable secondDropped = mock(Runnable.class);
    AtomicReference<ClaimReschedules> owner = new AtomicReference<>();
    FutureTask<Boolean> second =
        new FutureTask<>(
            () ->
                owner
                    .get()
                    .handOff(
                        JOB,
                        7,
                        "second",
                        TIME,
                        3,
                        () -> false,
                        secondAccepted,
                        secondRejected,
                        secondDropped));
    AtomicBoolean firstRead = new AtomicBoolean(true);
    AtomicLong now = new AtomicLong();
    LongSupplier handoffClock =
        () -> {
          if (firstRead.compareAndSet(true, false)) {
            // This read is before submit now, but between reserve and schedule in the old code.
            // Complete the competing hand-off in that window, while neither version holds a lock.
            new Thread(second).start();
            try {
              second.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw new AssertionError(failure);
            } catch (ExecutionException | TimeoutException failure) {
              throw new AssertionError(failure);
            }
          }
          return now.get();
        };
    ScheduledExecutorService releaseScheduler = mock(ScheduledExecutorService.class);
    ArrayDeque<Runnable> captured = new ArrayDeque<>();
    AtomicInteger schedules = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (schedules.incrementAndGet() == 1)
                throw new RejectedExecutionException("first refused");
              captured.add(invocation.getArgument(0));
              return null;
            })
        .when(releaseScheduler)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
    ClaimReschedules competing =
        new ClaimReschedules(store, () -> releaseScheduler, null, handoffClock);
    owner.set(competing);
    when(store.releaseClaim(eq(JOB), any(), eq(TIME), anyInt(), eq(7L))).thenReturn(true);
    boolean firstResult =
        competing.handOff(
            JOB, 7, "first", TIME, 2, () -> false, firstAccepted, firstRejected, firstDropped);
    boolean secondResult = second.get(5, TimeUnit.SECONDS);
    assertEquals(1, (firstResult ? 1 : 0) + (secondResult ? 1 : 0));
    assertEquals(1, captured.size());
    now.set(TimeUnit.SECONDS.toNanos(1));
    captured.remove().run();
    verify(store)
        .releaseClaim(JOB, firstResult ? "first" : "second", TIME, firstResult ? 2 : 3, 7L);
    verifyNoMoreInteractions(store);
    Runnable accepted = firstResult ? firstAccepted : secondAccepted;
    Runnable refusedAccepted = firstResult ? secondAccepted : firstAccepted;
    verify(accepted).run();
    verifyNoMoreInteractions(accepted);
    verifyNoInteractions(
        refusedAccepted, firstRejected, firstDropped, secondRejected, secondDropped);
    assertTrue(captured.isEmpty());
  }

  @Test
  void acceptedCallbackErrorDoesNotRunDroppedCallback() {
    assertTerminalCallbackError(true);
  }

  @Test
  void rejectedCallbackErrorDoesNotRunDroppedCallback() {
    assertTerminalCallbackError(false);
  }

  private void assertTerminalCallbackError(boolean accepted) {
    Runnable terminal = mock(Runnable.class);
    Runnable unexpected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    AssertionError failure = new AssertionError("terminal callback error");
    doThrow(failure).when(terminal).run();
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L)).thenReturn(accepted);
    assertTrue(
        reschedules.handOff(
            JOB,
            7,
            "release",
            TIME,
            2,
            () -> false,
            accepted ? terminal : unexpected,
            accepted ? unexpected : terminal,
            dropped));
    assertSame(failure, assertThrows(AssertionError.class, () -> drainAt(1)));
    verify(terminal).run();
    verifyNoMoreInteractions(terminal);
    verifyNoInteractions(unexpected, dropped);
    assertTrue(tasks.isEmpty());
    assertTrue(handOff(8, () -> {}));
    drainAt(2);
    verifyNoInteractions(dropped);
  }

  @Test
  void revokedDroppedCallbackErrorRunsOnlyOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    AssertionError failure = new AssertionError("dropped callback error");
    doThrow(failure).when(dropped).run();
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> true, accepted, rejected, dropped));
    assertSame(failure, assertThrows(AssertionError.class, () -> drainAt(1)));
    verify(dropped).run();
    verifyNoMoreInteractions(dropped);
    verifyNoInteractions(store, accepted, rejected);
    assertTrue(tasks.isEmpty());
    assertTrue(handOff(7, () -> {}));
    drainAt(2);
    verifyNoMoreInteractions(dropped);
  }

  @Test
  void storeErrorDropsOnceAndLaterHandOffsStillWork() {
    Runnable dropped = mock(Runnable.class);
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    AssertionError failure = new AssertionError("store error");
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L)).thenThrow(failure).thenReturn(true);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    assertSame(failure, assertThrows(AssertionError.class, () -> drainAt(1)));
    verify(dropped).run();
    verifyNoMoreInteractions(dropped);
    verifyNoInteractions(accepted, rejected);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    drainAt(2);
    verify(store, times(2)).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(accepted).run();
    verifyNoMoreInteractions(accepted, dropped);
    verifyNoInteractions(rejected);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void drainYieldsAfterTimeBudget() {
    when(store.releaseClaim(eq(JOB), any(), any(), anyInt(), anyLong()))
        .thenAnswer(
            invocation -> {
              clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(600));
              return true;
            });
    for (int i = 0; i < 4; i++) handOff(i, () -> {});
    drainAt(1);
    verify(store, times(2)).releaseClaim(eq(JOB), any(), any(), anyInt(), anyLong());
    verify(scheduler).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.NANOSECONDS));
    tasks.remove().run();
    verify(store, times(4)).releaseClaim(eq(JOB), any(), any(), anyInt(), anyLong());
    assertTrue(tasks.isEmpty());
  }

  @Test
  void wrappedTransientFailureRetriesThenAcceptsOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RuntimeException(new SQLTransientConnectionException("connection lost")))
        .thenReturn(true);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    // The first try runs a second after hand-off, the second five seconds after it.
    verify(scheduler)
        .schedule(any(Runnable.class), eq(TimeUnit.SECONDS.toNanos(1)), eq(TimeUnit.NANOSECONDS));
    drainAt(1);
    verifyNoInteractions(accepted, rejected, dropped);
    verify(scheduler)
        .schedule(any(Runnable.class), eq(TimeUnit.SECONDS.toNanos(4)), eq(TimeUnit.NANOSECONDS));
    drainAt(5);
    verify(store, times(2)).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(accepted).run();
    verifyNoMoreInteractions(accepted);
    verifyNoInteractions(rejected, dropped);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void acceptedWriteRunsOnlyAcceptedCallbackOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L)).thenReturn(true);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    drainAt(1);
    verify(accepted).run();
    verifyNoMoreInteractions(accepted);
    verifyNoInteractions(rejected, dropped);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void rejectedWriteRunsOnlyRejectedCallbackOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    drainAt(1);
    verify(rejected).run();
    verifyNoMoreInteractions(rejected);
    verifyNoInteractions(accepted, dropped);
  }

  @Test
  void revokedEntryRunsOnlyDroppedCallbackOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> true, accepted, rejected, dropped));
    drainAt(1);
    verify(dropped).run();
    verifyNoMoreInteractions(dropped);
    verifyNoInteractions(store, accepted, rejected);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void nonTransientFailureRunsOnlyDroppedCallbackOnce() {
    Runnable accepted = mock(Runnable.class);
    Runnable rejected = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new IllegalStateException("broken store"));
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> false, accepted, rejected, dropped));
    drainAt(1);
    verify(dropped).run();
    verifyNoMoreInteractions(dropped);
    verifyNoInteractions(accepted, rejected);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void refusedRearmDropsEveryEntryEvenWhenCallbackThrows() {
    AtomicBoolean droppedUnderLock = new AtomicBoolean(true);
    AtomicBoolean otherUnderLock = new AtomicBoolean(true);
    Runnable dropped = mock(Runnable.class);
    Runnable other = mock(Runnable.class);
    Runnable unexpected = () -> fail("unexpected write result");
    doAnswer(
            invocation -> {
              droppedUnderLock.set(Thread.holdsLock(drainLock(reschedules)));
              throw new IllegalStateException("callback failed");
            })
        .when(dropped)
        .run();
    doAnswer(
            invocation -> {
              otherUnderLock.set(Thread.holdsLock(drainLock(reschedules)));
              return null;
            })
        .when(other)
        .run();
    when(store.releaseClaim(eq(JOB), any(), any(), anyInt(), anyLong()))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    assertTrue(
        reschedules.handOff(
            JOB, 7, "release", TIME, 2, () -> false, unexpected, unexpected, dropped));
    assertTrue(
        reschedules.handOff(
            JOB, 8, "release", TIME, 2, () -> false, unexpected, unexpected, other));
    doThrow(new RejectedExecutionException("stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    assertDoesNotThrow(() -> drainAt(1));
    verify(dropped).run();
    verify(other).run();
    verifyNoMoreInteractions(dropped, other);
    assertFalse(droppedUnderLock.get());
    assertFalse(otherUnderLock.get());
    assertTrue(tasks.isEmpty());
  }

  @Test
  void throwingAcceptedCallbackDoesNotDropEntryAndDrainRearms() {
    Runnable accepted = mock(Runnable.class);
    Runnable dropped = mock(Runnable.class);
    doThrow(new IllegalStateException("callback failed")).when(accepted).run();
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L)).thenReturn(true);
    when(store.releaseClaim(JOB, "release", TIME, 2, 8L))
        .thenThrow(new RatchetTransientStoreException("unavailable"))
        .thenReturn(true);
    assertTrue(
        reschedules.handOff(
            JOB, 7, "release", TIME, 2, () -> false, accepted, () -> fail("rejected"), dropped));
    handOff(8, () -> fail("rejected"));
    assertDoesNotThrow(() -> drainAt(1));
    assertEquals(1, tasks.size());
    drainAt(5);
    verify(accepted).run();
    verifyNoMoreInteractions(accepted);
    verifyNoInteractions(dropped);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void deduplicatesSameClaimAndKeepsFirstWriteAndCallback() {
    Runnable first = mock(Runnable.class);
    Runnable second = mock(Runnable.class);
    assertTrue(handOff(7, first));
    assertTrue(
        reschedules.handOff(
            JOB, 7, "other", TIME.plusSeconds(1), 9, () -> false, () -> {}, second, () -> {}));
    assertEquals(1, tasks.size());
    drainAt(1);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(first).run();
    verifyNoInteractions(second);
    verifyNoMoreInteractions(store);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void revokedAtFirstDrainRemovesItemWithoutWriteOrCallback() {
    Runnable callback = mock(Runnable.class);
    assertTrue(
        reschedules.handOff(JOB, 7, "release", TIME, 2, () -> true, () -> {}, callback, () -> {}));
    drainAt(1);
    verifyNoInteractions(store, callback);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void revokedAfterTransientFailureMakesNoFurtherWrite() {
    AtomicBoolean revoked = new AtomicBoolean();
    Runnable callback = mock(Runnable.class);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    assertTrue(
        reschedules.handOff(
            JOB, 7, "release", TIME, 2, revoked::get, () -> {}, callback, () -> {}));
    drainAt(1);
    revoked.set(true);
    drainAt(5);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verifyNoMoreInteractions(store);
    verifyNoInteractions(callback);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void throwingRevocationSupplierStillWrites() {
    Runnable callback = mock(Runnable.class);
    assertTrue(
        reschedules.handOff(
            JOB,
            7,
            "release",
            TIME,
            2,
            () -> {
              throw new IllegalStateException("revocation unavailable");
            },
            () -> {},
            callback,
            () -> {}));
    assertDoesNotThrow(() -> drainAt(1));
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(callback).run();
    assertTrue(tasks.isEmpty());
  }

  @Test
  void differentClaimOfSameJobIsSeparate() {
    handOff(7, () -> {});
    handOff(8, () -> {});
    drainAt(1);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 8L);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void transientFailuresBackOffAndNeverGiveUp() {
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    Runnable callback = mock(Runnable.class);
    handOff(7, callback);
    // First drain at 1s, first retry at 5s from hand-off; subsequent gaps double to 60s.
    long[] due = {1, 5, 15, 35, 75, 135, 195, 255, 315, 375, 435, 495, 555, 615, 675};
    int calls = 0;
    for (long next : due) {
      if (next > 1) {
        drainAt(next - 1);
        verify(store, times(calls)).releaseClaim(JOB, "release", TIME, 2, 7L);
      }
      drainAt(next);
      verify(store, times(++calls)).releaseClaim(JOB, "release", TIME, 2, 7L);
    }
    assertEquals(1, tasks.size());
    verifyNoInteractions(callback);
  }

  @Test
  void successRemovesItemWithoutCallbackAndLaterHandoffStartsNewDrain() {
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L)).thenReturn(true);
    Runnable callback = mock(Runnable.class);
    handOff(7, callback);
    drainAt(1);
    assertTrue(tasks.isEmpty());
    verifyNoInteractions(callback);
    handOff(7, callback);
    assertEquals(1, tasks.size());
    drainAt(2);
    verify(store, times(2)).releaseClaim(JOB, "release", TIME, 2, 7L);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void rejectionRunsCallbackOnceAndRemovesItem() {
    Runnable callback = mock(Runnable.class);
    handOff(7, callback);
    drainAt(1);
    verify(callback).run();
    assertTrue(tasks.isEmpty());
  }

  @Test
  void throwingCallbackDoesNotStopOtherItems() {
    Runnable callback = mock(Runnable.class);
    doThrow(new IllegalStateException("callback failed")).when(callback).run();
    handOff(7, callback);
    handOff(8, callback);
    assertDoesNotThrow(() -> drainAt(1));
    verify(callback, times(2)).run();
    assertTrue(tasks.isEmpty());
  }

  @Test
  void nonTransientFailureRemovesItemWithoutCallback() {
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new IllegalStateException("broken store"));
    Runnable callback = mock(Runnable.class);
    handOff(7, callback);
    drainAt(1);
    verifyNoInteractions(callback);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void eachDrainProcessesAtMostOneHundredItems() {
    for (int i = 0; i < 101; i++) handOff(i, () -> {});
    drainAt(1);
    verify(store, times(100)).releaseClaim(eq(JOB), eq("release"), eq(TIME), eq(2), anyLong());
    assertEquals(1, tasks.size());
    drainAt(2);
    verify(store, times(101)).releaseClaim(eq(JOB), eq("release"), eq(TIME), eq(2), anyLong());
    assertTrue(tasks.isEmpty());
  }

  @Test
  void refusedInitialScheduleReturnsFalseAndRetainsNothing() {
    doThrow(new RejectedExecutionException("stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    assertFalse(handOff(7, () -> fail("refused item retained")));
    captureTasks();
    assertTrue(handOff(8, () -> {}));
    drainAt(1);
    verify(store, never()).scheduleJobRetry(eq(JOB), any(), any(), anyInt(), eq(7L));
    verify(store, never()).releaseClaim(eq(JOB), any(), any(), anyInt(), eq(7L));
  }

  @Test
  void missingOrThrowingSchedulerRefusesHandoff() {
    assertFalse(
        new ClaimReschedules(store, () -> null, null)
            .handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}, () -> {}, () -> {}));
    assertFalse(
        new ClaimReschedules(
                store,
                () -> {
                  throw new IllegalStateException("missing");
                },
                null)
            .handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}, () -> {}, () -> {}));
    verifyNoInteractions(store);
  }

  @Test
  void refusedRearmClearsPendingItems() {
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RatchetTransientStoreException("unavailable"));
    handOff(7, () -> fail("cleared item retained"));
    doThrow(new RejectedExecutionException("stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    assertDoesNotThrow(() -> drainAt(1));
    assertTrue(tasks.isEmpty());
    captureTasks();
    handOff(8, () -> {});
    drainAt(100);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 8L);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void handoffDuringLastCallbackDoesNotLoseWakeup() {
    handOff(7, () -> handOff(8, () -> {}));
    drainAt(1);
    assertEquals(1, tasks.size());
    drainAt(2);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 8L);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void refusedHandoffRetainsItemAndUsesOneTimerWithCappedBackoff() {
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ArrayDeque<Runnable> rearms = new ArrayDeque<>();
    doAnswer(
            invocation -> {
              rearms.add(invocation.getArgument(0));
              return null;
            })
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
    doThrow(new RejectedExecutionException("busy"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    ClaimReschedules retrying =
        new ClaimReschedules(store, () -> scheduler, () -> timer, clock::get);
    assertTrue(
        retrying.handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}, () -> {}, () -> {}));
    assertTrue(
        retrying.handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}, () -> {}, () -> {}));
    assertEquals(1, rearms.size());
    for (int i = 0; i < 7; i++) rearms.remove().run();
    ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
    verify(timer, times(8))
        .schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.NANOSECONDS));
    assertEquals(
        List.of(1L, 2L, 4L, 8L, 16L, 32L, 60L, 60L).stream()
            .map(TimeUnit.SECONDS::toNanos)
            .toList(),
        delays.getAllValues());
    verifyNoInteractions(store);
    captureTasks();
    rearms.remove().run();
    verifyNoInteractions(store);
    drainAt(100);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    assertTrue(rearms.isEmpty());
    assertTrue(tasks.isEmpty());
  }

  @Test
  void refusedDrainRearmRetainsPendingUntilTimerResubmits() {
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ArrayDeque<Runnable> rearms = new ArrayDeque<>();
    doAnswer(
            invocation -> {
              rearms.add(invocation.getArgument(0));
              return null;
            })
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
    ClaimReschedules retrying =
        new ClaimReschedules(store, () -> scheduler, () -> timer, clock::get);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RatchetTransientStoreException("down"))
        .thenReturn(true);
    assertTrue(
        retrying.handOff(
            JOB, 7, "release", TIME, 2, () -> false, () -> {}, () -> fail("rejected"), () -> {}));
    doThrow(new RejectedExecutionException("busy"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    drainAt(1);
    assertEquals(1, rearms.size());
    captureTasks();
    rearms.remove().run();
    drainAt(5);
    verify(store, times(2)).releaseClaim(JOB, "release", TIME, 2, 7L);
    assertTrue(tasks.isEmpty());
  }

  @Test
  void bothExecutorsRefusingDropsHandoffAndDrainItems() {
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    doThrow(new RejectedExecutionException("busy"))
        .when(timer)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    ClaimReschedules retrying =
        new ClaimReschedules(store, () -> scheduler, () -> timer, clock::get);
    AtomicBoolean droppedUnderLock = new AtomicBoolean(true);
    when(store.releaseClaim(JOB, "release", TIME, 2, 7L))
        .thenThrow(new RatchetTransientStoreException("down"));
    assertTrue(
        retrying.handOff(
            JOB,
            7,
            "release",
            TIME,
            2,
            () -> false,
            () -> {},
            () -> fail("rejected"),
            () -> droppedUnderLock.set(Thread.holdsLock(drainLock(retrying)))));
    doThrow(new RejectedExecutionException("busy"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    drainAt(1);
    assertFalse(droppedUnderLock.get());
    assertFalse(
        retrying.handOff(
            JOB, 8, "release", TIME, 2, () -> false, () -> {}, () -> fail("rejected"), () -> {}));
    captureTasks();
    assertTrue(
        retrying.handOff(JOB, 9, "release", TIME, 2, () -> false, () -> {}, () -> {}, () -> {}));
    drainAt(100);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 7L);
    verify(store).releaseClaim(JOB, "release", TIME, 2, 9L);
    verify(store, never()).releaseClaim(eq(JOB), any(), any(), anyInt(), eq(8L));
    assertTrue(tasks.isEmpty());
  }
}
