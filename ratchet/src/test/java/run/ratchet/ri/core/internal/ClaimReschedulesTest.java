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

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
      new ClaimReschedules(store, () -> scheduler, clock::get);

  @BeforeEach
  void captureTasks() {
    doAnswer(
            inv -> {
              tasks.add(inv.getArgument(0));
              return null;
            })
        .when(scheduler)
        .schedule(any(Runnable.class), eq(1L), eq(TimeUnit.SECONDS));
  }

  private boolean handOff(long seq, Runnable callback) {
    return reschedules.handOff(JOB, seq, "release", TIME, 2, () -> false, callback);
  }

  private void drainAt(long seconds) {
    clock.set(TimeUnit.SECONDS.toNanos(seconds));
    tasks.remove().run();
  }

  @Test
  void deduplicatesSameClaimAndKeepsFirstWriteAndCallback() {
    Runnable first = mock(Runnable.class);
    Runnable second = mock(Runnable.class);
    assertTrue(handOff(7, first));
    assertTrue(reschedules.handOff(JOB, 7, "other", TIME.plusSeconds(1), 9, () -> false, second));
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
    assertTrue(reschedules.handOff(JOB, 7, "release", TIME, 2, () -> true, callback));
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
    assertTrue(reschedules.handOff(JOB, 7, "release", TIME, 2, revoked::get, callback));
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
            callback));
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
        new ClaimReschedules(store, () -> null)
            .handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}));
    assertFalse(
        new ClaimReschedules(
                store,
                () -> {
                  throw new IllegalStateException("missing");
                })
            .handOff(JOB, 7, "release", TIME, 2, () -> false, () -> {}));
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
}
