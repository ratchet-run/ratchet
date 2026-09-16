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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;

class JobSuccessFinalizerTest {

  private static final UUID JOB_ID = new UUID(0L, 42L);
  private static final Instant START = Instant.parse("2026-07-12T12:00:00Z");
  private static final Instant END = START.plusSeconds(1);

  private PostExecutionHandler lifecycleFacade;
  private ExecutionObserver observer;
  private JobEntity job;
  private List<Long> delays;

  @BeforeEach
  void setUp() {
    lifecycleFacade = mock(PostExecutionHandler.class);
    observer = mock(ExecutionObserver.class);
    job = new JobEntity();
    job.setId(JOB_ID);
    delays = new ArrayList<>();
  }

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void transientConflictRetriesAndPersistsTheFullResult() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(true);
    JobSuccessFinalizer finalizer = finalizer(delays::add, 0L);

    JobSuccessFinalizer.Outcome outcome = finalizeSuccess(finalizer);

    assertEquals(JobSuccessFinalizer.Outcome.COMPLETED_FULL, outcome);
    assertEquals(List.of(25L), delays);
    verify(lifecycleFacade, times(2)).completeSuccess(job, "json", "type", START, END, 1_000L, 25L);
    verify(lifecycleFacade, never())
        .completeSuccessMinimal(any(JobEntity.class), any(), any(), anyLong(), anyLong());
    verify(observer).recordSuccessFinalizationRetry(job);
  }

  @Test
  void exhaustedFullWritesFallBackToMinimalSuccess() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(job, START, END, 1_000L, 25L)).thenReturn(true);
    JobSuccessFinalizer finalizer = finalizer(delays::add, 0L);

    JobSuccessFinalizer.Outcome outcome = finalizeSuccess(finalizer);

    assertEquals(JobSuccessFinalizer.Outcome.COMPLETED_MINIMAL, outcome);
    assertEquals(List.of(25L, 50L, 100L, 200L), delays);
    verify(lifecycleFacade, times(5)).completeSuccess(job, "json", "type", START, END, 1_000L, 25L);
    verify(observer, times(5)).recordSuccessFinalizationRetry(job);
    verify(observer).recordSuccessFinalizationMinimal(job);
  }

  @Test
  void minimalWriteConflictReportsStuckWithoutChangingTheOutcomeToFailure() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(job, START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    JobSuccessFinalizer finalizer = finalizer(delays::add, 0L);

    JobSuccessFinalizer.Outcome outcome = finalizeSuccess(finalizer);

    assertEquals(JobSuccessFinalizer.Outcome.STUCK, outcome);
    verify(observer).recordSuccessFinalizationStuck(job);
    verify(observer, never()).recordSuccessFinalizationMinimal(any());
  }

  @Test
  void falseCompareAndSwapReturnsTerminalSkippedWithoutRetrying() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenReturn(false);
    JobSuccessFinalizer finalizer = finalizer(delays::add, 0L);

    JobSuccessFinalizer.Outcome outcome = finalizeSuccess(finalizer);

    assertEquals(JobSuccessFinalizer.Outcome.TERMINAL_SKIPPED, outcome);
    assertTrue(delays.isEmpty());
    verify(observer, never()).recordSuccessFinalizationRetry(any());
  }

  @Test
  void interruptedRetryPreservesInterruptAndFallsBackToMinimalSuccess() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"));
    when(lifecycleFacade.completeSuccessMinimal(job, START, END, 1_000L, 25L)).thenReturn(true);
    JobSuccessFinalizer finalizer =
        finalizer(
            delay -> {
              delays.add(delay);
              throw new InterruptedException("stop");
            },
            0L);

    JobSuccessFinalizer.Outcome outcome = finalizeSuccess(finalizer);

    assertEquals(JobSuccessFinalizer.Outcome.COMPLETED_MINIMAL, outcome);
    assertEquals(List.of(25L), delays);
    assertTrue(Thread.currentThread().isInterrupted());
    verify(lifecycleFacade).completeSuccess(job, "json", "type", START, END, 1_000L, 25L);
    verify(lifecycleFacade).completeSuccessMinimal(job, START, END, 1_000L, 25L);
  }

  @Test
  void jitterIsAddedToTheConfiguredBackoff() {
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(new RatchetTransientStoreException("deadlock"))
        .thenReturn(true);
    JobSuccessFinalizer finalizer = finalizer(delays::add, 7L);

    finalizeSuccess(finalizer);

    assertEquals(List.of(32L), delays);
  }

  @Test
  void nonTransientStoreFailureStillEscapesToTheJobFailurePath() {
    IllegalStateException storeFailure = new IllegalStateException("database unavailable");
    when(lifecycleFacade.completeSuccess(job, "json", "type", START, END, 1_000L, 25L))
        .thenThrow(storeFailure);
    JobSuccessFinalizer finalizer = finalizer(delays::add, 0L);

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> finalizeSuccess(finalizer));

    assertEquals(storeFailure, failure);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  private JobSuccessFinalizer finalizer(JobSuccessFinalizer.Sleeper sleeper, long jitter) {
    return new JobSuccessFinalizer(lifecycleFacade, observer, sleeper, () -> jitter);
  }

  private JobSuccessFinalizer.Outcome finalizeSuccess(JobSuccessFinalizer finalizer) {
    return finalizer.finalizeSuccess(job, "json", "type", START, END, 1_000L, 25L);
  }
}
