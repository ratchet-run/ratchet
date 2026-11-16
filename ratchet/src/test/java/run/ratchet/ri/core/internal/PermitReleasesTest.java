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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.store.spi.ResourcePermitStore;

@ExtendWith(MockitoExtension.class)
class PermitReleasesTest {
  private static final String RESOURCE = "gpu";
  private static final UUID JOB_ID = UUID.randomUUID();
  private static final long CLAIM_SEQ = 7L;
  // The documented cadence: every 5 seconds, 60 times, so about 5 minutes after the first failure.
  private static final long RETRY_DELAY_MS = 5_000L;
  private static final int MAX_RETRIES = 60;

  @Mock private ResourcePermitService permits;
  @Mock private ScheduledExecutorService scheduler;

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  private PermitReleases releases() {
    return new PermitReleases(permits, () -> scheduler);
  }

  @Test
  void release_onFreeThread_releasesAtOnce() {
    // A free worker releases its claim's permit inline and schedules nothing.
    assertTrue(releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoInteractions(scheduler);
  }

  @Test
  void release_onInterruptedThread_handsOffAtOnceAndKeepsTheInterrupt() {
    // A JDBC call on an interrupted thread can break the pooled connection, so the worker does not
    // call the store. The release goes to the scheduler with no delay and the interrupt stays set.
    Thread.currentThread().interrupt();
    assertFalse(releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));
    verifyNoInteractions(permits);
    assertTrue(Thread.currentThread().isInterrupted());

    Runnable task = scheduled(1, 0L);
    Thread.interrupted();
    task.run();
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void release_failure_retriesTheSameClaimAfterTheDelay() {
    // A failed release is retried on the scheduler for the same resource, job and claim, so a retry
    // can never remove a permit that a newer attempt of the job took.
    doThrow(new RatchetTransientStoreException("lock wait timeout"))
        .doNothing()
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);

    assertFalse(assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ)));
    scheduled(1, RETRY_DELAY_MS).run();

    verify(permits, times(2)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(permits);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void retry_keepsRetryingUntilTheReleaseLands() {
    // Each failed retry schedules the next one; the first success ends the retries.
    doThrow(new RatchetTransientStoreException("first"))
        .doThrow(new IllegalStateException("second"))
        .doNothing()
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    scheduled(1, RETRY_DELAY_MS).run();
    scheduled(2, RETRY_DELAY_MS).run();

    verify(permits, times(3)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void retry_givesUpAfterTheRetryLimit() {
    // A release that keeps failing stops after the inline call and the bounded retries.
    doThrow(new RatchetTransientStoreException("store unavailable"))
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);

    releases().release(RESOURCE, JOB_ID, CLAIM_SEQ);
    for (int i = 1; i <= MAX_RETRIES; i++) {
      scheduled(i, RETRY_DELAY_MS).run();
    }

    verify(permits, times(1 + MAX_RETRIES)).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  void release_schedulerRejectsHandoff_doesNotThrowOrCallTheStore() {
    // A stopped scheduler rejects the handoff. The worker returns normally, keeps its interrupt
    // and still does not call the store.
    doThrow(new RejectedExecutionException("scheduler stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    Thread.currentThread().interrupt();

    assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));

    assertTrue(Thread.currentThread().isInterrupted());
    scheduled(1, 0L);
    verify(permits, never()).release(anyString(), any(UUID.class), anyLong());
  }

  @Test
  void release_schedulerRejectsRetry_doesNotThrow() {
    // A failed release whose retry the scheduler rejects returns normally.
    doThrow(new RatchetTransientStoreException("store unavailable"))
        .when(permits)
        .release(RESOURCE, JOB_ID, CLAIM_SEQ);
    doThrow(new RejectedExecutionException("scheduler stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    assertDoesNotThrow(() -> releases().release(RESOURCE, JOB_ID, CLAIM_SEQ));

    scheduled(1, RETRY_DELAY_MS);
    verify(permits).release(RESOURCE, JOB_ID, CLAIM_SEQ);
  }

  @Test
  void retry_thatLands_wakesTheResourceWaiters() {
    // The retry goes through the permit service, so a release that lands on retry also wakes the
    // jobs waiting for the resource.
    ResourcePermitStore store = mock(ResourcePermitStore.class);
    JobWakeupService wakeup = mock(JobWakeupService.class);
    DefaultResourcePermitService service =
        new DefaultResourcePermitService(store, wakeup, action -> Result.NO_ACTIVE_TRANSACTION);
    doThrow(new RatchetTransientStoreException("lock wait timeout"))
        .doNothing()
        .when(store)
        .releasePermit(RESOURCE, JOB_ID, CLAIM_SEQ);
    when(store.wakeWaiters(RESOURCE)).thenReturn(List.of());

    new PermitReleases(service, () -> scheduler).release(RESOURCE, JOB_ID, CLAIM_SEQ);
    verify(store, never()).wakeWaiters(anyString());
    scheduled(1, RETRY_DELAY_MS).run();

    InOrder order = inOrder(store);
    order.verify(store, times(2)).releasePermit(RESOURCE, JOB_ID, CLAIM_SEQ);
    order.verify(store).wakeWaiters(RESOURCE);
  }

  private Runnable scheduled(int count, long delayMillis) {
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(count))
        .schedule(task.capture(), eq(delayMillis), eq(TimeUnit.MILLISECONDS));
    return task.getAllValues().get(count - 1);
  }
}
