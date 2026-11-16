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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class HeldKeyReleasesTest {
  private static final UUID JOB_ID = UUID.randomUUID();
  private static final long CLAIM_SEQ = 7L;
  @Mock private JobTerminalStore store;
  @Mock private ScheduledExecutorService scheduler;

  private HeldKeyReleases releases() {
    return new HeldKeyReleases(store, () -> scheduler);
  }

  @Test
  void release_onFreeThread_callsStoreAtOnce() {
    // A free worker releases the hold inline without scheduling.
    try {
      releases().release(JOB_ID, CLAIM_SEQ);
      verify(store).releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ);
      verifyNoInteractions(scheduler);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void release_onInterruptedThread_handsOffAndKeepsTheInterrupt() {
    // An interrupted worker hands off immediately without clearing its interrupt.
    try {
      Thread.currentThread().interrupt();
      releases().release(JOB_ID, CLAIM_SEQ);
      verifyNoInteractions(store);
      assertTrue(Thread.currentThread().isInterrupted());
      Runnable task = scheduled(1, 0L);
      Thread.interrupted();
      task.run();
      verify(store).releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ);
      verifyNoMoreInteractions(scheduler);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void release_storeFailure_retriesAfterDelay() {
    // A failed inline release is retried once on the scheduler.
    try {
      when(store.releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ))
          .thenThrow(new IllegalStateException("store unavailable"))
          .thenReturn(true);
      assertDoesNotThrow(() -> releases().release(JOB_ID, CLAIM_SEQ));
      scheduled(1, ReleaseRetries.RETRY_DELAY.toMillis()).run();
      verify(store, times(2)).releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ);
      verifyNoMoreInteractions(scheduler);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void retry_keepsRetryingUntilTheStoreAnswers() {
    // Each failure retries, while a false store answer ends retrying.
    try {
      when(store.releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ))
          .thenThrow(new IllegalStateException("first"))
          .thenThrow(new IllegalStateException("second"))
          .thenReturn(false);
      releases().release(JOB_ID, CLAIM_SEQ);
      scheduled(1, ReleaseRetries.RETRY_DELAY.toMillis()).run();
      scheduled(2, ReleaseRetries.RETRY_DELAY.toMillis()).run();
      verify(store, times(3)).releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ);
      verifyNoMoreInteractions(scheduler);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void retry_givesUpAfterMaxRetries() {
    // Persistent failure stops after the inline attempt and the retry limit.
    try {
      when(store.releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ))
          .thenThrow(new IllegalStateException("store unavailable"));
      releases().release(JOB_ID, CLAIM_SEQ);
      for (int i = 1; i <= ReleaseRetries.MAX_RETRIES; i++) {
        scheduled(i, ReleaseRetries.RETRY_DELAY.toMillis()).run();
      }
      verify(store, times(1 + ReleaseRetries.MAX_RETRIES))
          .releaseBusinessKeyHold(JOB_ID, CLAIM_SEQ);
      verifyNoMoreInteractions(scheduler);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void release_schedulerRejects_doesNotThrow() {
    // A rejected interrupted handoff returns normally and preserves the interrupt.
    try {
      doThrow(new RejectedExecutionException("scheduler stopped"))
          .when(scheduler)
          .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
      Thread.currentThread().interrupt();
      assertDoesNotThrow(() -> releases().release(JOB_ID, CLAIM_SEQ));
      assertTrue(Thread.currentThread().isInterrupted());
      scheduled(1, 0L);
      verify(store, never()).releaseBusinessKeyHold(any(), anyLong());
    } finally {
      Thread.interrupted();
    }
  }

  private Runnable scheduled(int count, long delayMillis) {
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(count))
        .schedule(task.capture(), eq(delayMillis), eq(TimeUnit.MILLISECONDS));
    return task.getAllValues().get(count - 1);
  }
}
