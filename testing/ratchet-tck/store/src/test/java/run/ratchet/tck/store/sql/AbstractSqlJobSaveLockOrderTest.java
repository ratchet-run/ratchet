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
package run.ratchet.tck.store.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.tck.store.JpaContainerFixture;

/** Pins queue-before-job locking when save races a CAS cancel in the bundled SQL stores. */
public abstract class AbstractSqlJobSaveLockOrderTest {

  protected abstract JpaContainerFixture fixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  /** Pins a rescheduling save waiting for a cancel's queue lock without deadlocking. */
  @Test
  void saveThatReschedulesWhileCancelHoldsTheQueueRowWaitsInsteadOfDeadlocking() throws Exception {
    var store = fixture().store();
    var pending = store.create(fixture().newPendingJob());
    var loaded = store.findById(pending.getId()).orElseThrow();
    loaded.setScheduledTime(Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MILLIS));
    var caller = Thread.currentThread();
    var fired = new AtomicBoolean();
    var saveWrote = new CountDownLatch(1);
    var saveStarted = new CountDownLatch(1);
    var saveWroteMidCancel = new AtomicBoolean();
    var save = new AtomicReference<Future<?>>();
    var transientFailures = new ConcurrentLinkedQueue<String>();
    var executor = Executors.newSingleThreadExecutor();
    try {
      // The fixture retries a transient failure, so a deadlock would otherwise pass unseen.
      fixture().observeTransientFailures((method, failure) -> transientFailures.add(method));
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                String normalized = sql.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
                if (Thread.currentThread() != caller && !event.completed()) {
                  if (normalized.trim().startsWith("SELECT")
                      && normalized.contains("SCHEDULER_JOB_QUEUE")) {
                    saveStarted.countDown();
                  }
                  if (event.operation().equals("executeUpdate")
                      && normalized.trim().startsWith("UPDATE SCHEDULER_JOB")) {
                    saveWrote.countDown();
                  }
                }
                if (Thread.currentThread() == caller
                    && event.completed()
                    && normalized.trim().startsWith("SELECT JOB_ID FROM SCHEDULER_JOB_QUEUE")
                    && normalized.contains("AND STATUS = ?")
                    && fired.compareAndSet(false, true)) {
                  save.set(executor.submit(() -> store.save(loaded)));
                  try {
                    if (!saveStarted.await(10, TimeUnit.SECONDS)) {
                      throw new IllegalStateException("save did not start its queue-row lock");
                    }
                    saveWroteMidCancel.set(saveWrote.await(1, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                }
              });
      assertTrue(
          store.compareAndSwapStatus(pending.getId(), JobStatus.PENDING, JobStatus.CANCELED, null));
      assertTrue(fired.get());
      assertFalse(saveWroteMidCancel.get(), "the save wrote a job or queue row mid-cancel");
      var failure =
          assertThrows(ExecutionException.class, () -> save.get().get(30, TimeUnit.SECONDS));
      assertInstanceOf(RatchetOptimisticLockException.class, failure.getCause());
      assertFalse(failure.getCause() instanceof RatchetTransientStoreException);
      assertEquals(JobStatus.CANCELED, store.findById(pending.getId()).orElseThrow().getStatus());
      assertTrue(transientFailures.isEmpty(), "transient failures: " + transientFailures);
    } finally {
      fixture().observeTransientFailures((method, failure) -> {});
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
