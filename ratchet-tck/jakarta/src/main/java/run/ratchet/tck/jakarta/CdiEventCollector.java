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
package run.ratchet.tck.jakarta;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobSignaledEvent;

/**
 * CDI bean that collects {@link JobCompletedEvent}s observed via {@code @Observes} for use by
 * {@link AbstractCdiEventContract}. Co-shipped with the contracts so subclasses can bundle it in
 * their deployment with {@code addClasses(CdiEventCollector.class)} (or an {@code addPackage} of
 * this contract package).
 *
 * <p>Application-scoped so the test methods see one shared instance and the implementation's event
 * dispatcher binds the observer at deployment time.
 */
@ApplicationScoped
public class CdiEventCollector {

  private final Set<UUID> observedJobIds = ConcurrentHashMap.newKeySet();
  private final Set<UUID> observedSignaledJobIds = ConcurrentHashMap.newKeySet();
  private final Object lock = new Object();

  /**
   * Blocks up to {@code timeout} for {@code jobId} to be observed via the CDI {@code @Observes}
   * pathway. Returns {@code true} on observation, {@code false} on timeout.
   */
  public boolean awaitJobId(UUID jobId, Duration timeout) {
    return awaitObserved(observedJobIds, jobId, timeout);
  }

  /** Blocks up to {@code timeout} for {@code jobId} to be observed as signaled via CDI. */
  public boolean awaitSignaledJobId(UUID jobId, Duration timeout) {
    return awaitObserved(observedSignaledJobIds, jobId, timeout);
  }

  /** Clears all observed job ids. Call from {@code @AfterEach}. */
  public void reset() {
    observedJobIds.clear();
    observedSignaledJobIds.clear();
  }

  void onCompleted(@Observes JobCompletedEvent event) {
    if (event != null && event.getJobId() != null) {
      observedJobIds.add(event.getJobId());
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  void onSignaled(@Observes JobSignaledEvent event) {
    if (event != null && event.getJobId() != null) {
      observedSignaledJobIds.add(event.getJobId());
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  private boolean awaitObserved(Set<UUID> observedIds, UUID jobId, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    // Guarded wait: the condition is re-checked while holding the monitor before
    // each wait(), so a notifyAll() landing between the check and the wait cannot
    // be lost. Re-checking outside the lock (the previous form) let a notify slip
    // through that window and forced a full-timeout sleep on every such race.
    synchronized (lock) {
      while (!observedIds.contains(jobId)) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        try {
          lock.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return true;
    }
  }
}
