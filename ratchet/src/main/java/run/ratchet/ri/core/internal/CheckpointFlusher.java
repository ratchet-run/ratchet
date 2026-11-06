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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;

/** Event-driven, bounded checkpoint persistence; idle nodes schedule no work. */
@ApplicationScoped
public class CheckpointFlusher {
  private static final Logger log = Logger.getLogger(CheckpointFlusher.class);
  private final Set<JobCheckpointWriter> dirty = ConcurrentHashMap.newKeySet();
  private final long intervalMs;
  // The scheduling monitor never encloses writer calls: writers call markDirty under their lock.
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> handle;
  private boolean scheduled;

  protected CheckpointFlusher() {
    intervalMs = 1000;
  }

  @Inject
  public CheckpointFlusher(RatchetOptions options) {
    intervalMs = options.execution().checkpointFlushIntervalMs();
  }

  public void markDirty(JobCheckpointWriter writer) {
    dirty.add(writer);
    scheduleIfNeeded();
  }

  public void remove(JobCheckpointWriter writer) {
    dirty.remove(writer);
  }

  public synchronized void start(ScheduledExecutorService executor) {
    this.executor = executor;
    scheduleIfNeeded();
  }

  public synchronized void stop() {
    executor = null;
    if (handle != null) handle.cancel(false);
    handle = null;
    scheduled = false;
  }

  private synchronized void scheduleIfNeeded() {
    if (executor == null || scheduled || dirty.isEmpty()) return;
    scheduled = true;
    try {
      handle = executor.schedule(this::flushDirty, intervalMs, TimeUnit.MILLISECONDS);
    } catch (RuntimeException failure) {
      scheduled = false;
      log.warn("Could not schedule checkpoint flush", failure);
    }
  }

  void flushDirty() {
    try {
      drain(true);
    } catch (Throwable failure) {
      log.warn("Checkpoint flush failed", failure);
    } finally {
      synchronized (this) {
        scheduled = false;
        handle = null;
      }
      scheduleIfNeeded();
    }
  }

  public void flushAll() {
    drain(false);
  }

  private void drain(boolean bounded) {
    long started = System.nanoTime();
    int count = 0;
    // Snapshot limits each writer to one attempt per drain, including writers that keep failing.
    for (JobCheckpointWriter writer : new ArrayList<>(dirty)) {
      if (bounded && (count >= 100 || System.nanoTime() - started >= 200_000_000L)) break;
      if (!dirty.remove(writer)) continue;
      count++;
      try {
        writer.flush();
      } catch (Throwable failure) {
        log.warnf(failure, "Checkpoint flush failed for job %s", writer.jobId());
      } finally {
        synchronized (writer) {
          if (writer.hasUnflushed()) dirty.add(writer);
        }
      }
    }
  }
}
