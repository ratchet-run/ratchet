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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;

/** Event-driven, bounded state persistence; idle nodes schedule no work. */
@ApplicationScoped
public class JobStateFlusher {
  private static final Logger log = Logger.getLogger(JobStateFlusher.class);
  private final Map<FlushableState, Long> dirty = new ConcurrentHashMap<>();
  private final long checkpointIntervalMs;
  private final long progressIntervalMs;
  private final LongSupplier nanoTime;
  private final Object drainLock = new Object();
  // Never call writers under this monitor: writers call markDirty under their own monitor.
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> handle;
  private long scheduledDue;
  private long generation;
  private boolean draining;
  // Set when the executor rejected a schedule, so the next markDirty retries even for a queued
  // writer.
  private volatile boolean scheduleFailed;

  protected JobStateFlusher() {
    this(RatchetOptions.builder().build(), System::nanoTime);
  }

  @Inject
  public JobStateFlusher(RatchetOptions options) {
    this(options, System::nanoTime);
  }

  JobStateFlusher(RatchetOptions options, LongSupplier nanoTime) {
    checkpointIntervalMs = options.execution().checkpointFlushIntervalMs();
    progressIntervalMs = options.execution().progressFlushIntervalMs();
    this.nanoTime = nanoTime;
  }

  public long checkpointFlushIntervalMs() {
    return checkpointIntervalMs;
  }

  public long progressFlushIntervalMs() {
    return progressIntervalMs;
  }

  public void markDirty(FlushableState writer) {
    long due = nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(writer.flushIntervalMs());
    if (dirty.putIfAbsent(writer, due) == null || scheduleFailed) scheduleIfNeeded();
  }

  public void remove(FlushableState writer) {
    dirty.remove(writer);
    if (dirty.isEmpty()) {
      synchronized (this) {
        if (dirty.isEmpty() && handle != null) {
          handle.cancel(false);
          handle = null;
          generation++;
        }
      }
    }
  }

  public synchronized void start(ScheduledExecutorService executor) {
    this.executor = executor;
    scheduleIfNeeded();
  }

  public synchronized void stop() {
    executor = null;
    generation++;
    if (handle != null) handle.cancel(false);
    handle = null;
  }

  private synchronized void scheduleIfNeeded() {
    if (executor == null || draining || dirty.isEmpty()) return;
    long now = nanoTime.getAsLong();
    var earliest = dirty.values().stream().min((a, b) -> Long.compare(a - now, b - now));
    if (earliest.isEmpty()) return;
    long due = earliest.get();
    if (handle != null && due - scheduledDue >= 0) return;
    if (handle != null) handle.cancel(false);
    long ticket = ++generation;
    scheduledDue = due;
    try {
      handle =
          executor.schedule(
              () -> runScheduled(ticket), Math.max(0, due - now), TimeUnit.NANOSECONDS);
      scheduleFailed = false;
    } catch (RuntimeException failure) {
      handle = null;
      scheduleFailed = true;
      log.warn("Could not schedule job state flush", failure);
    }
  }

  private void runScheduled(long ticket) {
    synchronized (this) {
      if (ticket != generation || executor == null) return;
      handle = null;
      draining = true;
    }
    try {
      flushDirty();
    } catch (Throwable failure) {
      log.warn("Job state flush failed", failure);
    } finally {
      synchronized (this) {
        draining = false;
      }
      scheduleIfNeeded();
    }
  }

  void flushDirty() {
    synchronized (drainLock) {
      drain(true);
    }
  }

  public void flushAll() {
    synchronized (drainLock) {
      drain(false);
    }
  }

  private void drain(boolean bounded) {
    long started = nanoTime.getAsLong();
    int count = 0;
    // Snapshot gives every writer at most one attempt in this drain, even on failure.
    for (var entry : new ArrayList<>(dirty.entrySet())) {
      if (bounded && (count >= 100 || nanoTime.getAsLong() - started >= 200_000_000L)) break;
      FlushableState writer = entry.getKey();
      if (bounded ? entry.getValue() - nanoTime.getAsLong() > 0 : !writer.flushOnShutdown())
        continue;
      if (!dirty.remove(writer, entry.getValue())) continue;
      count++;
      try {
        writer.flush();
      } catch (Throwable failure) {
        log.warnf(failure, "State flush failed for job %s", writer.jobId());
      } finally {
        synchronized (writer) {
          if (writer.hasUnflushed()) {
            // Replace any enqueue during I/O so this writer observes a full interval after flush.
            dirty.put(
                writer,
                nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(writer.flushIntervalMs()));
          }
        }
      }
    }
  }
}
