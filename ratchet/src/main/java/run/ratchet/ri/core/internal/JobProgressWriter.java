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

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.JobProgress;
import run.ratchet.api.Nullable;
import run.ratchet.api.internal.JobProgressSink;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.util.ExtensionValidation;

/** Coalesces observational progress for one claim without transaction callbacks. */
public final class JobProgressWriter implements JobProgressSink, FlushableState {
  public static final String NAMESPACE = "ratchet.progress";
  private final UUID jobId;
  private final JobAttemptControl attempt;
  private final JobStateStore store;
  private final PayloadSerializer serializer;
  private final JobStateFlusher flusher;
  private final Clock clock;
  private final long intervalMs;
  private final Object ioLock = new Object();
  private volatile @Nullable JobProgress latest;
  private boolean dirty;
  private boolean closed;
  private @Nullable Integer knownVersion;

  public JobProgressWriter(
      UUID jobId,
      JobAttemptControl attempt,
      JobStateStore store,
      PayloadSerializer serializer,
      JobStateFlusher flusher,
      Clock clock) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.store = store;
    this.serializer = serializer;
    this.flusher = flusher;
    this.clock = clock;
    this.intervalMs = flusher.progressFlushIntervalMs();
  }

  @Override
  public UUID jobId() {
    return jobId;
  }

  @Override
  public long flushIntervalMs() {
    return intervalMs;
  }

  @Override
  public boolean flushOnShutdown() {
    return false;
  }

  @Override
  public void report(long done, long total) {
    report(done, total, clock.instant());
  }

  public synchronized void report(long done, long total, Instant reportedAt) {
    if (closed || attempt.isSuperseded()) return;
    latest = new JobProgress(done, total, reportedAt);
    if (!dirty) {
      dirty = true;
      flusher.markDirty(this);
    } else if (flusher.scheduleFailed()) {
      flusher.markDirty(this);
    }
  }

  @Override
  public void flush() {
    synchronized (ioLock) {
      JobProgress value;
      synchronized (this) {
        if (!dirty) return;
        value = latest;
        dirty = false;
      }
      try {
        String json =
            serializer.serialize(
                Map.of(
                    "done",
                    value.done(),
                    "total",
                    value.total(),
                    "reportedAt",
                    value.reportedAt().toString()),
                ExtensionValidation.MAX_STATE_BYTES);
        if (fencedWrite(json) == StateWriteResult.SUPERSEDED) {
          synchronized (this) {
            attempt.markSuperseded();
            dirty = false;
            latest = null;
            close();
          }
        }
      } catch (RuntimeException | Error failure) {
        synchronized (this) {
          dirty = true;
        }
        throw failure;
      }
    }
  }

  private StateWriteResult fencedWrite(String json) {
    for (int i = 0; i < 5; i++) {
      if (knownVersion == null) {
        var state = store.getState(jobId, NAMESPACE);
        if (state.isPresent()) knownVersion = state.get().version();
      }
      StateWriteResult result;
      if (knownVersion == null) {
        result = store.initState(jobId, NAMESPACE, json, attempt.claimSeq());
        if (result == StateWriteResult.UPDATED) knownVersion = 0;
      } else {
        result = store.updateState(jobId, NAMESPACE, json, knownVersion, attempt.claimSeq());
        if (result == StateWriteResult.UPDATED) knownVersion++;
      }
      if (result != StateWriteResult.VERSION_CONFLICT) return result;
      knownVersion = null;
    }
    throw new IllegalStateException("Progress version conflicts exhausted for job " + jobId);
  }

  @Override
  public synchronized boolean hasUnflushed() {
    return !closed && dirty;
  }

  public synchronized void close() {
    closed = true;
    flusher.remove(this);
  }
}
