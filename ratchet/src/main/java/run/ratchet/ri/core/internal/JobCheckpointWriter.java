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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.Nullable;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.api.internal.JobCheckpoints;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.util.ExtensionValidation;

/** Serializes and fences the committed checkpoints of one attempt. */
public final class JobCheckpointWriter implements JobCheckpoints, FlushableState {
  public static final String NAMESPACE = "ratchet.checkpoint";
  private static final Logger log = Logger.getLogger(JobCheckpointWriter.class);
  private final UUID jobId;
  private final JobAttemptControl attempt;
  private final @Nullable JobStateStore stateStore;
  private final AfterCommitRegistrar registrar;
  private final PayloadSerializer serializer;
  private final JobStateFlusher flusher;
  private final long intervalMs;
  // Store calls and knownVersion use ioLock; local state uses this monitor.
  private final Object ioLock = new Object();
  private long localSeq;
  private long durableSeq;
  private @Nullable Pending pending;
  private @Nullable String durableValue;
  private @Nullable String cache;
  private @Nullable Integer knownVersion;
  private boolean loaded;
  private final CheckpointSeedSource seedSource;
  private String seedValue;
  private boolean closed;

  public JobCheckpointWriter(
      UUID jobId,
      JobAttemptControl attempt,
      @Nullable JobStateStore stateStore,
      AfterCommitRegistrar registrar,
      PayloadSerializer serializer,
      JobStateFlusher flusher) {
    this(jobId, attempt, stateStore, registrar, serializer, flusher, null);
  }

  public JobCheckpointWriter(
      UUID jobId,
      JobAttemptControl attempt,
      @Nullable JobStateStore stateStore,
      AfterCommitRegistrar registrar,
      PayloadSerializer serializer,
      JobStateFlusher flusher,
      @Nullable CheckpointSeedSource seedSource) {
    this.seedSource = seedSource;
    this.jobId = jobId;
    this.attempt = attempt;
    this.stateStore = stateStore;
    this.registrar = registrar;
    this.serializer = serializer;
    this.flusher = flusher;
    this.intervalMs = flusher.checkpointFlushIntervalMs();
  }

  public UUID jobId() {
    return jobId;
  }

  @Override
  public long flushIntervalMs() {
    return intervalMs;
  }

  @Override
  public boolean flushOnShutdown() {
    return true;
  }

  @Override
  public void write(String value) {
    requireStore();
    if (attempt.isSuperseded()) throw superseded();
    String json = serializer.serialize(Map.of("value", value), ExtensionValidation.MAX_STATE_BYTES);
    long seq;
    synchronized (this) {
      seq = ++localSeq;
    }
    switch (registrar.registerAfterCommit(
        ExecutorClassLoader.preserveContext(() -> onCommit(seq, value, json)))) {
      case NO_ACTIVE_TRANSACTION -> {
        synchronized (ioLock) {
          synchronized (this) {
            if (seq <= durableSeq) return;
          }
          if (fencedWrite(json) == StateWriteResult.SUPERSEDED) {
            synchronized (this) {
              supersede();
            }
            throw superseded();
          }
          synchronized (this) {
            recordDurable(new Pending(seq, value, json));
          }
        }
      }
      case REGISTERED -> {}
      case ROLLBACK_ONLY -> log.debugf("Checkpoint dropped for rollback-only job %s", jobId);
      case REGISTRATION_FAILED ->
          throw new IllegalStateException(
              "Checkpoint for job "
                  + jobId
                  + " could not be tied to the current transaction;"
                  + " abort the transaction rather than commit work without its checkpoint");
    }
  }

  private synchronized void onCommit(long seq, String value, String json) {
    if (closed) return;
    if (seq > Math.max(durableSeq, pending == null ? 0 : pending.seq())) {
      pending = new Pending(seq, value, json);
      cache = value;
      flusher.markDirty(this);
    }
  }

  @Override
  public Optional<String> read() {
    JobStateStore store = requireStore();
    synchronized (this) {
      if (cache != null || loaded) return Optional.ofNullable(cache);
    }
    synchronized (ioLock) {
      synchronized (this) {
        if (cache != null || loaded) return Optional.ofNullable(cache);
      }
      var state = store.getState(jobId, NAMESPACE);
      String storedValue = null;
      if (state.isPresent()) {
        knownVersion = state.get().version();
        Map<?, ?> decoded = serializer.deserialize(state.get().json(), Map.class);
        Object value = decoded == null ? null : decoded.get("value");
        if (!(value instanceof String text)) {
          throw new IllegalStateException("Invalid checkpoint for job " + jobId);
        }
        storedValue = text;
      }
      String seeded = null;
      if (state.isEmpty() && seedSource != null) {
        var document = seedSource.seed(jobId);
        if (document.isPresent()) {
          Map<?, ?> decoded = serializer.deserialize(document.get(), Map.class);
          if (decoded == null || !(decoded.get("value") instanceof String))
            throw new IllegalStateException("Invalid checkpoint seed for job " + jobId);
          seeded = (String) decoded.get("value");
        }
      }
      synchronized (this) {
        seedValue = seeded;
        durableValue = storedValue;
        if (cache == null) cache = storedValue != null ? storedValue : seedValue;
        loaded = true;
        return Optional.ofNullable(cache);
      }
    }
  }

  public void flush() {
    synchronized (ioLock) {
      Pending next;
      synchronized (this) {
        if (pending == null || pending.seq() <= durableSeq) return;
        next = pending;
      }
      StateWriteResult result = fencedWrite(next.json());
      synchronized (this) {
        if (result == StateWriteResult.SUPERSEDED) {
          supersede();
        } else {
          recordDurable(next);
        }
      }
    }
  }

  // Called with the state monitor held, after the store call has finished.
  private void recordDurable(Pending next) {
    if (next.seq() > durableSeq) {
      durableSeq = next.seq();
      durableValue = next.value();
    }
    if (pending != null && pending.seq() <= durableSeq) pending = null;
    cache = pending == null ? durableValue : pending.value();
    loaded = true;
  }

  private void supersede() {
    attempt.markSuperseded();
    pending = null;
    cache = durableValue;
    close();
  }

  private StateWriteResult fencedWrite(String json) {
    JobStateStore store = requireStore();
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
    throw new IllegalStateException("Checkpoint version conflicts exhausted for job " + jobId);
  }

  private JobStateStore requireStore() {
    if (stateStore == null)
      throw new UnsupportedOperationException(
          "Job " + jobId + " store does not support job state (JobStateStore)");
    return stateStore;
  }

  private AttemptSupersededException superseded() {
    return new AttemptSupersededException("Attempt superseded for job " + jobId);
  }

  public synchronized boolean hasUnflushed() {
    return !closed && pending != null && pending.seq() > durableSeq;
  }

  public synchronized void close() {
    closed = true;
    flusher.remove(this);
  }

  private record Pending(long seq, String value, String json) {}
}
