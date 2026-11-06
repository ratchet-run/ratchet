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
public final class JobCheckpointWriter implements JobCheckpoints {
  public static final String NAMESPACE = "ratchet.checkpoint";
  private static final Logger log = Logger.getLogger(JobCheckpointWriter.class);
  private final UUID jobId;
  private final JobAttemptControl attempt;
  private final @Nullable JobStateStore stateStore;
  private final AfterCommitRegistrar registrar;
  private final PayloadSerializer serializer;
  private final CheckpointFlusher flusher;
  private long localSeq;
  private long durableSeq;
  private @Nullable Pending pending;
  private @Nullable String durableValue;
  private @Nullable String cache;
  private @Nullable Integer knownVersion;
  private boolean loaded;
  private boolean closed;

  public JobCheckpointWriter(
      UUID jobId,
      JobAttemptControl attempt,
      @Nullable JobStateStore stateStore,
      AfterCommitRegistrar registrar,
      PayloadSerializer serializer,
      CheckpointFlusher flusher) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.stateStore = stateStore;
    this.registrar = registrar;
    this.serializer = serializer;
    this.flusher = flusher;
  }

  public UUID jobId() {
    return jobId;
  }

  @Override
  public synchronized void write(String value) {
    requireStore();
    if (attempt.isSuperseded()) throw superseded();
    String json = serializer.serialize(Map.of("value", value), ExtensionValidation.MAX_STATE_BYTES);
    long seq = ++localSeq;
    switch (registrar.registerAfterCommit(
        ExecutorClassLoader.preserveContext(() -> onCommit(seq, value, json)))) {
      case NO_ACTIVE_TRANSACTION -> {
        if (fencedWrite(json) == StateWriteResult.SUPERSEDED) {
          attempt.markSuperseded();
          throw superseded();
        }
        durableSeq = seq;
        durableValue = value;
        cache = value;
        loaded = true;
        if (pending != null && pending.seq() <= seq) pending = null;
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
  public synchronized Optional<String> read() {
    JobStateStore store = requireStore();
    if (cache != null) return Optional.of(cache);
    if (!loaded) {
      var state = store.getState(jobId, NAMESPACE);
      if (state.isPresent()) {
        knownVersion = state.get().version();
        Map<?, ?> decoded = serializer.deserialize(state.get().json(), Map.class);
        Object value = decoded == null ? null : decoded.get("value");
        if (!(value instanceof String text)) {
          throw new IllegalStateException("Invalid checkpoint for job " + jobId);
        }
        cache = text;
        durableValue = text;
      }
      loaded = true;
    }
    return Optional.ofNullable(cache);
  }

  public synchronized void flush() {
    if (closed || pending == null || pending.seq() <= durableSeq) return;
    Pending next = pending;
    if (fencedWrite(next.json()) == StateWriteResult.SUPERSEDED) {
      attempt.markSuperseded();
      pending = null;
      cache = durableValue;
      loaded = true;
      close();
      return;
    }
    durableSeq = next.seq();
    durableValue = next.value();
    if (pending == next) pending = null;
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
