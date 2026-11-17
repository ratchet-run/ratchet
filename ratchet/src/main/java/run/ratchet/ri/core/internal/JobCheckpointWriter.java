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
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.api.Nullable;
import run.ratchet.api.TransactionalUnit;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.api.internal.JobCheckpoints;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;
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
  private final @Nullable UnitCarrier units;
  private Mode mode = Mode.NONE;
  private boolean unitOpen;
  private boolean unknown;
  private boolean firstUseDone;

  private enum Mode {
    NONE,
    DEFERRED,
    UNITS
  }

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
    this(jobId, attempt, stateStore, registrar, serializer, flusher, seedSource, null);
  }

  public JobCheckpointWriter(
      UUID jobId,
      JobAttemptControl attempt,
      @Nullable JobStateStore stateStore,
      AfterCommitRegistrar registrar,
      PayloadSerializer serializer,
      JobStateFlusher flusher,
      @Nullable CheckpointSeedSource seedSource,
      @Nullable UnitCarrier units) {
    this.units = units;
    this.seedSource = seedSource;
    this.jobId = jobId;
    this.attempt = attempt;
    this.stateStore = stateStore;
    this.registrar = registrar;
    this.serializer = serializer;
    this.flusher = flusher;
    this.intervalMs = flusher.checkpointFlushIntervalMs();
  }

  @Override
  public String runInTransaction(TransactionalUnit unit) throws Exception {
    Objects.requireNonNull(unit, "unit");
    JobStateStore store = requireStore();
    if (units == null || units.store() == null || units.transactions() == null)
      throw new UnsupportedOperationException(
          "Job "
              + jobId
              + " store cannot save a checkpoint in the unit's transaction"
              + " (TransactionalCompletionStore)");
    if (attempt.isSuperseded()) throw superseded();
    if (units.transactions().hasActiveTransaction())
      throw new IllegalStateException(
          "Job "
              + jobId
              + " has an open transaction; units start their own; call with no transaction");
    synchronized (this) {
      checkUnitAllowed();
    }
    String start;
    Integer startVersion;
    synchronized (ioLock) {
      synchronized (this) {
        checkUnitAllowed();
        mode = Mode.UNITS;
        unitOpen = true;
      }
      try {
        boolean load;
        synchronized (this) {
          load = !loaded || unknown;
        }
        if (load) loadState(store);
        synchronized (this) {
          start = cache;
          startVersion = knownVersion;
        }
      } catch (RuntimeException | Error failure) {
        synchronized (this) {
          unitOpen = false;
        }
        throw failure;
      }
    }
    try {
      boolean firstUse;
      synchronized (this) {
        firstUse = !firstUseDone;
        firstUseDone = true;
      }
      if (firstUse && units.firstUse() != null) units.firstUse().run();
      AtomicBoolean storeCalled = new AtomicBoolean();
      AtomicBoolean committed = new AtomicBoolean();
      try {
        String next =
            units
                .transactions()
                .run(
                    () -> {
                      String result = unit.run(start);
                      Objects.requireNonNull(
                          result,
                          "TransactionalUnit for job "
                              + jobId
                              + " returned null; return an end marker instead");
                      if (attempt.isSuperseded()) throw superseded();
                      String json =
                          serializer.serialize(
                              Map.of("value", result), ExtensionValidation.MAX_STATE_BYTES);
                      Result registration =
                          registrar.registerAfterCommit(() -> committed.set(true));
                      if (registration != Result.REGISTERED) {
                        if (registration == Result.ROLLBACK_ONLY)
                          throw new IllegalStateException(
                              "Job "
                                  + jobId
                                  + " unit's transaction is marked rollback-only, for example by a"
                                  + " failed nested transactional call that the unit caught;"
                                  + " the unit rolled back");
                        throw new IllegalStateException(
                            "Job "
                                + jobId
                                + " unit's commit cannot be confirmed ("
                                + registration
                                + "); the unit rolled back");
                      }
                      storeCalled.set(true);
                      StateWriteResult outcome =
                          units
                              .store()
                              .writeClaimState(
                                  jobId, NAMESPACE, json, attempt.claimSeq(), startVersion);
                      switch (outcome) {
                        case UPDATED -> {
                          return result;
                        }
                        case SUPERSEDED -> {
                          synchronized (JobCheckpointWriter.this) {
                            supersede();
                          }
                          throw superseded();
                        }
                        case VERSION_CONFLICT ->
                            throw new IllegalStateException(
                                "Checkpoint for job "
                                    + jobId
                                    + " changed outside this attempt; the unit rolled back");
                      }
                      throw new IllegalStateException("Unknown checkpoint write result");
                    });
        if (!committed.get())
          throw new IllegalStateException(
              "Job "
                  + jobId
                  + " unit's transaction rolled back without an error; nothing was saved");
        synchronized (ioLock) {
          synchronized (this) {
            knownVersion = startVersion == null ? 0 : startVersion + 1;
            durableValue = next;
            cache = next;
            loaded = true;
            unknown = false;
          }
        }
        return next;
      } catch (Throwable failure) {
        if (storeCalled.get() && !attempt.isSuperseded()) {
          synchronized (ioLock) {
            synchronized (this) {
              unknown = true;
              loaded = false;
              cache = null;
              durableValue = null;
              knownVersion = null;
            }
          }
        }
        throw failure;
      }
    } finally {
      synchronized (this) {
        unitOpen = false;
      }
    }
  }

  private void checkUnitAllowed() {
    if (closed)
      throw new IllegalStateException("Checkpoint writer for job " + jobId + " is closed");
    if (mode == Mode.DEFERRED)
      throw new IllegalStateException("Job " + jobId + " this attempt used checkpoint()");
    if (unitOpen)
      throw new IllegalStateException("Job " + jobId + " another unit of this attempt is running");
  }

  private void checkDeferredAllowed() {
    if (mode == Mode.UNITS)
      throw new IllegalStateException(
          "Job "
              + jobId
              + " saves checkpoints with runInTransaction(); checkpoint() is not allowed in the"
              + " same attempt");
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
    boolean pickMode;
    synchronized (this) {
      checkDeferredAllowed();
      pickMode = mode == Mode.NONE;
    }
    if (pickMode) {
      synchronized (ioLock) {
        synchronized (this) {
          checkDeferredAllowed();
          mode = Mode.DEFERRED;
        }
      }
    }
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
      if (!unknown && (cache != null || loaded)) return Optional.ofNullable(cache);
    }
    synchronized (ioLock) {
      synchronized (this) {
        if (!unknown && (cache != null || loaded)) return Optional.ofNullable(cache);
      }
      return loadState(store);
    }
  }

  // Called with ioLock held.
  private Optional<String> loadState(JobStateStore store) {
    var state = store.getState(jobId, NAMESPACE);
    knownVersion = null;
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
      if (unknown || cache == null) cache = storedValue != null ? storedValue : seedValue;
      unknown = false;
      loaded = true;
      return Optional.ofNullable(cache);
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
