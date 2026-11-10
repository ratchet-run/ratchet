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
package run.ratchet.api;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.api.exception.CancellationRequestedException;
import run.ratchet.api.internal.CancellationIdentities;
import run.ratchet.api.internal.CheckpointBindings;
import run.ratchet.api.internal.ContinuationBindings;
import run.ratchet.api.internal.JobCheckpoints;
import run.ratchet.api.internal.JobContinuation;
import run.ratchet.api.internal.JobProgressSink;
import run.ratchet.api.internal.ProgressBindings;
import run.ratchet.spi.JobLogger;

/**
 * Thread-local context for the executing job.
 *
 * <p>This is a plain {@link ThreadLocal}. Job code that hands work to another thread, for example
 * with {@code CompletableFuture.supplyAsync(...)}, does not carry this context with it. Submissions
 * that should inherit the executing job's caller principal must be made on the job execution thread
 * while the context is bound.
 *
 * <h2>Cooperative cancellation</h2>
 *
 * <p>Check {@link #isCancellationRequested()} or {@link #throwIfCancellationRequested()} at safe
 * points between units of work. A {@link CancellationRequestedException} created after a request in
 * the same attempt produces the timeout outcome (the same retries/DLQ and
 * JobExecutionTimedOutEvent) without an interrupt. One created before the request or kept from an
 * earlier attempt is an ordinary failure. Returning normally counts as success unless continuation
 * is requested: Ratchet cannot distinguish early return from finished work. The hard interrupt at
 * the deadline remains the backstop.
 *
 * @since 0.1
 */
@Incubating
public final class JobContext {

  private static final ThreadLocal<JobContext> TL = new ThreadLocal<>();

  private static final BooleanSupplier NEVER = () -> false;

  static {
    CancellationIdentities.installContextReader(context -> context.cancellationIdentity);
    CheckpointBindings.installContextReader(context -> context.checkpoints);
    ProgressBindings.installContextReader(context -> context.progress);
    ContinuationBindings.installContextReader(context -> context.continuation);
  }

  private final @Nullable Object cancellationIdentity;
  private final @Nullable AttemptToken attemptToken;
  private final BooleanSupplier currentAttempt;
  private final @Nullable JobCheckpoints checkpoints;
  private final @Nullable JobProgressSink progress;
  private final @Nullable JobContinuation continuation;
  private final @Nullable Instant deadline;
  private final BooleanSupplier cancellationRequested;
  private final UUID jobId;
  private final JobLogger logger;
  private final Map<String, String> params;
  private final Serializable signalPayload;
  private final @Nullable String callerPrincipal;

  private JobContext(UUID jobId, JobLogger logger) {
    this(jobId, logger, Collections.emptyMap(), null, null);
  }

  private JobContext(UUID jobId, JobLogger logger, Map<String, String> params) {
    this(jobId, logger, params, null, null);
  }

  private JobContext(
      UUID jobId, JobLogger logger, Map<String, String> params, Serializable signalPayload) {
    this(jobId, logger, params, signalPayload, null);
  }

  private JobContext(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      @Nullable Serializable signalPayload,
      @Nullable String callerPrincipal) {
    this(
        jobId,
        logger,
        params,
        signalPayload,
        callerPrincipal,
        null,
        NEVER,
        null,
        null,
        NEVER,
        null,
        null);
  }

  private JobContext(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      @Nullable Serializable signalPayload,
      @Nullable String callerPrincipal,
      @Nullable Instant deadline,
      BooleanSupplier cancellationRequested,
      @Nullable Object cancellationIdentity,
      @Nullable AttemptToken attemptToken,
      BooleanSupplier currentAttempt,
      @Nullable JobCheckpoints checkpoints,
      @Nullable JobProgressSink progress) {
    if (attemptToken != null && !attemptToken.jobId().equals(jobId)) {
      throw new IllegalArgumentException("Attempt token belongs to another job");
    }
    this.checkpoints = checkpoints;
    this.continuation =
        attemptToken != null && checkpoints != null ? new JobContinuation(jobId) : null;
    this.progress = attemptToken == null ? null : progress;
    this.attemptToken = attemptToken;
    Objects.requireNonNull(currentAttempt);
    this.currentAttempt = attemptToken == null ? NEVER : currentAttempt;
    this.cancellationIdentity = cancellationIdentity;
    this.deadline = deadline;
    this.cancellationRequested = Objects.requireNonNull(cancellationRequested);
    this.jobId = jobId;
    this.logger = logger;
    this.params = params != null ? Collections.unmodifiableMap(params) : Collections.emptyMap();
    this.signalPayload = signalPayload;
    this.callerPrincipal = callerPrincipal;
  }

  /**
   * Binds a new context to the current thread. Always pair with {@link #clear()} in a finally
   * block.
   */
  public static JobContext bind(UUID jobId, JobLogger logger) {
    JobContext ctx = new JobContext(jobId, logger);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new context with parameters to the current thread. Always pair with {@link #clear()} in
   * a finally block.
   */
  public static JobContext bind(UUID jobId, JobLogger logger, Map<String, String> params) {
    JobContext ctx = new JobContext(jobId, logger, params);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new context with parameters and a pre-deserialized signal payload. Called by the job
   * executor for signal-waiting jobs; the payload is deserialized before bind so {@code JobContext}
   * carries no serializer dependency. Always pair with {@link #clear()} in a finally block.
   */
  public static JobContext bind(
      UUID jobId, JobLogger logger, Map<String, String> params, Serializable signalPayload) {
    JobContext ctx = new JobContext(jobId, logger, params, signalPayload);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a new context with parameters, the captured caller principal, and a pre-deserialized
   * signal payload. Called by the job executor for persisted jobs; {@code callerPrincipal} is the
   * immutable principal captured when the job was created, or {@code null} when none was captured.
   * Always pair with {@link #clear()} in a finally block.
   */
  public static JobContext bind(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      @Nullable String callerPrincipal,
      @Nullable Serializable signalPayload) {
    JobContext ctx = new JobContext(jobId, logger, params, signalPayload, callerPrincipal);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a context for a watched execution. The deadline is when the watchdog interrupts this
   * attempt; the supplier reads its cancellation flag, which another thread may set. Always pair
   * with {@link #clear()} in a finally block. The cancellation identity is an opaque identity of
   * this attempt supplied by the runtime. A {@link CancellationRequestedException} records it so
   * one kept from an earlier attempt is not taken as this attempt's stop. It is null outside
   * watched executions.
   */
  public static JobContext bind(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      @Nullable String callerPrincipal,
      @Nullable Serializable signalPayload,
      @Nullable Instant deadline,
      BooleanSupplier cancellationRequested,
      @Nullable Object cancellationIdentity) {
    return bind(
        jobId,
        logger,
        params,
        callerPrincipal,
        signalPayload,
        deadline,
        cancellationRequested,
        cancellationIdentity,
        null,
        NEVER,
        null,
        null);
  }

  /**
   * Binds a context with a persisted attempt token. Called by the runtime. The cancellation
   * identity remains an opaque identity for cooperative stops. Always pair with {@link #clear()} in
   * a finally block.
   */
  public static JobContext bind(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      @Nullable String callerPrincipal,
      @Nullable Serializable signalPayload,
      @Nullable Instant deadline,
      BooleanSupplier cancellationRequested,
      @Nullable Object cancellationIdentity,
      @Nullable AttemptToken attemptToken,
      BooleanSupplier currentAttempt,
      @Nullable JobCheckpoints checkpoints,
      @Nullable JobProgressSink progress) {
    JobContext ctx =
        new JobContext(
            jobId,
            logger,
            params,
            signalPayload,
            callerPrincipal,
            deadline,
            cancellationRequested,
            cancellationIdentity,
            attemptToken,
            currentAttempt,
            checkpoints,
            progress);
    TL.set(ctx);
    return ctx;
  }

  /**
   * Binds a context for unit tests of job code. The supplier decides what {@link
   * #isCurrentAttempt()} returns. The in-memory checkpoint holder ignores transactions. Always pair
   * with {@link #clear()} in a finally block.
   */
  public static JobContext bindAttempt(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      AttemptToken attemptToken,
      BooleanSupplier currentAttempt) {
    return bindAttempt(jobId, logger, params, attemptToken, currentAttempt, null);
  }

  /**
   * Binds a seeded in-memory checkpoint holder for unit tests of job code. This holder ignores
   * transactions. Always pair with {@link #clear()} in a finally block.
   */
  public static JobContext bindAttempt(
      UUID jobId,
      JobLogger logger,
      Map<String, String> params,
      AttemptToken attemptToken,
      BooleanSupplier currentAttempt,
      @Nullable String initialCheckpoint) {
    Objects.requireNonNull(attemptToken);
    return bind(
        jobId,
        logger,
        params,
        null,
        null,
        null,
        NEVER,
        null,
        attemptToken,
        currentAttempt,
        new InMemoryCheckpoints(initialCheckpoint),
        null);
  }

  /**
   * Saves a position for at-least-once execution. Work since the last checkpoint can be repeated
   * after a crash, timeout or reclaim; make it idempotent, for example using {@code
   * attemptToken().orElseThrow().sequence()} as a fence.
   *
   * <p>Inside an open transaction the value is accepted only after commit and dropped on rollback.
   * A background flush makes committed values durable; a crash can repeat work in that window. With
   * no transaction it is saved before this call returns. Call on the thread whose transaction did
   * the work. Values are limited to 1 MiB including JSON framing: store keys and positions, not
   * data. Values are encrypted at rest when payload encryption is enabled.
   *
   * @throws IllegalStateException outside an executing persisted attempt, including lifecycle
   *     callbacks
   * @throws UnsupportedOperationException if the store does not support job state; call {@link
   *     #lastCheckpoint()} before doing work to check support
   * @throws AttemptSupersededException if this attempt has been replaced; stop doing work, as its
   *     outcome writes are fenced and ignored
   * @throws IllegalArgumentException if the framed value exceeds 1 MiB
   * @throws NullPointerException if value is null
   */
  public void checkpoint(String value) {
    Objects.requireNonNull(value);
    requireCheckpoints().write(value);
  }

  /**
   * Returns the latest committed checkpoint from this execution, otherwise the value saved by an
   * earlier execution. An uncommitted or rolled-back value is never returned. See {@link
   * #checkpoint(String)} for transaction, durability, idempotency, size and encryption rules.
   *
   * @throws IllegalStateException outside an executing persisted attempt, including lifecycle
   *     callbacks
   * @throws UnsupportedOperationException if the store does not support job state; call this before
   *     doing work to surface unsupported stores
   */
  public Optional<String> lastCheckpoint() {
    return requireCheckpoints().read();
  }

  /**
   * Records the latest progress in memory. This call is cheap and does no store I/O. Progress is
   * observational, not transactional, and rollback does not undo it. Persistence is best effort, at
   * most once per interval plus once before completion. A superseded attempt, a context without a
   * runtime sink, or a store without job state silently ignores valid reports.
   *
   * @throws IllegalArgumentException if either value is negative; done may exceed total
   */
  public void progress(long done, long total) {
    if (done < 0 || total < 0)
      throw new IllegalArgumentException("Progress values must be non-negative");
    if (progress != null) progress.report(done, total);
  }

  /** Requests another execution at the next poll. See {@link #requestContinuation(Duration)}. */
  public void requestContinuation() {
    requestContinuation(Duration.ZERO);
  }

  /**
   * Requests another execution after the delay, without spending a retry attempt. This only records
   * an in-memory request: the last call wins, and it takes effect only when the payload returns
   * normally. Attempts and the circuit breaker are untouched. The return value is ignored and
   * onSuccess callbacks are not called for a continued run.
   *
   * <p>Ratchet clears the request before each call it makes, including resilience-strategy re-runs.
   * An interceptor that re-calls the method inside one call does not clear the request, so call
   * {@code requestContinuation()} only right before returning.
   *
   * <p>Return only after the work's transaction commits. Rollback, including rollback-only, drops
   * its checkpoint and the next execution repeats that work. Eligible jobs are picked up at the
   * next poll. If the returning run lands its transition before the watchdog, it counts as a
   * continuation; if the watchdog wins, normal timeout handling spends a retry attempt.
   *
   * <p>The global {@code ratchet.execution.max-continuations} budget counts the life of the job
   * across retries. The count is an upper bound: a crash, lost race or failed transition can spend
   * a continuation that never runs. A counter or transition store failure uses normal failure
   * handling and spends a retry attempt. Execution-history rows are marked SUCCEEDED only after the
   * continuation transition succeeds and may share an attempt number. The transition is timed as
   * {@code schedule_retry} in store metrics.
   *
   * @throws NullPointerException if delay is null
   * @throws IllegalArgumentException if delay is negative
   * @throws IllegalStateException outside an executing persisted attempt, including lifecycle
   *     callbacks
   * @throws UnsupportedOperationException if the store does not support job state
   */
  public void requestContinuation(Duration delay) {
    Objects.requireNonNull(delay);
    if (delay.isNegative())
      throw new IllegalArgumentException("Continuation delay must be non-negative");
    if (continuation == null) {
      throw new IllegalStateException(
          "Continuations are available only while a persisted job attempt is executing");
    }
    continuation.request(delay);
  }

  private JobCheckpoints requireCheckpoints() {
    if (checkpoints == null) {
      throw new IllegalStateException(
          "Checkpoints are available only while a persisted job attempt is executing");
    }
    return checkpoints;
  }

  private static final class InMemoryCheckpoints implements JobCheckpoints {
    private @Nullable String value;

    private InMemoryCheckpoints(@Nullable String value) {
      this.value = value;
    }

    @Override
    public synchronized void write(String value) {
      this.value = value;
    }

    @Override
    public synchronized Optional<String> read() {
      return Optional.ofNullable(value);
    }
  }

  /** Returns the token for this claim, or empty outside an executing attempt. */
  public Optional<AttemptToken> attemptToken() {
    return Optional.ofNullable(attemptToken);
  }

  /**
   * Returns true only while the store shows this job RUNNING under this token's sequence. This
   * conservative hint can be false while the store still shows RUNNING, for example after the
   * watchdog takes over. It can turn false immediately after returning true, so a check is not a
   * fence. Store {@link AttemptToken#sequence()} with your data and refuse writes carrying a lower
   * sequence.
   *
   * <p>Each call reads the store unless a local signal already answers false. On SQL stores the
   * read uses its own transaction and a second pooled connection while your transaction holds one.
   * Call at safe points outside long transactions and never in a loop. Store errors propagate and
   * never read as current. Safe from any thread holding this context.
   */
  public boolean isCurrentAttempt() {
    return attemptToken != null && currentAttempt.getAsBoolean();
  }

  /**
   * Returns when Ratchet's watchdog interrupts this attempt: its start time plus the job timeout,
   * or {@code ratchet.timeout.default-sla-seconds} when none is configured. Empty for contexts
   * outside watched executions, such as onFailure callbacks or tests. Use {@code
   * Duration.between(Instant.now(), deadline)} to compute time remaining.
   */
  public Optional<Instant> deadline() {
    return Optional.ofNullable(deadline);
  }

  /**
   * Returns true once Ratchet asks this attempt to stop. Today the execution watchdog sets the flag
   * {@code ratchet.timeout.cancellation-grace-seconds} before the deadline, or immediately before
   * interrupting when grace is zero or not shorter than the timeout. Safe from any thread and cheap
   * (no I/O); every attempt, including retries, starts cleared. Check at safe points and stop via
   * {@link #throwIfCancellationRequested()}: returning normally counts as success unless you
   * request continuation.
   */
  public boolean isCancellationRequested() {
    return cancellationRequested.getAsBoolean();
  }

  /**
   * Throws {@link CancellationRequestedException} if Ratchet has requested cancellation. The
   * exception counts as a timeout only when created after the request in the same attempt. One
   * created before the request or kept from an earlier attempt is an ordinary failure. Safe from
   * any thread holding this context.
   */
  public void throwIfCancellationRequested() {
    if (isCancellationRequested()) {
      throw CancellationRequestedException.forContext(
          "Cancellation requested for job " + jobId, this);
    }
  }

  /** Removes the context bound to the current thread. */
  public static void clear() {
    TL.remove();
  }

  /**
   * @return the context bound to the current thread
   * @throws IllegalStateException if no context is bound
   */
  public static JobContext current() {
    JobContext ctx = TL.get();
    if (ctx == null) {
      throw new IllegalStateException("No JobContext bound to current thread");
    }
    return ctx;
  }

  /**
   * @return the context bound to the current thread, or {@code null} if no context is bound
   */
  public static @Nullable JobContext currentOrNull() {
    return TL.get();
  }

  /** Returns the UUID of the executing job. */
  public UUID jobId() {
    return jobId;
  }

  /** Returns the job-scoped logger (automatically includes job ID in all entries). */
  public JobLogger logger() {
    return logger;
  }

  /**
   * Returns the caller principal captured when this job was created, or {@code null} when no
   * principal was captured.
   */
  public @Nullable String callerPrincipal() {
    return callerPrincipal;
  }

  /**
   * @return the parameter value, or null if the key does not exist
   */
  public String param(String key) {
    return params.get(key);
  }

  /**
   * @return the parameter value if present, otherwise {@code defaultValue}
   */
  public String param(String key, String defaultValue) {
    return params.getOrDefault(key, defaultValue);
  }

  /**
   * Returns an unmodifiable map of job parameters.
   *
   * @return job parameters, or an empty map if none were supplied
   */
  public Map<String, String> params() {
    return params;
  }

  /**
   * Returns the signal payload delivered to this job, cast to the requested type, or {@code null}
   * if this job was not a signal-waiting job or no payload was included with the signal.
   *
   * <p>A payload delivered via {@code deliverSignal(..., Serializable)} is observed as its
   * JSON-native form — a {@code String}, a {@link Number} (typically a {@code java.math.BigDecimal}
   * under the default JSON-B serializer), a {@code Boolean}, a {@code List}, or a {@code Map} — so
   * request that type rather than the original concrete class. A {@link SignalDecision} delivered
   * via {@code deliverSignal(..., SignalDecision)} is obtained by requesting {@code
   * SignalDecision.class}; its {@code outcome} and {@code rejectionReason} are typed as declared,
   * but its inner {@link SignalDecision#payload(Class) payload} carries the same JSON-native-only
   * limitation.
   *
   * @throws ClassCastException if the payload cannot be cast to {@code type}
   */
  @SuppressWarnings("unchecked")
  public <T extends Serializable> T signalPayload(Class<T> type) {
    return signalPayload == null ? null : type.cast(signalPayload);
  }
}
