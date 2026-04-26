package run.ratchet.tck.api;

import run.ratchet.api.JobHandle;
import java.time.Duration;
import java.util.List;

/**
 * Observation surface for TCK contracts. Translates scheduler events into {@link ProbeEvent}s keyed
 * by {@link JobHandle}.
 *
 * <p>The probe is observation-only: it does not expose any store types, RI internals, or the
 * scheduler's internal event hierarchy. Contracts assert lifecycle behaviour through this interface
 * alone, which keeps {@code ratchet-tck-api} free of {@code ratchet-store-core} or
 * implementation-specific dependencies.
 *
 * <p><b>Handle scoping.</b> All signals are keyed on the {@link JobHandle} returned from a job
 * submission. Late events arriving after {@link RatchetTckRuntime#clear()} from a prior test's job
 * MUST be dropped by the implementation rather than enqueued against a fresh handle (see rule 6 of
 * the {@code clear()} contract).
 */
public interface RatchetTckProbe {

  /**
   * Registers a handle for observation. Tests MUST call this immediately after every job submission
   * so the probe knows the handle is in-scope for the current test. The contract:
   *
   * <ul>
   *   <li>Events that arrive for {@code handle.id()} <em>after</em> {@code track} is called MUST be
   *       recorded against {@code handle}.
   *   <li>Implementations MAY buffer events that arrive for {@code handle.id()} <em>before</em>
   *       {@code track} is called, to cover the (small) window between {@code submit()} returning
   *       and the test invoking {@code track}; on {@code track}, those buffered events are promoted
   *       into the per-handle event list. Implementations choosing not to buffer MUST guarantee
   *       that {@code submit()} is synchronous with respect to the first observable lifecycle event
   *       (which is generally true for poll-driven schedulers).
   *   <li>Events for IDs that are not (and were never) tracked MUST be dropped — see {@link
   *       RatchetTckRuntime#clear() clear() rule 6} for why.
   * </ul>
   *
   * <p>Calling {@code track} repeatedly for the same handle is a no-op.
   */
  void track(JobHandle handle);

  /**
   * Blocks until the probe observes a {@link ProbeEvent.Type#STARTED} for the given handle, or
   * until {@code timeout} elapses.
   *
   * @return {@code true} if STARTED was observed within the timeout, {@code false} otherwise
   */
  boolean awaitExecuted(JobHandle handle, Duration timeout);

  /**
   * Blocks until the probe observes a {@link ProbeEvent.Type#COMPLETED} for the given handle, or
   * until {@code timeout} elapses.
   */
  boolean awaitCompleted(JobHandle handle, Duration timeout);

  /**
   * Blocks until the probe observes a {@link ProbeEvent.Type#FAILED} for the given handle, or until
   * {@code timeout} elapses.
   */
  boolean awaitFailed(JobHandle handle, Duration timeout);

  /**
   * Blocks until the probe observes a {@link ProbeEvent.Type#CANCELLED} for the given handle, or
   * until {@code timeout} elapses.
   */
  boolean awaitCancelled(JobHandle handle, Duration timeout);

  /**
   * Returns the number of times the task body has been invoked for the given handle. This is the
   * count of {@link ProbeEvent.Type#STARTED} events for the handle, so retries increment it.
   */
  int invocationCount(JobHandle handle);

  /**
   * Returns the recorded events for the given handle, in observation order. The list is effectively
   * a snapshot; callers MUST NOT assume it stays in sync with the live probe state. Returns an
   * empty list for unknown handles.
   */
  List<ProbeEvent> events(JobHandle handle);
}
