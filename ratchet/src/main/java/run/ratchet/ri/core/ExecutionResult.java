package run.ratchet.ri.core;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Represents the result of attempting to submit a job for execution.
 *
 * <p>This immutable record encapsulates the outcome of a job submission attempt, which can be
 * either:
 *
 * <ul>
 *   <li><b>Success:</b> The job was accepted by the executor and a {@link Future} is available to
 *       track its completion
 *   <li><b>Rejection:</b> The executor refused the task (typically due to thread pool saturation)
 *       and the {@link RejectedExecutionException} is available for logging or retry decisions
 * </ul>
 *
 * <p>Usage pattern:
 *
 * <pre>{@code
 * ExecutionResult result = executor.submit(job);
 * if (result.isRejected()) {
 *     // Handle rejection - return job to queue, log, or retry later
 *     log.warning("Job rejected: " + result.exception().getMessage());
 * } else {
 *     // Optionally wait for completion
 *     result.future().get(timeout, TimeUnit.SECONDS);
 * }
 * }</pre>
 *
 * <p>Factory methods {@link #success(Future)} and {@link #rejected(RejectedExecutionException)}
 * should be used to create instances rather than the constructor directly.
 *
 * <p>Thread Safety: This record is immutable and thread-safe.
 *
 * @param rejected {@code true} if the executor rejected the task, {@code false} if accepted
 * @param future the {@link Future} representing the running job (null if rejected)
 * @param exception the rejection exception (null if successful)
 * @see ThreadPoolManager for the executor that produces these results
 */
public record ExecutionResult(
    boolean rejected, Future<Void> future, RejectedExecutionException exception) {

  /**
   * Creates a rejected execution result indicating the job was refused.
   *
   * <p>Use this factory method when the executor rejects a job submission, typically due to thread
   * pool saturation or shutdown. The returned result will have:
   *
   * <ul>
   *   <li>{@code rejected = true}
   *   <li>{@code future = null}
   *   <li>{@code exception = the provided exception}
   * </ul>
   *
   * @param exception the {@link RejectedExecutionException} from the executor; should not be null
   *     but is not validated
   * @return a new ExecutionResult indicating the submission was rejected
   */
  public static ExecutionResult rejected(RejectedExecutionException exception) {
    return new ExecutionResult(true, null, exception);
  }

  /**
   * Creates a successful execution result indicating the job was accepted.
   *
   * <p>Use this factory method when the executor successfully accepts a job for execution. The
   * returned result will have:
   *
   * <ul>
   *   <li>{@code rejected = false}
   *   <li>{@code future = the provided Future}
   *   <li>{@code exception = null}
   * </ul>
   *
   * @param future the {@link Future} representing the running job; must not be null
   * @return a new ExecutionResult indicating successful submission
   */
  public static ExecutionResult success(Future<Void> future) {
    return new ExecutionResult(false, future, null);
  }

  /**
   * Checks whether the executor rejected the job submission.
   *
   * <p>When this returns {@code true}, the job was not started and should be returned to the queue
   * or handled according to the rejection policy. The {@link #exception()} method will return the
   * underlying cause.
   *
   * <p>When this returns {@code false}, the job was successfully submitted and {@link #future()}
   * will return a valid Future for tracking completion.
   *
   * @return {@code true} if the executor rejected the task, {@code false} if the job is now running
   *     or queued for execution
   */
  public boolean isRejected() {
    return rejected;
  }
}
