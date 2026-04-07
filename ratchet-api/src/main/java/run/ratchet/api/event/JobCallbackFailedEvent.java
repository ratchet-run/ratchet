package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Signals that a lifecycle callback (for example {@code onSuccess} or {@code onFailure}) attached
 * to a job threw an exception.
 *
 * <p>Callback failures do not fail the parent job — by design, lifecycle hooks are fire-and-log.
 * This event exists so operators and test harnesses can observe otherwise-silent breakage. Listen
 * for it via CDI {@code @Observes} or the programmatic event listener API.
 *
 * <p>The event extends {@link AbstractJobSchedulerEvent} and carries the parent job's metadata plus
 * the callback type, the exception's message, and its class name.
 */
public class JobCallbackFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1L;

  /** Identifies which callback failed — e.g. {@code ON_SUCCESS}, {@code ON_FAILURE}. */
  public enum CallbackType {
    ON_SUCCESS,
    ON_FAILURE
  }

  private final CallbackType callbackType;
  private final String errorMessage;
  private final String causeClassName;
  private final Integer callbackAttempt;

  public JobCallbackFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      CallbackType callbackType,
      String errorMessage,
      String causeClassName,
      Integer callbackAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.callbackType = callbackType;
    this.errorMessage = errorMessage;
    this.causeClassName = causeClassName;
    this.callbackAttempt = callbackAttempt;
  }

  public JobCallbackFailedEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      CallbackType callbackType,
      String errorMessage,
      String causeClassName,
      Integer callbackAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.callbackType = callbackType;
    this.errorMessage = errorMessage;
    this.causeClassName = causeClassName;
    this.callbackAttempt = callbackAttempt;
  }

  /** Returns which callback failed ({@code ON_SUCCESS} or {@code ON_FAILURE}). */
  public CallbackType getCallbackType() {
    return callbackType;
  }

  /** Returns the callback exception's message, possibly {@code null}. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the fully-qualified class name of the thrown exception. */
  public String getCauseClassName() {
    return causeClassName;
  }

  /** Returns the 1-based invocation count for the callback. */
  public Integer getCallbackAttempt() {
    return callbackAttempt;
  }
}
