package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a lifecycle callback ({@code onSuccess} / {@code onFailure}) throws an exception. */
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

  public CallbackType getCallbackType() {
    return callbackType;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getCauseClassName() {
    return causeClassName;
  }

  public Integer getCallbackAttempt() {
    return callbackAttempt;
  }
}
