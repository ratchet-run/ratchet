package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a lifecycle callback ({@code onSuccess} / {@code onFailure}) throws an exception. */
public class JobCallbackFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1L;
  private final CallbackType callbackType;
  private final String errorMessage;
  private final String causeClassName;
  private final int callbackAttempt;

  /**
   * Creates an event with an explicit timestamp.
   *
   * @param callbackAttempt 1-based callback attempt number
   */
  public JobCallbackFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      CallbackType callbackType,
      String errorMessage,
      String causeClassName,
      int callbackAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.callbackType = callbackType;
    this.errorMessage = errorMessage;
    this.causeClassName = causeClassName;
    this.callbackAttempt = callbackAttempt;
  }

  /**
   * Creates an event using the current system clock instant.
   *
   * @param callbackAttempt 1-based callback attempt number
   */
  public JobCallbackFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      CallbackType callbackType,
      String errorMessage,
      String causeClassName,
      int callbackAttempt) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.callbackType = callbackType;
    this.errorMessage = errorMessage;
    this.causeClassName = causeClassName;
    this.callbackAttempt = callbackAttempt;
  }

  /** Returns which lifecycle callback failed. */
  public CallbackType getCallbackType() {
    return callbackType;
  }

  /** Returns the callback failure message. */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** Returns the class name of the thrown callback exception. */
  public String getCauseClassName() {
    return causeClassName;
  }

  /** Returns the 1-based callback attempt number. */
  public int getCallbackAttempt() {
    return callbackAttempt;
  }

  /** Identifies which callback failed — e.g. {@code ON_SUCCESS}, {@code ON_FAILURE}. */
  public enum CallbackType {
    /** The {@code onSuccess} lifecycle callback threw an exception. */
    ON_SUCCESS,

    /** The {@code onFailure} lifecycle callback threw an exception. */
    ON_FAILURE
  }
}
