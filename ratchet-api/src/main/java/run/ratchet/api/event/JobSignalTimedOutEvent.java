package run.ratchet.api.event;

import java.io.Serial;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a WAITING job's signal timeout elapses and it is transitioned to FAILED. */
public class JobSignalTimedOutEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 3876540291834670029L;

  private final String signalKey;
  private final Duration signalTimeout;

  /**
   * Creates a signal-timeout event.
   *
   * @param signalKey signal key the job was waiting on
   * @param signalTimeout configured maximum wait duration that elapsed
   */
  public JobSignalTimedOutEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String signalKey,
      Duration signalTimeout) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.signalKey = EventContract.requireNonBlank(signalKey, "signalKey");
    this.signalTimeout = EventContract.requireNonNull(signalTimeout, "signalTimeout");
  }

  /**
   * Creates a signal-timeout event using the current system clock instant.
   *
   * @param signalKey signal key the job was waiting on
   * @param signalTimeout configured maximum wait duration that elapsed
   */
  public JobSignalTimedOutEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      Duration signalTimeout) {
    this(jobId, businessKey, jobType, priority, nodeId, Instant.now(), signalKey, signalTimeout);
  }

  /** Returns the signal key the job was waiting on. */
  public String getSignalKey() {
    return signalKey;
  }

  /** Returns the configured maximum wait duration that elapsed. Never {@code null}. */
  public Duration getSignalTimeout() {
    return signalTimeout;
  }
}
