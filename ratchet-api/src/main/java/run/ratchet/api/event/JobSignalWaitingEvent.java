package run.ratchet.api.event;

import java.io.Serial;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job has been created in WAITING state, blocked on a named signal.
 *
 * <p>{@code signalTimeout} is the maximum time the job may wait before timing out. A {@code null}
 * timeout means the job waits until a matching signal is delivered or the job is canceled.
 */
public class JobSignalWaitingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 7412309856012374810L;

  private final String signalKey;
  private final Duration signalTimeout;

  public JobSignalWaitingEvent(
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
    this.signalTimeout = signalTimeout;
  }

  public JobSignalWaitingEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      Duration signalTimeout) {
    this(jobId, businessKey, jobType, priority, nodeId, Instant.now(), signalKey, signalTimeout);
  }

  /** Returns the signal key the job is waiting on. */
  public String getSignalKey() {
    return signalKey;
  }

  /**
   * Returns the maximum time the job may wait for its signal.
   *
   * <p>A {@code null} value means the job has no signal timeout and waits until a matching signal
   * is delivered or the job is canceled.
   */
  public Duration getSignalTimeout() {
    return signalTimeout;
  }
}
