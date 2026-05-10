package run.ratchet.api.event;

import java.io.Serial;
import java.time.Duration;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a job has been created in WAITING state, blocked on a named signal. */
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
      String signalKey,
      Duration signalTimeout) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.signalKey = signalKey;
    this.signalTimeout = signalTimeout;
  }

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
