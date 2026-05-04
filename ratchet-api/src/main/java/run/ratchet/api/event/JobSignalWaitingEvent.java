package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Duration;
import java.util.UUID;

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

  public Duration getSignalTimeout() {
    return signalTimeout;
  }
}
