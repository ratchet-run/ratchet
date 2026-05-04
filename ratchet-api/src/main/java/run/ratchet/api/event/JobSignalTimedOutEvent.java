package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Duration;
import java.util.UUID;

/** Fired when a WAITING job's signal timeout elapses and it is transitioned to FAILED. */
public class JobSignalTimedOutEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 3876540291834670029L;

  private final String signalKey;
  private final Duration signalTimeout;

  public JobSignalTimedOutEvent(
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
