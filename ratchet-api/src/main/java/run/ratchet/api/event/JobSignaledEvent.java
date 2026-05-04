package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.util.UUID;

/**
 * Fired after a signal has been successfully delivered to a WAITING job, transitioning it to
 * PENDING. Published only on successful CAS — a delivery that finds the job already terminal or
 * non-WAITING does NOT produce this event.
 */
public class JobSignaledEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -2918473650123490087L;

  private final String signalKey;
  private final String signalDeliveredBy;

  public JobSignaledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      String signalDeliveredBy) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.signalKey = signalKey;
    this.signalDeliveredBy = signalDeliveredBy;
  }

  public String getSignalKey() {
    return signalKey;
  }

  public String getSignalDeliveredBy() {
    return signalDeliveredBy;
  }
}
