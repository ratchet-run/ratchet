package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;
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
  private final SignalDecision.Outcome outcome;
  private final String rejectionReason;

  public JobSignaledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      String signalDeliveredBy) {
    this(
        jobId,
        businessKey,
        jobType,
        priority,
        nodeId,
        signalKey,
        signalDeliveredBy,
        SignalDecision.Outcome.APPROVED,
        null);
  }

  public JobSignaledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      String signalDeliveredBy,
      SignalDecision.Outcome outcome,
      String rejectionReason) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.signalKey = signalKey;
    this.signalDeliveredBy = signalDeliveredBy;
    this.outcome = outcome != null ? outcome : SignalDecision.Outcome.APPROVED;
    this.rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
  }

  public String getSignalKey() {
    return signalKey;
  }

  public String getSignalDeliveredBy() {
    return signalDeliveredBy;
  }

  public SignalDecision.Outcome getOutcome() {
    return outcome;
  }

  public String getRejectionReason() {
    return rejectionReason;
  }
}
