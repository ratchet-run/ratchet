package run.ratchet.api.event;

import java.io.Serial;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;

/**
 * Fired after a signal has been successfully delivered to a WAITING job, transitioning it to
 * PENDING. Published only on successful CAS — a delivery that finds the job already terminal or
 * non-WAITING does NOT produce this event.
 *
 * <p>{@code signalKey} is the delivered key, {@code signalDeliveredBy} identifies the principal or
 * system component that delivered it, {@code outcome} records approval/rejection metadata, and
 * {@code rejectionReason} is present only for rejected decisions.
 */
public class JobSignaledEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -2918473650123490087L;

  private final String signalKey;
  private final String signalDeliveredBy;
  private final SignalDecision.Outcome outcome;
  private final String rejectionReason;

  /**
   * Creates an approved signal-delivered event with no rejection reason.
   *
   * <p>Equivalent to the full constructor with {@link SignalDecision.Outcome#APPROVED} and a {@code
   * null} rejection reason.
   */
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

  /**
   * Creates a signal-delivered event.
   *
   * @param signalKey signal key delivered to the waiting job
   * @param signalDeliveredBy principal or system component that delivered the signal
   * @param outcome approval/rejection outcome; must not be {@code null}
   * @param rejectionReason optional rejection reason; blank values are normalized to {@code null}
   */
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
    this.signalKey = Objects.requireNonNull(signalKey, "signalKey");
    this.signalDeliveredBy = Objects.requireNonNull(signalDeliveredBy, "signalDeliveredBy");
    this.outcome = Objects.requireNonNull(outcome, "outcome");
    this.rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
    if (this.outcome == SignalDecision.Outcome.APPROVED && this.rejectionReason != null) {
      throw new IllegalArgumentException("approved events cannot carry a rejection reason");
    }
  }

  /** Returns the signal key delivered to the waiting job. */
  public String getSignalKey() {
    return signalKey;
  }

  /** Returns the principal or system component that delivered the signal. */
  public String getSignalDeliveredBy() {
    return signalDeliveredBy;
  }

  /** Returns the approval/rejection outcome recorded for this signal. */
  public SignalDecision.Outcome getOutcome() {
    return outcome;
  }

  /** Returns the rejection reason, or {@code null} when the signal was approved. */
  public String getRejectionReason() {
    return rejectionReason;
  }
}
