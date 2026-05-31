package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import run.ratchet.api.Incubating;
import run.ratchet.api.SignalDecision;

/**
 * Fired exactly once per successful key-based signal delivery when at least one WAITING job is
 * unblocked.
 *
 * <p>Standalone event sibling of {@link JobSignaledEvent}. Bulk signal delivery can unblock many
 * jobs and does not carry a single {@code jobId} / {@code businessKey} / {@code priority}, so this
 * event does not extend {@link AbstractJobSchedulerEvent}.
 *
 * @see run.ratchet.api.JobSchedulerService#deliverSignal(String, Serializable)
 * @see run.ratchet.api.JobSchedulerService#deliverSignal(String, SignalDecision)
 */
@Incubating
public class JobsBulkSignaledEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String signalKey;
  private final int count;
  private final String signalDeliveredBy;
  private final SignalDecision.Outcome outcome;
  private final String rejectionReason;
  private final Instant signaledAt;

  public JobsBulkSignaledEvent(
      String signalKey,
      int count,
      String signalDeliveredBy,
      SignalDecision.Outcome outcome,
      String rejectionReason,
      Instant signaledAt) {
    this.signalKey = EventContract.requireNonBlank(signalKey, "signalKey");
    this.count = EventContract.requirePositive(count, "count");
    this.signalDeliveredBy = EventContract.requireNonBlank(signalDeliveredBy, "signalDeliveredBy");
    this.outcome = EventContract.requireNonNull(outcome, "outcome");
    this.rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
    if (this.outcome == SignalDecision.Outcome.APPROVED && this.rejectionReason != null) {
      throw new IllegalArgumentException("approved events cannot carry a rejection reason");
    }
    this.signaledAt = EventContract.requireNonNull(signaledAt, "signaledAt");
  }

  /** Returns the signal key that was delivered. */
  public String getSignalKey() {
    return signalKey;
  }

  /** Returns the number of WAITING jobs unblocked by this signal delivery. */
  public int getCount() {
    return count;
  }

  /** Returns the principal or system component that delivered the signal. */
  public String getSignalDeliveredBy() {
    return signalDeliveredBy;
  }

  /** Returns the approval/rejection outcome of the signal. */
  public SignalDecision.Outcome getOutcome() {
    return outcome;
  }

  /**
   * Returns the rejection reason, or {@code null} when the outcome is {@link
   * SignalDecision.Outcome#APPROVED}.
   */
  public String getRejectionReason() {
    return rejectionReason;
  }

  /** Returns the instant at which the signal was delivered. */
  public Instant getSignaledAt() {
    return signaledAt;
  }
}
