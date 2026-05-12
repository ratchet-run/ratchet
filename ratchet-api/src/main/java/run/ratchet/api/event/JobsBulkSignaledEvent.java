package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
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
    this.signalKey = Objects.requireNonNull(signalKey, "signalKey");
    this.count = count;
    this.signalDeliveredBy = signalDeliveredBy;
    this.outcome = Objects.requireNonNull(outcome, "outcome");
    this.rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
    this.signaledAt = Objects.requireNonNull(signaledAt, "signaledAt");
  }

  public String getSignalKey() {
    return signalKey;
  }

  public int getCount() {
    return count;
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

  public Instant getSignaledAt() {
    return signaledAt;
  }
}
