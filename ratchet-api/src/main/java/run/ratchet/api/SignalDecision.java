package run.ratchet.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * Structured decision delivered to a signal-waiting job.
 *
 * <p>Approval and rejection are scheduler-visible metadata for audit, metrics, and events. They do
 * not by themselves decide job success or failure: a delivered decision still unblocks the job from
 * WAITING to PENDING, and the job body reads the decision from {@link
 * JobContext#signalPayload(Class)} and applies domain-specific behavior.
 *
 * @since 0.1
 */
@Incubating
public record SignalDecision(Outcome outcome, Serializable payload, String rejectionReason)
    implements Serializable {

  @Serial private static final long serialVersionUID = 8364271059123847041L;

  public SignalDecision {
    if (outcome == null) {
      throw new IllegalArgumentException("outcome must not be null");
    }
    if (outcome == Outcome.APPROVED && rejectionReason != null && !rejectionReason.isBlank()) {
      throw new IllegalArgumentException("approved decisions cannot include a rejection reason");
    }
    rejectionReason =
        rejectionReason == null || rejectionReason.isBlank() ? null : rejectionReason.trim();
  }

  public static SignalDecision approved(Serializable payload) {
    return new SignalDecision(Outcome.APPROVED, payload, null);
  }

  public static SignalDecision rejected(Serializable payload, String rejectionReason) {
    return new SignalDecision(Outcome.REJECTED, payload, rejectionReason);
  }

  public boolean isApproved() {
    return outcome == Outcome.APPROVED;
  }

  public boolean isRejected() {
    return outcome == Outcome.REJECTED;
  }

  public <T> T payload(Class<T> type) {
    return payload == null ? null : type.cast(payload);
  }

  public enum Outcome {
    APPROVED,
    REJECTED
  }
}
