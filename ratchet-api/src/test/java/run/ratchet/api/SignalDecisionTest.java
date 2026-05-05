package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SignalDecisionTest {

  @Test
  void approvedDecisionRejectsRejectionReason() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SignalDecision(SignalDecision.Outcome.APPROVED, "payload", "not allowed"));
  }

  @Test
  void rejectedDecisionTrimsReasonAndExposesOutcomeHelpers() {
    SignalDecision decision = SignalDecision.rejected("payload", " denied ");

    assertFalse(decision.approved());
    assertTrue(decision.rejected());
    assertEquals(SignalDecision.Outcome.REJECTED, decision.outcome());
    assertEquals("payload", decision.payload(String.class));
    assertEquals("denied", decision.rejectionReason());
  }

  @Test
  void blankRejectionReasonNormalizesToNull() {
    SignalDecision decision = SignalDecision.rejected(null, " ");

    assertNull(decision.rejectionReason());
  }
}
