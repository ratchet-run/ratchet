package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import org.junit.jupiter.api.Test;

class SignalDecisionTest {

  @Test
  void approvedDecisionExposesOutcomeHelpersAndPayload() {
    SignalDecision decision = SignalDecision.approved("payload");

    assertTrue(decision.isApproved());
    assertFalse(decision.isRejected());
    assertEquals(SignalDecision.Outcome.APPROVED, decision.outcome());
    assertEquals("payload", decision.payload(String.class));
    assertNull(decision.rejectionReason());
  }

  @Test
  void approvedDecisionAllowsNullPayload() {
    SignalDecision decision = SignalDecision.approved(null);

    assertTrue(decision.isApproved());
    assertNull(decision.payload(String.class));
  }

  @Test
  void approvedDecisionRejectsRejectionReason() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SignalDecision(SignalDecision.Outcome.APPROVED, "payload", "not allowed"));
  }

  @Test
  void rejectedDecisionTrimsReasonAndExposesOutcomeHelpers() {
    SignalDecision decision = SignalDecision.rejected("payload", " denied ");

    assertFalse(decision.isApproved());
    assertTrue(decision.isRejected());
    assertEquals(SignalDecision.Outcome.REJECTED, decision.outcome());
    assertEquals("payload", decision.payload(String.class));
    assertEquals("denied", decision.rejectionReason());
  }

  @Test
  void blankRejectionReasonNormalizesToNull() {
    SignalDecision decision = SignalDecision.rejected(null, " ");

    assertNull(decision.rejectionReason());
  }

  @Test
  void payloadRecordComponentIsSerializableTyped() {
    assertEquals(Serializable.class, SignalDecision.class.getRecordComponents()[1].getType());
  }
}
