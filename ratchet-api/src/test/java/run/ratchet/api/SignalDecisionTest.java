/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  void rejectedDecisionRequiresReason() {
    assertThrows(IllegalArgumentException.class, () -> SignalDecision.rejected(null, " "));
    assertThrows(IllegalArgumentException.class, () -> SignalDecision.rejected(null, null));
  }

  @Test
  void payloadRecordComponentIsSerializableTyped() {
    assertEquals(Serializable.class, SignalDecision.class.getRecordComponents()[1].getType());
  }
}
