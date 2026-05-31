package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadMaskingPolicy;

class PayloadMaskingPolicyTest {

  @AfterEach
  void resetHolder() {
    // Tests install a custom policy; revert so other tests see the built-in default.
    PayloadMaskingPolicyHolder.set(null);
  }

  @Test
  void defaultPolicy_masksBuiltInFields() {
    PayloadMaskingPolicy policy = new DefaultPayloadMaskingPolicy();

    assertTrue(policy.isSensitiveField("password"));
    assertTrue(policy.isSensitiveField("apiKey"));
    assertTrue(policy.isSensitiveField("refresh_token"));
    assertTrue(policy.isSensitiveField("privateKeyPem"));
    assertTrue(policy.isSensitiveField("ssn"));
    assertTrue(policy.isSensitiveField("cvv"));
    assertTrue(policy.isSensitiveField("pin"));
  }

  @Test
  void defaultPolicy_shortMarkersMatchOnWordBoundariesOnly() {
    PayloadMaskingPolicy policy = new DefaultPayloadMaskingPolicy();

    assertFalse(policy.isSensitiveField("spinner"));
    assertFalse(policy.isSensitiveField("session"));
    assertFalse(policy.isSensitiveField("username"));
    assertFalse(policy.isSensitiveField("endpoint"));
  }

  @Test
  void defaultPolicy_nullFieldIsNotSensitive() {
    assertFalse(new DefaultPayloadMaskingPolicy().isSensitiveField(null));
  }

  @Test
  void holder_returnsDefaultPolicyWhenNoneInstalled() {
    PayloadMaskingPolicyHolder.set(null);

    assertTrue(PayloadMaskingPolicyHolder.get().isSensitiveField("password"));
    assertFalse(PayloadMaskingPolicyHolder.get().isSensitiveField("username"));
  }

  @Test
  void customPolicy_viaHolder_overridesWhichFieldsMask() {
    // A policy that flags exactly "username" and nothing the default would flag.
    PayloadMaskingPolicyHolder.set(
        fieldName -> "username".equals(fieldName.toLowerCase(Locale.ROOT)));

    String masked = PayloadMasker.maskPayload("{\"username\":\"alice\",\"password\":\"hunter2\"}");

    assertTrue(masked.contains("\"username\":\"***REDACTED***\""));
    assertTrue(masked.contains("\"password\":\"hunter2\""));
  }

  @Test
  void customPolicy_isConsultedForNestedFields() {
    PayloadMaskingPolicyHolder.set(fieldName -> "secretSauce".equals(fieldName));

    String masked =
        PayloadMasker.maskPayload(
            "{\"config\":{\"secretSauce\":\"x\"},\"other\":{\"password\":\"y\"}}");

    assertTrue(masked.contains("\"secretSauce\":\"***REDACTED***\""));
    // The custom policy does not flag "password", so it passes through.
    assertTrue(masked.contains("\"password\":\"y\""));
  }

  @Test
  void afterClearingCustomPolicy_holderRevertsToDefault() {
    PayloadMaskingPolicyHolder.set(fieldName -> false);
    assertFalse(PayloadMaskingPolicyHolder.get().isSensitiveField("password"));

    PayloadMaskingPolicyHolder.set(null);
    assertEquals(true, PayloadMaskingPolicyHolder.get().isSensitiveField("password"));
  }
}
