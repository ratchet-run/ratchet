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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  void holderTokenInstall_rejectsDifferentOwnerAndPreservesPolicy() {
    Object owner = new Object();
    PayloadMaskingPolicy first = fieldName -> true;
    PayloadMaskingPolicyHolder.install(owner, first);

    assertThrows(
        IllegalStateException.class,
        () -> PayloadMaskingPolicyHolder.install(new Object(), fieldName -> false));
    assertSame(first, PayloadMaskingPolicyHolder.get());
  }

  @Test
  void holderTokenInstall_sameOwnerMayReplacePolicy() {
    Object owner = new Object();
    PayloadMaskingPolicy replacement = fieldName -> false;
    PayloadMaskingPolicyHolder.install(owner, fieldName -> true);

    PayloadMaskingPolicyHolder.install(owner, replacement);

    assertSame(replacement, PayloadMaskingPolicyHolder.get());
  }

  @Test
  void holderTokenUninstall_onlyClearsMatchingOwner() {
    Object owner = new Object();
    PayloadMaskingPolicy policy = fieldName -> false;
    PayloadMaskingPolicyHolder.install(owner, policy);

    PayloadMaskingPolicyHolder.uninstall(new Object());
    assertSame(policy, PayloadMaskingPolicyHolder.get());

    PayloadMaskingPolicyHolder.uninstall(owner);
    assertTrue(PayloadMaskingPolicyHolder.get().isSensitiveField("password"));
  }

  @Test
  void holderTokenInstall_allowsSequentialOwners() {
    Object firstOwner = new Object();
    Object secondOwner = new Object();
    PayloadMaskingPolicy replacement = fieldName -> false;

    PayloadMaskingPolicyHolder.install(firstOwner, fieldName -> true);
    PayloadMaskingPolicyHolder.uninstall(firstOwner);
    PayloadMaskingPolicyHolder.install(secondOwner, replacement);

    assertSame(replacement, PayloadMaskingPolicyHolder.get());
  }

  @Test
  void holderLegacySet_isAnonymousAndReplaceableByToken() {
    PayloadMaskingPolicy replacement = fieldName -> false;
    PayloadMaskingPolicyHolder.set(fieldName -> true);

    PayloadMaskingPolicyHolder.install(new Object(), replacement);

    assertSame(replacement, PayloadMaskingPolicyHolder.get());
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
