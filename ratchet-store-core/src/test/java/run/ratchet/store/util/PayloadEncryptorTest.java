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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.testsupport.AesGcmTestEngine;
import run.ratchet.store.testsupport.StaticKeyProvider;

class PayloadEncryptorTest {

  private static final UUID JOB = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
  private final EncryptionTarget args =
      EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, JOB);
  private final EncryptionTarget params =
      EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, JOB);
  private final EncryptionTarget result = EncryptionTarget.rowBound(ProtectedSurface.RESULT, JOB);

  @BeforeEach
  void enable() {
    // Engine installed so writeEngine/keyProvider resolve; each test passes the active flag
    // explicitly (the global/per-job decision lives in EncryptionHolder.encryptionActiveFor).
    EncryptionHolder.install(
        List.of(new AesGcmTestEngine()),
        AesGcmTestEngine.ALGORITHM_ID,
        new StaticKeyProvider(),
        false);
  }

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
  }

  @Test
  void args_roundTrip_leavingRoutingMetadataCleartext() {
    String payload =
        "{\"target\":\"com.acme.Pay\",\"method\":\"run\",\"args\":[\"4111-secret-pan\"]}";

    String stored = PayloadEncryptor.encryptArgs(payload, true, args);

    assertFalse(stored.contains("4111-secret-pan"), "args value must not be cleartext at rest");
    assertTrue(stored.contains("com.acme.Pay"), "target stays cleartext for the generated column");
    assertTrue(stored.contains("\"method\":\"run\""), "method stays cleartext");
    assertEquals(payload, PayloadEncryptor.decryptArgs(stored, args));
  }

  @Test
  void paramMap_encryptsValuesNotKeys_andRoundTrips() {
    String map = "{\"card\":\"super-secret-value\",\"region\":\"us\"}";

    String stored = PayloadEncryptor.encryptParamMap(map, true, params);

    assertFalse(stored.contains("super-secret-value"));
    assertTrue(stored.contains("\"card\":"), "keys stay cleartext");
    assertTrue(stored.contains("\"region\":"));
    assertEquals(map, PayloadEncryptor.decryptParamMap(stored, params));
  }

  @Test
  void singleValue_andJsonColumn_roundTrip() {
    String token = PayloadEncryptor.encryptValue("classified", true, result);
    assertTrue(EncryptionEnvelope.isFramed(token));
    assertEquals("classified", PayloadEncryptor.decryptValue(token, result));

    String column = PayloadEncryptor.encryptJsonColumn("{\"ok\":true}", true, result);
    assertFalse(column.contains("\"ok\""));
    assertEquals("{\"ok\":true}", PayloadEncryptor.decryptJsonColumn(column, result));
  }

  @Test
  void inactive_storesPlaintext() {
    String payload = "{\"target\":\"T\",\"method\":\"m\",\"args\":[\"x\"]}";
    assertEquals(payload, PayloadEncryptor.encryptArgs(payload, false, args));
    assertEquals("plain", PayloadEncryptor.encryptValue("plain", false, result));
  }

  @Test
  void legacyPlaintext_passesThroughOnDecrypt() {
    assertEquals("{\"args\":[1]}", PayloadEncryptor.decryptArgs("{\"args\":[1]}", args));
    assertEquals("bare", PayloadEncryptor.decryptValue("bare", result));
    assertEquals("{\"x\":\"y\"}", PayloadEncryptor.decryptParamMap("{\"x\":\"y\"}", params));
    assertNull(PayloadEncryptor.decryptValue(null, result));
  }

  @Test
  void tamperedCiphertext_isPoison() {
    String token = PayloadEncryptor.encryptValue("secret", true, result);
    // Flip a character inside the base64 body so the AEAD tag (or the framing) rejects it.
    int mid = token.length() - 5;
    char c = token.charAt(mid);
    String tampered = token.substring(0, mid) + (c == 'A' ? 'B' : 'A') + token.substring(mid + 1);

    assertThrows(
        PayloadDecryptionException.class, () -> PayloadEncryptor.decryptValue(tampered, result));
  }

  @Test
  void ciphertextRelocatedToAnotherJob_failsTheAadTag() {
    // A value encrypted for one job must not decrypt under another job's binding.
    String token = PayloadEncryptor.encryptValue("secret", true, result);
    EncryptionTarget otherJob =
        EncryptionTarget.rowBound(
            ProtectedSurface.RESULT, UUID.fromString("00000000-0000-0000-0000-0000000000bb"));

    assertThrows(
        PayloadDecryptionException.class, () -> PayloadEncryptor.decryptValue(token, otherJob));
  }

  @Test
  void ciphertextRelocatedToAnotherSurface_failsTheAadTag() {
    String token = PayloadEncryptor.encryptValue("secret", true, result);
    EncryptionTarget otherSurface = EncryptionTarget.rowBound(ProtectedSurface.PARAM_VALUE, JOB);

    assertThrows(
        PayloadDecryptionException.class, () -> PayloadEncryptor.decryptValue(token, otherSurface));
  }
}
