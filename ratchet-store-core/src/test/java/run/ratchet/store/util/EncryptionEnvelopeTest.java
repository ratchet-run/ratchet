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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.store.util.EncryptionEnvelope.Frame;

class EncryptionEnvelopeTest {

  private static final byte[] BODY = "nonce||ct||tag".getBytes(UTF_8);

  @Test
  void encode_thenDecode_roundTripsAllFields() {
    byte[] header = EncryptionEnvelope.canonicalHeader("AES-256-GCM", "key-1", new byte[0]);
    String stored = EncryptionEnvelope.encode(header, BODY);

    Frame frame = EncryptionEnvelope.decode(stored);

    assertEquals("AES-256-GCM", frame.algorithmId());
    assertEquals("key-1", frame.keyId());
    assertArrayEquals(new byte[0], frame.wrappedKey());
    assertArrayEquals(BODY, frame.body());
  }

  @Test
  void decodedCanonicalHeader_matchesTheBuilder_soAadReconstructsIdentically() {
    byte[] header = EncryptionEnvelope.canonicalHeader("AES-256-GCM", "key-1", new byte[0]);
    String stored = EncryptionEnvelope.encode(header, BODY);

    Frame frame = EncryptionEnvelope.decode(stored);

    // The header bytes recovered on read must be byte-identical to what was fed to AAD on write.
    assertArrayEquals(header, frame.canonicalHeader());
  }

  @Test
  void reservedWrappedKeyField_roundTrips() {
    byte[] wrapped = "wrapped-dek-blob".getBytes(UTF_8);
    byte[] header = EncryptionEnvelope.canonicalHeader("AES-256-GCM", "kms-key", wrapped);
    String stored = EncryptionEnvelope.encode(header, BODY);

    Frame frame = EncryptionEnvelope.decode(stored);

    assertArrayEquals(wrapped, frame.wrappedKey());
    assertArrayEquals(BODY, frame.body());
  }

  @Test
  void isFramed_recognizesMarkerOnly() {
    byte[] header = EncryptionEnvelope.canonicalHeader("A", "k", new byte[0]);
    assertTrue(EncryptionEnvelope.isFramed(EncryptionEnvelope.encode(header, BODY)));
    assertFalse(EncryptionEnvelope.isFramed("{\"args\":[1]}"));
    assertFalse(EncryptionEnvelope.isFramed(null));
  }

  @Test
  void decode_legacyPlaintext_returnsNull() {
    // Unframed values are legacy plaintext and pass through untouched (coexistence during rollout).
    assertNull(EncryptionEnvelope.decode("{\"args\":[1,2,3]}"));
    assertNull(EncryptionEnvelope.decode("rcph:2:old-spike-value"));
    assertNull(EncryptionEnvelope.decode(null));
  }

  @Test
  void decode_markerWithInvalidBase64_isPoison() {
    assertThrows(
        PayloadDecryptionException.class,
        () -> EncryptionEnvelope.decode(EncryptionEnvelope.MARKER + "not valid base64 !!!"));
  }

  @Test
  void decode_frameTruncatedInsideHeader_isPoison() {
    // Cut into the header so a declared field length overruns the buffer: keep only the version
    // byte plus two bytes of the 4-byte algorithm-id length, so reading that length underflows.
    // (Truncating only the body is NOT an envelope error — the engine's AEAD tag catches that.)
    byte[] header = EncryptionEnvelope.canonicalHeader("AES-256-GCM", "key-1", new byte[0]);
    String truncated =
        EncryptionEnvelope.MARKER
            + java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(java.util.Arrays.copyOf(header, 3));

    assertThrows(PayloadDecryptionException.class, () -> EncryptionEnvelope.decode(truncated));
  }

  @Test
  void decode_unsupportedVersion_isPoison() {
    // A frame whose first byte is not the current version: base64url of {0x09} alone.
    String wrongVersion =
        EncryptionEnvelope.MARKER
            + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[] {0x09});

    assertThrows(PayloadDecryptionException.class, () -> EncryptionEnvelope.decode(wrongVersion));
  }
}
