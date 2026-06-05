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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.testsupport.AesGcmTestEngine;
import run.ratchet.store.testsupport.FakeKmsKeyProvider;
import run.ratchet.store.util.EncryptionEnvelope.Frame;

/**
 * Exercises the framework's envelope-encryption (WrappedKeyProvider) write/read seam end to end.
 */
class PayloadEncryptorWrappedKeyTest {

  private static final EncryptionTarget TARGET = EncryptionTarget.signal("sig-key");

  @BeforeEach
  void install() {
    EncryptionHolder.install(
        List.of(new AesGcmTestEngine()),
        AesGcmTestEngine.ALGORITHM_ID,
        new FakeKmsKeyProvider(),
        true);
  }

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
  }

  @Test
  void wrappedProvider_roundTrips_andPersistsTheWrappedDek() {
    String stored = PayloadEncryptor.encryptValue("secret-value", true, TARGET);

    Frame frame = EncryptionEnvelope.decode(stored);
    assertTrue(frame.wrappedKey().length > 0, "the wrapped DEK must be persisted in the envelope");
    assertEquals(FakeKmsKeyProvider.MASTER_KEY_ID, frame.keyId());
    assertEquals("secret-value", PayloadEncryptor.decryptValue(stored, TARGET));
  }

  @Test
  void eachWrite_usesAFreshDek() {
    String a = PayloadEncryptor.encryptValue("same", true, TARGET);
    String b = PayloadEncryptor.encryptValue("same", true, TARGET);

    // Fresh DEK + fresh wrap per write: identical plaintext stores differently, both decrypt.
    assertNotEquals(a, b);
    assertEquals("same", PayloadEncryptor.decryptValue(a, TARGET));
    assertEquals("same", PayloadEncryptor.decryptValue(b, TARGET));
  }

  @Test
  void relocationToAnotherBinding_failsTheTag() {
    String stored = PayloadEncryptor.encryptValue("secret-value", true, TARGET);

    // The AAD binds the surface's identity (here the signal key), so decrypting under a different
    // binding must fail even though the same provider and engine are installed.
    assertThrows(
        PayloadEncryptionException.class,
        () -> PayloadEncryptor.decryptValue(stored, EncryptionTarget.signal("other-key")));
  }

  @Test
  void tamperedWrappedKeyField_failsClosed() {
    String stored = PayloadEncryptor.encryptValue("secret-value", true, TARGET);
    Frame frame = EncryptionEnvelope.decode(stored);

    byte[] corruptedWrapped = frame.wrappedKey().clone();
    corruptedWrapped[0] ^= 0x01;
    byte[] tamperedHeader =
        EncryptionEnvelope.canonicalHeader(frame.algorithmId(), frame.keyId(), corruptedWrapped);
    String tampered = EncryptionEnvelope.encode(tamperedHeader, frame.body());

    // A flipped wrapped-key byte either fails to unwrap or fails the AEAD tag — never silent.
    assertThrows(
        PayloadEncryptionException.class, () -> PayloadEncryptor.decryptValue(tampered, TARGET));
  }
}
