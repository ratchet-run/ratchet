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
package run.ratchet.ri.encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.encryption.AesGcmPayloadEncryption;
import run.ratchet.encryption.SecretKeyProvider;
import run.ratchet.encryption.XChaCha20Poly1305PayloadEncryption;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionEnvelope;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/**
 * Proves the two reference engines coexist in one registry and that reads dispatch on the algorithm
 * id recorded in the envelope, not on whichever engine currently writes. This is the rotation seam:
 * an old engine stays installed to decrypt not-yet-drained rows while a new engine takes over new
 * writes. It lives in the reference-implementation test tree because it spans store-core's framing
 * ({@link EncryptionHolder}, {@link PayloadEncryptor}) and the engines in {@code
 * ratchet-encryption} at once.
 */
class EngineRegistryCoexistenceTest {

  private static final EncryptionTarget TARGET =
      EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID());

  private final AesGcmPayloadEncryption aes = new AesGcmPayloadEncryption();
  private final XChaCha20Poly1305PayloadEncryption xchacha =
      new XChaCha20Poly1305PayloadEncryption();
  private final SecretKeyProvider provider = new SecretKeyProvider(Map.of("k1", key()), "k1");

  @AfterEach
  void reset() {
    EncryptionHolder.disable();
  }

  @Test
  void readDispatchesByAlgorithmId_evenAfterTheWriterFlips() {
    // Both engines installed; XChaCha20 writes. The shared 256-bit key backs both engines.
    EncryptionHolder.install(
        List.of(aes, xchacha), XChaCha20Poly1305PayloadEncryption.ALGORITHM_ID, provider, true);

    String writtenByXChaCha = PayloadEncryptor.encryptValue("top secret", true, TARGET);
    assertEquals(
        XChaCha20Poly1305PayloadEncryption.ALGORITHM_ID,
        EncryptionEnvelope.decode(writtenByXChaCha).algorithmId());
    assertEquals("top secret", PayloadEncryptor.decryptValue(writtenByXChaCha, TARGET));

    // Flip the writer to AES; both engines stay installed. The XChaCha20 value must still decrypt,
    // because the read path resolves the engine from the envelope's algorithm id.
    EncryptionHolder.install(
        List.of(aes, xchacha), AesGcmPayloadEncryption.ALGORITHM_ID, provider, true);
    assertEquals("top secret", PayloadEncryptor.decryptValue(writtenByXChaCha, TARGET));

    // New writes now use AES, and the two stored values name different engines.
    String writtenByAes = PayloadEncryptor.encryptValue("top secret", true, TARGET);
    assertEquals(
        AesGcmPayloadEncryption.ALGORITHM_ID,
        EncryptionEnvelope.decode(writtenByAes).algorithmId());
    assertEquals("top secret", PayloadEncryptor.decryptValue(writtenByAes, TARGET));
  }

  @Test
  void valueWhoseEngineWasRemoved_isPoison() {
    EncryptionHolder.install(
        List.of(aes, xchacha), XChaCha20Poly1305PayloadEncryption.ALGORITHM_ID, provider, true);
    String writtenByXChaCha = PayloadEncryptor.encryptValue("top secret", true, TARGET);

    // Retire the XChaCha20 engine before its rows drained: the value is now poison, not plaintext.
    EncryptionHolder.install(List.of(aes), AesGcmPayloadEncryption.ALGORITHM_ID, provider, true);
    assertThrows(
        PayloadDecryptionException.class,
        () -> PayloadEncryptor.decryptValue(writtenByXChaCha, TARGET));
  }

  private static SecretKey key() {
    byte[] raw = new byte[32];
    Arrays.fill(raw, (byte) 7);
    return new SecretKeySpec(raw, "AES");
  }
}
