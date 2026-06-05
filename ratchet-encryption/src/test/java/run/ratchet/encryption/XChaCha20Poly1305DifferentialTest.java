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
package run.ratchet.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.google.crypto.tink.subtle.XChaCha20Poly1305;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.ProtectedSurface;

/**
 * Differential test against an independent XChaCha20-Poly1305 implementation (Google Tink). The two
 * implementations share nothing but the algorithm, and Tink's wire format is the same {@code
 * nonce(24) ∥ ciphertext ∥ tag(16)} this engine produces, so each can decrypt the other's output.
 *
 * <p>The published spec vectors (Appendix A.3, Wycheproof) check the engine against fixed nonces;
 * this checks it against thousands of <em>random</em> inputs, which is the coverage a hand-rolled
 * primitive most needs. The decisive direction is the engine decrypting Tink's ciphertext: that
 * runs the hand-rolled {@link HChaCha20} on a nonce Tink chose, so a derivation that is wrong for
 * some input — but still passes the single A.1 vector — cannot recover the plaintext and the test
 * fails.
 */
class XChaCha20Poly1305DifferentialTest {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int ITERATIONS = 2000;

  @Test
  void engineAndTinkDecryptEachOtherOverRandomInputs() throws GeneralSecurityException {
    XChaCha20Poly1305PayloadEncryption engine = new XChaCha20Poly1305PayloadEncryption();

    for (int i = 0; i < ITERATIONS; i++) {
      byte[] keyBytes = randomBytes(32);
      byte[] aad = randomBytes(RANDOM.nextInt(65)); // 0..64
      byte[] plaintext = randomBytes(RANDOM.nextInt(257)); // 0..256
      XChaCha20Poly1305 tink = new XChaCha20Poly1305(keyBytes);
      EncryptionContext ctx = ctx(keyBytes, aad);

      // The engine decrypts Tink's ciphertext — HChaCha20 runs over Tink's independently chosen
      // 24-byte nonce. A wrong derivation here cannot recover the plaintext.
      byte[] tinkBody = tink.encrypt(plaintext, aad);
      assertArrayEquals(
          plaintext, engine.decrypt(tinkBody, ctx), "engine must decrypt Tink output @ " + i);

      // Tink decrypts the engine's ciphertext — the engine's subkey, inner nonce, and framing must
      // match the standard or Tink's tag check rejects it.
      byte[] engineBody = engine.encrypt(plaintext, ctx);
      assertArrayEquals(
          plaintext, tink.decrypt(engineBody, aad), "Tink must decrypt engine output @ " + i);
    }
  }

  private static byte[] randomBytes(int length) {
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  private static EncryptionContext ctx(byte[] keyBytes, byte[] aad) {
    return new EncryptionContext(
        ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID(), key(keyBytes), aad);
  }

  private static EncryptionKey key(byte[] keyBytes) {
    SecretKey material = new SecretKeySpec(keyBytes, "ChaCha20");
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return "differential-key";
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }
}
