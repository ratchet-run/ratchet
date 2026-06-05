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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.ProtectedSurface;

/**
 * Forgery and boundary hardening for the XChaCha20-Poly1305 engine, beyond the single-bit-flip the
 * shared conformance contract performs. The hand-rolled surface is only {@link HChaCha20}; these
 * tests instead pin down the property that matters at the engine boundary — that <em>every</em>
 * byte of the stored body, including the cleartext 24-byte nonce that is not in the AAD, is covered
 * by the authentication tag, and that the engine round-trips across block boundaries and large
 * payloads.
 */
class XChaCha20Poly1305HardeningTest {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final byte[] AAD = "row:42|surface:PAYLOAD_ARGS".getBytes(UTF_8);

  private final XChaCha20Poly1305PayloadEncryption engine =
      new XChaCha20Poly1305PayloadEncryption();
  private final EncryptionKey key = randomKey();

  @Test
  void everySingleBitFlipAnywhereInTheBodyFailsAuthentication() {
    // Covers the whole body: nonce(24) ∥ ciphertext ∥ tag(16). The nonce is cleartext and not in
    // the
    // AAD, so this is the test that proves it is still authenticated — a flipped nonce bit derives
    // a
    // different subkey and inner nonce, so the tag cannot verify.
    byte[] body = engine.encrypt("forge me".getBytes(UTF_8), ctx(AAD));

    for (int byteIndex = 0; byteIndex < body.length; byteIndex++) {
      for (int bit = 0; bit < 8; bit++) {
        byte[] tampered = body.clone();
        tampered[byteIndex] ^= (byte) (1 << bit);
        assertThrows(
            PayloadDecryptionException.class,
            () -> engine.decrypt(tampered, ctx(AAD)),
            "flipping bit " + bit + " of byte " + byteIndex + " must fail authentication");
      }
    }
  }

  @Test
  void truncatedOrExtendedBodyFails() {
    byte[] body = engine.encrypt("secret".getBytes(UTF_8), ctx(AAD));

    byte[] missingTagByte = Arrays.copyOf(body, body.length - 1);
    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(missingTagByte, ctx(AAD)));

    byte[] extraByte = Arrays.copyOf(body, body.length + 1);
    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(extraByte, ctx(AAD)));

    // A body too short to even carry a nonce and a tag is rejected before the cipher is consulted.
    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(new byte[39], ctx(AAD)));
  }

  @Test
  void aadSingleBitDifferenceFails() {
    byte[] body = engine.encrypt("secret".getBytes(UTF_8), ctx(AAD));

    for (int byteIndex = 0; byteIndex < AAD.length; byteIndex++) {
      byte[] otherAad = AAD.clone();
      otherAad[byteIndex] ^= 0x01;
      assertThrows(
          PayloadDecryptionException.class,
          () -> engine.decrypt(body, ctx(otherAad)),
          "a one-bit AAD difference at byte " + byteIndex + " must fail authentication");
    }
  }

  @Test
  void swappingTheNonceBetweenTwoBodiesFails() {
    byte[] a = engine.encrypt("alpha".getBytes(UTF_8), ctx(AAD));
    byte[] b = engine.encrypt("bravo".getBytes(UTF_8), ctx(AAD));

    // Graft a's nonce onto b's ciphertext+tag: the tag was computed under b's subkey, so it fails.
    byte[] spliced = b.clone();
    System.arraycopy(a, 0, spliced, 0, 24);
    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(spliced, ctx(AAD)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 15, 63, 64, 65, 127, 128, 129, 1024, 65_535, 102_400})
  void roundTripsAcrossBlockBoundariesAndLargePayloads(int length) {
    byte[] plaintext = new byte[length];
    RANDOM.nextBytes(plaintext);

    byte[] body = engine.encrypt(plaintext, ctx(AAD));
    assertArrayEquals(plaintext, engine.decrypt(body, ctx(AAD)));

    // And tampering still fails at every size.
    byte[] tampered = body.clone();
    tampered[tampered.length - 1] ^= 0x01;
    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(tampered, ctx(AAD)));
  }

  private EncryptionContext ctx(byte[] aad) {
    return new EncryptionContext(ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID(), key, aad);
  }

  private static EncryptionKey randomKey() {
    byte[] raw = new byte[32];
    RANDOM.nextBytes(raw);
    SecretKey material = new SecretKeySpec(raw, "ChaCha20");
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return "hardening-key";
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }
}
