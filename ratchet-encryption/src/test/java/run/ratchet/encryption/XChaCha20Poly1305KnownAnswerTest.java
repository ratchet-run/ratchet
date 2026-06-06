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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.ProtectedSurface;

/**
 * End-to-end known-answer test for the full XChaCha20-Poly1305 AEAD against
 * draft-irtf-cfrg-xchacha-03 Appendix A.3. The {@link HChaCha20Test} vector locks the subkey
 * derivation in isolation; this one locks the whole engine path — subkey derivation, the {@code
 * 0x00000000 ∥ nonce[16..24]} inner-nonce split, and the JDK ChaCha20-Poly1305 transform — to the
 * spec, by encrypting under the vector's fixed nonce and matching the published ciphertext and tag
 * byte for byte. It proves correctness against the standard, not merely that the engine round-trips
 * with itself.
 */
class XChaCha20Poly1305KnownAnswerTest {

  private static final HexFormat HEX = HexFormat.of();

  // draft-irtf-cfrg-xchacha-03 Appendix A.3.1.
  private static final byte[] KEY =
      HEX.parseHex("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
  private static final byte[] NONCE =
      HEX.parseHex("404142434445464748494a4b4c4d4e4f5051525354555657");
  private static final byte[] AAD = HEX.parseHex("50515253c0c1c2c3c4c5c6c7");
  private static final byte[] PLAINTEXT =
      ("Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future,"
              + " sunscreen would be it.")
          .getBytes(US_ASCII);
  private static final byte[] CIPHERTEXT =
      HEX.parseHex(
          "bd6d179d3e83d43b9576579493c0e939572a1700252bfaccbed2902c21396cbb"
              + "731c7f1b0b4aa6440bf3a82f4eda7e39ae64c6708c54c216cb96b72e1213b452"
              + "2f8c9ba40db5d945b11b69b982c1bb9e3f3fac2bc369488f76b2383565d3fff9"
              + "21f9664c97637da9768812f615c68b13b52e");
  private static final byte[] TAG = HEX.parseHex("c0875924c1c7987947deafd8780acf49");

  @Test
  void encrypt_matchesAppendixA3Vector() {
    // Guard against a stray character in the ASCII plaintext: the AEAD ciphertext length equals the
    // plaintext length, so a mismatch here would otherwise surface as a confusing KAT failure.
    assertEquals(CIPHERTEXT.length, PLAINTEXT.length, "plaintext length must match the vector");

    XChaCha20Poly1305PayloadEncryption engine =
        new XChaCha20Poly1305PayloadEncryption(fixedNonce(NONCE));

    byte[] body = engine.encrypt(PLAINTEXT, ctx());

    // Body layout: nonce(24) ∥ ciphertext ∥ tag(16).
    assertArrayEquals(NONCE, Arrays.copyOfRange(body, 0, NONCE.length), "nonce prefix");
    assertArrayEquals(
        concat(CIPHERTEXT, TAG),
        Arrays.copyOfRange(body, NONCE.length, body.length),
        "ciphertext ∥ tag must match Appendix A.3");
  }

  @Test
  void decrypt_recoversAppendixA3Plaintext() {
    byte[] body = concat(NONCE, concat(CIPHERTEXT, TAG));

    byte[] recovered = new XChaCha20Poly1305PayloadEncryption().decrypt(body, ctx());

    assertArrayEquals(PLAINTEXT, recovered);
  }

  private static EncryptionContext ctx() {
    return new EncryptionContext(ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID(), key(), AAD);
  }

  private static EncryptionKey key() {
    SecretKey material = new SecretKeySpec(KEY, "ChaCha20");
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return "a3-key";
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }

  /** A {@link SecureRandom} that always fills the requested buffer with {@code nonce}. */
  private static SecureRandom fixedNonce(byte[] nonce) {
    return new SecureRandom() {
      @Override
      public void nextBytes(byte[] bytes) {
        if (bytes.length != nonce.length) {
          throw new IllegalStateException("unexpected nonce request of " + bytes.length + " bytes");
        }
        System.arraycopy(nonce, 0, bytes, 0, nonce.length);
      }
    };
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
