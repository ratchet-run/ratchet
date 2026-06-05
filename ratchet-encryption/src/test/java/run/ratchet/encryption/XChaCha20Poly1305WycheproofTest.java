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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.ProtectedSurface;

/**
 * Runs the full Project Wycheproof XChaCha20-Poly1305 vector set against the engine — Google's
 * adversarial conformance suite, vendored verbatim. Each {@code valid} vector must encrypt (under
 * the vector's fixed nonce) to the exact published ciphertext and tag and decrypt back to the
 * plaintext; each {@code invalid} vector (corrupt tag or ciphertext, modified AAD, wrong nonce
 * size) must be rejected as poison. This is correctness against an independent authority across
 * hundreds of cases, including edge cases a single spec vector never reaches.
 */
class XChaCha20Poly1305WycheproofTest {

  private static final String RESOURCE = "/wycheproof/xchacha20_poly1305_test.json";
  private static final HexFormat HEX = HexFormat.of();

  @TestFactory
  Stream<DynamicTest> wycheproofVectors() {
    JsonObject root = load();
    List<DynamicTest> tests = new ArrayList<>();
    for (JsonValue groupValue : root.getJsonArray("testGroups")) {
      for (JsonValue testValue : groupValue.asJsonObject().getJsonArray("tests")) {
        JsonObject test = testValue.asJsonObject();
        Vector vector =
            new Vector(
                test.getInt("tcId"),
                test.getString("comment", ""),
                HEX.parseHex(test.getString("key")),
                HEX.parseHex(test.getString("iv")),
                HEX.parseHex(test.getString("aad")),
                HEX.parseHex(test.getString("msg")),
                HEX.parseHex(test.getString("ct")),
                HEX.parseHex(test.getString("tag")),
                test.getString("result"));
        tests.add(
            DynamicTest.dynamicTest(
                "tcId " + vector.tcId + " (" + vector.result + ") " + vector.comment,
                () -> check(vector)));
      }
    }
    return tests.stream();
  }

  private record Vector(
      int tcId,
      String comment,
      byte[] key,
      byte[] iv,
      byte[] aad,
      byte[] msg,
      byte[] ct,
      byte[] tag,
      String result) {}

  private static void check(Vector vector) {
    byte[] body = concat(vector.iv, concat(vector.ct, vector.tag));
    EncryptionContext ctx = ctx(vector.key, vector.aad);

    if ("invalid".equals(vector.result)) {
      // Corrupt tag/ciphertext, modified AAD, or a non-192-bit nonce — all must fail authentication
      // rather than return plausible plaintext.
      assertThrows(
          PayloadDecryptionException.class,
          () -> new XChaCha20Poly1305PayloadEncryption().decrypt(body, ctx),
          "invalid vector must be rejected");
      return;
    }

    // Valid: encrypting under the vector's nonce must reproduce its ciphertext and tag exactly, and
    // the body must decrypt back to the message.
    XChaCha20Poly1305PayloadEncryption engine =
        new XChaCha20Poly1305PayloadEncryption(fixedNonce(vector.iv));
    assertArrayEquals(body, engine.encrypt(vector.msg, ctx), "ciphertext must match the vector");
    assertArrayEquals(vector.msg, engine.decrypt(body, ctx), "decryption must recover the message");
  }

  private static JsonObject load() {
    try (InputStream in = XChaCha20Poly1305WycheproofTest.class.getResourceAsStream(RESOURCE)) {
      assertNotNull(in, "missing Wycheproof vector resource: " + RESOURCE);
      try (JsonReader reader = Json.createReader(in)) {
        return reader.readObject();
      }
    } catch (java.io.IOException e) {
      throw new IllegalStateException("could not read " + RESOURCE, e);
    }
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
        return "wycheproof-key";
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }

  private static SecureRandomNonce fixedNonce(byte[] nonce) {
    return new SecureRandomNonce(nonce);
  }

  /** A {@link java.security.SecureRandom} that always returns a fixed nonce, for vector replay. */
  private static final class SecureRandomNonce extends java.security.SecureRandom {
    private final byte[] nonce;

    private SecureRandomNonce(byte[] nonce) {
      this.nonce = nonce;
    }

    @Override
    public void nextBytes(byte[] bytes) {
      if (bytes.length != nonce.length) {
        throw new IllegalStateException("unexpected nonce request of " + bytes.length + " bytes");
      }
      System.arraycopy(nonce, 0, bytes, 0, nonce.length);
    }
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
