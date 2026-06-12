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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.ProtectedSurface;

/**
 * Verifies the AES-256-GCM nonce lifecycle: the epoch is drawn lazily and folds a per-process
 * component so a cloned RNG cannot reproduce the nonce stream, while uniqueness within a single
 * engine stays intact.
 */
class AesGcmNonceLifecycleTest {

  private static final byte[] AAD = "nonce-lifecycle-aad".getBytes(StandardCharsets.UTF_8);
  private static final int NONCE_LENGTH = 12;
  private static final int EPOCH_LENGTH = 8;

  private static EncryptionContext ctx() {
    return new EncryptionContext(ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID(), key(), AAD);
  }

  private static LocalEncryptionKey key() {
    byte[] raw = new byte[32];
    new SecureRandom().nextBytes(raw);
    SecretKey material = new SecretKeySpec(raw, "AES");
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return "k";
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }

  /** A SecureRandom that returns a fixed nextLong(), simulating two clones sharing RNG state. */
  private static SecureRandom fixedRandom(long value) {
    return new SecureRandom() {
      @Override
      public long nextLong() {
        return value;
      }
    };
  }

  private static byte[] epochOf(byte[] body) {
    return Arrays.copyOfRange(body, 0, EPOCH_LENGTH);
  }

  @Test
  void clonedRngAndNodeEntropy_stillProduceDifferentEpochs() {
    // Identical SecureRandom output and identical node entropy: the only thing separating the two
    // engines is the per-process nanoTime fold drawn lazily at first encrypt.
    long sharedRngValue = 0x0123456789ABCDEFL;
    long sharedNodeEntropy = 0xCAFEBABEL;
    AesGcmPayloadEncryption a =
        new AesGcmPayloadEncryption(fixedRandom(sharedRngValue), sharedNodeEntropy);
    AesGcmPayloadEncryption b =
        new AesGcmPayloadEncryption(fixedRandom(sharedRngValue), sharedNodeEntropy);

    byte[] epochA = epochOf(a.encrypt(new byte[] {1}, ctx()));
    byte[] epochB = epochOf(b.encrypt(new byte[] {1}, ctx()));

    assertFalse(
        Arrays.equals(epochA, epochB),
        "two clones sharing RNG state and node id must not share a nonce epoch");
  }

  @Test
  void nonceIsUniqueWithinOneEngine() {
    AesGcmPayloadEncryption engine = new AesGcmPayloadEncryption();
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < 5000; i++) {
      byte[] nonce = Arrays.copyOfRange(engine.encrypt(new byte[] {2}, ctx()), 0, NONCE_LENGTH);
      assertTrue(seen.add(Arrays.toString(nonce)), "nonce repeated within a single engine");
    }
  }

  @Test
  void reseed_changesTheEpochStream() {
    AesGcmPayloadEncryption engine = new AesGcmPayloadEncryption();
    byte[] before = epochOf(engine.encrypt(new byte[] {3}, ctx()));

    engine.reseed();
    byte[] after = epochOf(engine.encrypt(new byte[] {3}, ctx()));

    assertNotEquals(
        Arrays.toString(before), Arrays.toString(after), "reseed must draw a fresh epoch");
  }
}
