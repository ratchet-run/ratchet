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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.ProtectedSurface;

/**
 * Conformance contract every {@link PayloadEncryption} engine must satisfy. The store integration
 * contract proves a store <em>wires</em> the encryption seam; this proves the engine itself is a
 * sound AEAD and not, for example, a transform that silently returns garbage on an authentication
 * failure (which would let a job execute on corrupted arguments with no DLQ routing).
 *
 * <p>Subclasses supply a fresh engine instance and two distinct keys the engine accepts.
 */
public abstract class AbstractPayloadEncryptionEngineContract {

  private static final int NONCE_UNIQUENESS_SAMPLES = 1000;
  private static final byte[] AAD = "aad-bytes-for-the-contract".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OTHER_AAD =
      "a-different-aad-binding".getBytes(StandardCharsets.UTF_8);

  private PayloadEncryption engine;

  /** Returns a fresh engine instance under test. Called once per test. */
  protected abstract PayloadEncryption newEngine();

  /** Returns a key the engine accepts. */
  protected abstract EncryptionKey keyA();

  /** Returns a second, distinct key the engine accepts (for the wrong-key authentication test). */
  protected abstract EncryptionKey keyB();

  @BeforeEach
  void setUpEngine() {
    engine = newEngine();
  }

  private EncryptionContext ctx(EncryptionKey key, byte[] aad) {
    return new EncryptionContext(ProtectedSurface.PAYLOAD_ARGS, UUID.randomUUID(), key, aad);
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void algorithmId_isStableAndNonBlank() {
    assertFalse(engine.algorithmId() == null || engine.algorithmId().isBlank());
    assertTrue(engine.algorithmId().equals(engine.algorithmId()));
  }

  @Test
  void roundTrip_recoversPlaintext() {
    byte[] plaintext = bytes("the quick brown fox");
    byte[] body = engine.encrypt(plaintext, ctx(keyA(), AAD));

    byte[] recovered = engine.decrypt(body, ctx(keyA(), AAD));

    assertArrayEquals(plaintext, recovered);
  }

  @Test
  void emptyPlaintext_roundTrips() {
    byte[] body = engine.encrypt(new byte[0], ctx(keyA(), AAD));

    assertArrayEquals(new byte[0], engine.decrypt(body, ctx(keyA(), AAD)));
  }

  @Test
  void tamperedCiphertext_failsWithPayloadDecryptionException() {
    byte[] body = engine.encrypt(bytes("sensitive"), ctx(keyA(), AAD));
    body[body.length - 1] ^= 0x01; // flip a bit in the tag/ciphertext

    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(body, ctx(keyA(), AAD)));
  }

  @Test
  void mismatchedAad_failsWithPayloadDecryptionException() {
    // The decisive AEAD property: decrypting with AAD that differs from the encryption AAD MUST
    // fail, not return plausible plaintext. An engine that ignores the AAD would silently pass here
    // and let a relocated ciphertext decrypt against the wrong row.
    byte[] body = engine.encrypt(bytes("sensitive"), ctx(keyA(), AAD));

    assertThrows(
        PayloadDecryptionException.class, () -> engine.decrypt(body, ctx(keyA(), OTHER_AAD)));
  }

  @Test
  void wrongKey_failsWithPayloadDecryptionException() {
    byte[] body = engine.encrypt(bytes("sensitive"), ctx(keyA(), AAD));

    assertThrows(PayloadDecryptionException.class, () -> engine.decrypt(body, ctx(keyB(), AAD)));
  }

  @Test
  void repeatedEncryption_producesDistinctBodies() {
    // Nonce uniqueness per key: encrypting the same plaintext under the same key and AAD must never
    // reuse a nonce, so every produced body is distinct. Reuse is catastrophic for GCM.
    Set<String> bodies = new HashSet<>();
    byte[] plaintext = bytes("identical every time");
    for (int i = 0; i < NONCE_UNIQUENESS_SAMPLES; i++) {
      byte[] body = engine.encrypt(plaintext, ctx(keyA(), AAD));
      assertTrue(
          bodies.add(java.util.Base64.getEncoder().encodeToString(body)),
          "Engine reused a nonce — duplicate ciphertext body at sample " + i);
    }
  }

  @Test
  void concurrentEncryption_isThreadSafeAndNonceUnique() throws Exception {
    int threads = 8;
    int perThread = 500;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    Set<String> bodies = Collections.newSetFromMap(new ConcurrentHashMap<>());
    byte[] plaintext = bytes("concurrent payload");
    try {
      Future<?>[] futures = new Future<?>[threads];
      for (int t = 0; t < threads; t++) {
        futures[t] =
            pool.submit(
                () -> {
                  for (int i = 0; i < perThread; i++) {
                    byte[] body = engine.encrypt(plaintext, ctx(keyA(), AAD));
                    bodies.add(java.util.Base64.getEncoder().encodeToString(body));
                    assertArrayEquals(plaintext, engine.decrypt(body, ctx(keyA(), AAD)));
                  }
                });
      }
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertTrue(
        bodies.size() == threads * perThread,
        "Engine reused a nonce under concurrency: expected "
            + (threads * perThread)
            + " distinct bodies, got "
            + bodies.size());
  }
}
