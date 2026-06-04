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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobDefinition;

/**
 * Portability contract: every store MUST route payload arguments and parameter values through the
 * {@link PayloadEncryption} seam on write, and reverse it on read. This catches a store that wires
 * encryption into only some of its persistence paths — the JPA stores share the row mappers, but
 * MongoDB has its own document mapper, and any future store must opt in.
 *
 * <p>The contract installs a recording AES-GCM engine and a static key provider into the static
 * {@link EncryptionHolder} (the same seam the framework's installer uses at startup, here with the
 * global switch on so every job is encrypted) and asserts both that values survive a {@code
 * save}/{@code findById} round-trip and that the engine was actually invoked on the write path. It
 * verifies write-side encryption portably, without reading raw columns: the engine records the
 * plaintext it was handed, so we can prove the sensitive data was encrypted before it reached the
 * database, and that routing metadata ({@code target}) was not.
 *
 * <p>Result and signal payloads are encrypted in the reference implementation, not the store, so
 * they are out of scope here and covered by an RI integration test instead. Crypto negative paths
 * (tamper, wrong key, relocation) are unit-tested against the encryptor in store-core.
 */
public abstract class AbstractPayloadEncryptionStoreContract implements JobStoreContractFixture {

  private RecordingEngine engine;

  @BeforeEach
  void installEngine() {
    cleanupStore();
    engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);
  }

  @AfterEach
  void clearEngine() {
    EncryptionHolder.disable();
    cleanupStore();
  }

  @Test
  void payloadArgsAndParamValues_roundTripThroughTheEncryptionSeam() {
    JobEntity job = newPendingJob();
    job.setPayload(
        new JobPayload(
            "com.example.Svc",
            "charge",
            "(Ljava/lang/String;)V",
            true,
            List.of("4111-secret-pan")));
    job.setParams(Map.of("apiKey", "super-secret-value"));

    JobEntity saved = persist(job);
    JobEntity reloaded = store().findById(saved.getId()).orElseThrow();

    // 1. Transparent round-trip: what goes in comes back out.
    assertEquals(List.of("4111-secret-pan"), reloaded.getPayload().args());
    assertEquals("super-secret-value", reloaded.getParams().get("apiKey"));

    // 2. The engine was actually invoked on the write path — proves the store routes payload args
    // and param values through the seam (not that the value merely survived a plaintext
    // round-trip).
    assertTrue(
        engine.encryptedPlaintexts.stream().anyMatch(p -> p.contains("4111-secret-pan")),
        "payload args were not handed to the engine before storage");
    assertTrue(
        engine.encryptedPlaintexts.contains("super-secret-value"),
        "param value was not handed to the engine before storage");
    assertTrue(engine.decryptCount.get() > 0, "read path did not invoke the engine");

    // 3. Routing metadata stays cleartext (never handed to the engine), so the indexed generated
    // columns derived from the payload structure keep working.
    assertEquals("com.example.Svc", reloaded.getPayload().target());
    assertFalse(
        engine.encryptedPlaintexts.contains("com.example.Svc"),
        "routing metadata must not be encrypted");
  }

  @Test
  void recurringMasterTemplate_roundTripsThroughTheEncryptionSeam() {
    UUID id = UUID.randomUUID();
    RecurringJobDefinition master =
        new RecurringJobDefinition(
            id,
            "0 * * * * ?",
            "UTC",
            Instant.now().plusSeconds(3600),
            false,
            null,
            2,
            0,
            BackoffPolicy.NONE,
            0,
            0,
            new JobPayload(
                "com.example.Svc",
                "charge",
                "(Ljava/lang/String;)V",
                true,
                List.of("4111-recurring-secret")),
            null,
            null,
            null,
            null,
            null,
            Instant.now(),
            null,
            true);

    recurringStore().createRecurring(master);
    RecurringJobDefinition reloaded = recurringStore().getRecurring(id).orElseThrow();

    // 1. Transparent round-trip: the template args survive encrypt + decrypt.
    assertEquals(List.of("4111-recurring-secret"), reloaded.payload().args());
    assertTrue(reloaded.encryptedPayload(), "recurring master should report its encryption flag");

    // 2. The engine actually saw the template plaintext on the write path — proves the recurring
    // store routes its payload template through the seam, not just the live-job path.
    assertTrue(
        engine.encryptedPlaintexts.stream().anyMatch(p -> p.contains("4111-recurring-secret")),
        "recurring template args were not handed to the engine before storage");

    // 3. Routing metadata stays cleartext so the generated target/method columns keep working.
    assertEquals("com.example.Svc", reloaded.payload().target());
  }

  /** Real AES-256-GCM engine that records every plaintext it was asked to encrypt. */
  static final class RecordingEngine implements PayloadEncryption {

    static final String ALGORITHM_ID = "TCK-AES-256-GCM";
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_BITS = 128;

    final List<String> encryptedPlaintexts = new CopyOnWriteArrayList<>();
    final AtomicInteger decryptCount = new AtomicInteger();
    private final SecureRandom random = new SecureRandom();

    @Override
    public String algorithmId() {
      return ALGORITHM_ID;
    }

    @Override
    public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
      encryptedPlaintexts.add(new String(plaintext, java.nio.charset.StandardCharsets.UTF_8));
      try {
        byte[] nonce = new byte[NONCE_LENGTH];
        random.nextBytes(nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, key(ctx), new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(ctx.additionalAuthenticatedData());
        byte[] ct = cipher.doFinal(plaintext);
        byte[] out = new byte[NONCE_LENGTH + ct.length];
        System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
        System.arraycopy(ct, 0, out, NONCE_LENGTH, ct.length);
        return out;
      } catch (GeneralSecurityException e) {
        throw new PayloadEncryptionException("encrypt failed", e);
      }
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
      decryptCount.incrementAndGet();
      try {
        byte[] nonce = Arrays.copyOfRange(ciphertext, 0, NONCE_LENGTH);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, key(ctx), new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(ctx.additionalAuthenticatedData());
        return cipher.doFinal(ciphertext, NONCE_LENGTH, ciphertext.length - NONCE_LENGTH);
      } catch (GeneralSecurityException e) {
        throw new PayloadDecryptionException("authentication failed", e);
      }
    }

    private static SecretKey key(EncryptionContext ctx) {
      return ((LocalEncryptionKey) ctx.key()).material();
    }
  }

  /** A single deterministic AES-256 key. */
  static final class SingleKeyProvider implements KeyProvider {

    private final LocalEncryptionKey key =
        new LocalEncryptionKey() {
          private final SecretKey material = deterministicKey();

          @Override
          public String keyId() {
            return "tck-key-1";
          }

          @Override
          public SecretKey material() {
            return material;
          }
        };

    @Override
    public EncryptionKey currentKey() {
      return key;
    }

    @Override
    public EncryptionKey keyById(String keyId) {
      return key;
    }

    private static SecretKey deterministicKey() {
      byte[] raw = new byte[32];
      for (int i = 0; i < raw.length; i++) {
        raw[i] = (byte) (i * 7 + 1);
      }
      return new SecretKeySpec(raw, "AES");
    }
  }
}
