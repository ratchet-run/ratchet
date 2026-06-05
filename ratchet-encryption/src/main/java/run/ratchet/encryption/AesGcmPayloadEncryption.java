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

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;

/**
 * The reference AES-256-GCM {@link PayloadEncryption} engine. Genuine AEAD over the framework's
 * context AAD, with a deterministic nonce so nonce uniqueness per key is structural rather than
 * probabilistic.
 *
 * <p><b>Deterministic nonce (NIST SP 800-38D §8.2.1).</b> The 96-bit nonce is a 64-bit per-instance
 * <em>epoch</em> concatenated with a 32-bit monotonic <em>counter</em>. The epoch is drawn from
 * {@link SecureRandom} when the engine is constructed; the counter increments per encryption and a
 * fresh epoch is drawn when it would overflow (after 2^32 encryptions). Because each engine
 * instance holds a distinct epoch and the counter never repeats within one, an (epoch, counter)
 * pair — and therefore a nonce — is never reused under one key. The whole nonce is produced under a
 * single lock so the epoch read, counter increment, and overflow redraw are one atomic step; a
 * non-atomic construction could pair a stale epoch with a reset counter and reuse a nonce, which is
 * catastrophic for GCM.
 *
 * <p><b>Process/clone uniqueness.</b> Two engines (two nodes) that share a key must not share an
 * epoch. The 64-bit random epoch makes an accidental collision between independently seeded
 * processes negligible, but a checkpoint/restore or forked JVM can inherit its parent's {@link
 * SecureRandom} state and redraw an identical epoch. To stay safe under cloning, the deployment
 * mixes per-node entropy into the epoch via {@link #AesGcmPayloadEncryption(SecureRandom, long)}:
 * as long as the node identity differs, the epoch differs even when the RNG state is shared.
 *
 * <p><b>Thread-safety.</b> {@link Cipher} is not thread-safe, so a fresh instance is created per
 * call; only the small nonce-state critical section is synchronized. Safe for concurrent use by
 * poller worker threads.
 */
public final class AesGcmPayloadEncryption implements PayloadEncryption {

  /** The stable algorithm id recorded in the envelope and used for read-time engine dispatch. */
  public static final String ALGORITHM_ID = "AES-256-GCM";

  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int NONCE_LENGTH = 12;
  private static final int TAG_BITS = 128;
  private static final long COUNTER_MAX = 0xFFFF_FFFFL;

  private final SecureRandom random;
  private final long nodeEntropy;

  private long epoch;
  private long counter;

  /**
   * Creates an engine with a random epoch and no per-node entropy (for tests and single-node use).
   */
  public AesGcmPayloadEncryption() {
    this(new SecureRandom(), 0L);
  }

  /**
   * Creates an engine whose nonce epoch mixes per-node entropy, so two nodes sharing a key cannot
   * collide on a nonce even if a checkpoint/restore left them with identical {@link SecureRandom}
   * state.
   *
   * @param random the source of epoch randomness; must not be {@code null}
   * @param nodeEntropy a per-node value (for example derived from the node id) mixed into every
   *     epoch; pass {@code 0} for no mixing
   */
  public AesGcmPayloadEncryption(SecureRandom random, long nodeEntropy) {
    this.random = random;
    this.nodeEntropy = nodeEntropy;
    this.epoch = random.nextLong() ^ nodeEntropy;
    this.counter = 0L;
  }

  @Override
  public String algorithmId() {
    return ALGORITHM_ID;
  }

  @Override
  public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
    byte[] nonce = nextNonce();
    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, material(ctx), new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      byte[] ct = cipher.doFinal(plaintext);
      byte[] out = new byte[NONCE_LENGTH + ct.length];
      System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
      System.arraycopy(ct, 0, out, NONCE_LENGTH, ct.length);
      return out;
    } catch (GeneralSecurityException e) {
      throw new PayloadEncryptionException("AES-256-GCM encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
    if (ciphertext.length < NONCE_LENGTH) {
      throw new PayloadDecryptionException("Ciphertext too short to carry a nonce");
    }
    try {
      byte[] nonce = Arrays.copyOfRange(ciphertext, 0, NONCE_LENGTH);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.DECRYPT_MODE, material(ctx), new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      return cipher.doFinal(ciphertext, NONCE_LENGTH, ciphertext.length - NONCE_LENGTH);
    } catch (GeneralSecurityException e) {
      // AEADBadTagException (tamper / wrong key / wrong AAD) and friends are poison, not transient.
      throw new PayloadDecryptionException("AES-256-GCM authentication failed", e);
    }
  }

  /**
   * Produces the next 96-bit nonce under a single lock so the epoch read, counter increment, and
   * overflow redraw are atomic. A fresh epoch is drawn before the counter would exceed 32 bits, so
   * no (epoch, counter) pair ever repeats.
   */
  private synchronized byte[] nextNonce() {
    if (counter > COUNTER_MAX) {
      epoch = random.nextLong() ^ nodeEntropy;
      counter = 0L;
    }
    long currentEpoch = epoch;
    int currentCounter = (int) counter;
    counter++;
    return ByteBuffer.allocate(NONCE_LENGTH).putLong(currentEpoch).putInt(currentCounter).array();
  }

  private static SecretKey material(EncryptionContext ctx) {
    if (!(ctx.key() instanceof LocalEncryptionKey local)) {
      throw new PayloadEncryptionException(
          "AES-256-GCM requires in-process key material (LocalEncryptionKey), but got "
              + ctx.key().getClass().getName());
    }
    return local.material();
  }
}
