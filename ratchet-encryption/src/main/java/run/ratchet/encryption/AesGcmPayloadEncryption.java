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
 * <em>epoch</em> concatenated with a 32-bit monotonic <em>counter</em>. The epoch is drawn lazily
 * on the first {@link #encrypt}; the counter increments per encryption and a fresh epoch is drawn
 * when it would overflow (after 2^32 encryptions). Because each engine instance holds a distinct
 * epoch and the counter never repeats within one, an (epoch, counter) pair — and therefore a nonce
 * — is never reused under one key. The whole nonce is produced under a single lock so the epoch
 * draw, counter increment, and overflow redraw are one atomic step; a non-atomic construction could
 * pair a stale epoch with a reset counter and reuse a nonce, which is catastrophic for GCM.
 *
 * <p><b>Process/clone uniqueness — and its limits.</b> Two engines (two nodes) that share a key
 * must not share an epoch. Each drawn epoch mixes three sources: {@link SecureRandom}, the optional
 * per-node entropy passed to {@link #AesGcmPayloadEncryption(SecureRandom, long)}, and {@link
 * System#nanoTime()} captured at the moment of the draw. The nanoTime fold is the one component a
 * snapshot/restore cannot reproduce: because the epoch is drawn lazily on the first encrypt rather
 * than at construction, two processes resumed from the same checkpoint — sharing both {@link
 * SecureRandom} state and node identity — still draw different epochs the first time each encrypts.
 *
 * <p>This is a mitigation, not a guarantee. A checkpoint/restore of a <em>live</em> engine — one
 * that has already drawn its epoch and advanced its counter — clones the in-flight nonce state
 * verbatim, and both clones then continue the same (epoch, counter) stream under the same key. That
 * is unrecoverable nonce reuse and the operator must avoid snapshotting a running engine, or call
 * {@link #reseed()} on each restored process before it encrypts. For deployments that cannot make
 * that guarantee, {@link XChaCha20Poly1305PayloadEncryption} draws a fresh random 192-bit nonce per
 * write and carries no cross-call nonce state to clone.
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

  private boolean epochDrawn;
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
    this.epochDrawn = false;
    this.counter = 0L;
  }

  /**
   * Forces a fresh epoch on the next nonce, discarding the current (epoch, counter) state. Call
   * this on a process restored from a checkpoint, before it encrypts, so a restored engine cannot
   * resume the parent's nonce stream under the same key.
   */
  public synchronized void reseed() {
    epochDrawn = false;
    counter = 0L;
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
   * Produces the next 96-bit nonce under a single lock so the epoch draw, counter increment, and
   * overflow redraw are atomic. The epoch is drawn on first use and again before the counter would
   * exceed 32 bits, so no (epoch, counter) pair ever repeats within one engine.
   */
  private synchronized byte[] nextNonce() {
    if (!epochDrawn || counter > COUNTER_MAX) {
      epoch = drawEpoch();
      epochDrawn = true;
      counter = 0L;
    }
    long currentEpoch = epoch;
    int currentCounter = (int) counter;
    counter++;
    return ByteBuffer.allocate(NONCE_LENGTH).putLong(currentEpoch).putInt(currentCounter).array();
  }

  /**
   * Draws a fresh epoch by folding the RNG, the per-node entropy, and {@link System#nanoTime()}.
   * nanoTime is the component a checkpoint/restore cannot reproduce, so two processes resumed from
   * the same snapshot draw different epochs on their first encrypt.
   */
  private long drawEpoch() {
    return random.nextLong() ^ nodeEntropy ^ System.nanoTime();
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
