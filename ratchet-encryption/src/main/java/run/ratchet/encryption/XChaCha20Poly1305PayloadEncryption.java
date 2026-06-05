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

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;

/**
 * An XChaCha20-Poly1305 {@link PayloadEncryption} engine: genuine AEAD over the framework's context
 * AAD with a random 192-bit nonce per write.
 *
 * <p><b>Stateless by design.</b> XChaCha20's 192-bit nonce moves the random-nonce birthday bound to
 * roughly 2<sup>96</sup> writes per key, so a fresh {@link SecureRandom} nonce per encryption is
 * safe with no per-key write ceiling. There is no epoch, no counter, no overflow redraw, and no
 * synchronization — the AES-256-GCM engine needs all of that to keep its 96-bit nonce unique, and
 * dropping it is the entire point of this engine. The only mutable state is a thread-safe {@link
 * SecureRandom}, so the engine is thread-safe by construction and carries no clone/CRaC nonce-epoch
 * hazard (the residual assumption is simply a correctly seeded RNG, shared by all randomized
 * cryptography).
 *
 * <p><b>Pure-Java, AES-NI-independent.</b> The JDK exposes IETF ChaCha20-Poly1305 (RFC 8439, 96-bit
 * nonce) but not XChaCha20 (192-bit). The engine bridges the two with the one primitive the JDK
 * lacks — {@link HChaCha20}: it derives a 256-bit subkey from the key and the first 16 nonce bytes,
 * then runs the JDK's audited {@code ChaCha20-Poly1305} cipher under that subkey with the 96-bit
 * nonce {@code 0x00000000 ∥ nonce[16..24]}. ChaCha20 is a software cipher (no AES-NI intrinsic), so
 * the engine behaves identically on any CPU. The only hand-rolled cryptography is {@code
 * HChaCha20}, which is locked by a known-answer test against the spec; the stream cipher, the
 * Poly1305 MAC, and the constant-time tag verification stay in the JDK.
 *
 * <p><b>Body layout.</b> {@link #encrypt(byte[], EncryptionContext)} returns {@code nonce(24) ∥
 * ciphertext ∥ Poly1305 tag(16)}. The framework wraps this opaque body in its versioned envelope.
 * The 24-byte nonce travels in the clear, as in any AEAD; tampering with it changes the derived
 * subkey and inner nonce, so the Poly1305 tag fails and decryption raises {@link
 * PayloadDecryptionException} rather than returning garbage.
 */
public final class XChaCha20Poly1305PayloadEncryption implements PayloadEncryption {

  /** The stable algorithm id recorded in the envelope and used for read-time engine dispatch. */
  public static final String ALGORITHM_ID = "XChaCha20-Poly1305";

  private static final String TRANSFORMATION = "ChaCha20-Poly1305";
  private static final int NONCE_LENGTH = 24; // XChaCha20 nonce
  private static final int INNER_NONCE_LENGTH = 12; // IETF ChaCha20-Poly1305 nonce
  private static final int TAG_LENGTH = 16; // Poly1305 tag
  private static final int KEY_LENGTH = 32; // 256-bit key

  private final SecureRandom random;

  /** Creates an engine with a fresh {@link SecureRandom} nonce source. */
  public XChaCha20Poly1305PayloadEncryption() {
    this(new SecureRandom());
  }

  /**
   * Creates an engine with a caller-supplied nonce source. Production uses the no-arg constructor;
   * this exists for deterministic known-answer testing against a fixed nonce.
   *
   * @param random the source of the 24-byte nonce; must not be {@code null}
   */
  public XChaCha20Poly1305PayloadEncryption(SecureRandom random) {
    this.random = random;
  }

  @Override
  public String algorithmId() {
    return ALGORITHM_ID;
  }

  @Override
  public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
    byte[] nonce = new byte[NONCE_LENGTH];
    random.nextBytes(nonce);
    try {
      Cipher cipher = innerCipher(Cipher.ENCRYPT_MODE, ctx, nonce);
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      byte[] ct = cipher.doFinal(plaintext);
      byte[] out = new byte[NONCE_LENGTH + ct.length];
      System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
      System.arraycopy(ct, 0, out, NONCE_LENGTH, ct.length);
      return out;
    } catch (GeneralSecurityException e) {
      throw new PayloadEncryptionException("XChaCha20-Poly1305 encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
    if (ciphertext.length < NONCE_LENGTH + TAG_LENGTH) {
      throw new PayloadDecryptionException(
          "Ciphertext too short to carry a 24-byte nonce and a 16-byte tag");
    }
    try {
      byte[] nonce = Arrays.copyOfRange(ciphertext, 0, NONCE_LENGTH);
      Cipher cipher = innerCipher(Cipher.DECRYPT_MODE, ctx, nonce);
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      return cipher.doFinal(ciphertext, NONCE_LENGTH, ciphertext.length - NONCE_LENGTH);
    } catch (GeneralSecurityException e) {
      // AEADBadTagException (tamper / wrong key / wrong AAD / tampered nonce) is poison, not
      // transient. The JDK cipher performs the tag comparison in constant time.
      throw new PayloadDecryptionException("XChaCha20-Poly1305 authentication failed", e);
    }
  }

  /**
   * Builds the inner IETF ChaCha20-Poly1305 cipher for one operation: derive the subkey with {@link
   * HChaCha20} over the first 16 nonce bytes, then key the JDK AEAD with that subkey and the
   * 12-byte inner nonce ({@code 0x00000000 ∥ nonce[16..24]}). A fresh {@link Cipher} per call keeps
   * the engine thread-safe ({@code Cipher} is not).
   */
  private Cipher innerCipher(int mode, EncryptionContext ctx, byte[] nonce24)
      throws GeneralSecurityException {
    byte[] keyBytes = material(ctx);
    byte[] subkey =
        HChaCha20.subkey(keyBytes, Arrays.copyOfRange(nonce24, 0, HChaCha20.NONCE_BYTES));
    byte[] innerNonce = new byte[INNER_NONCE_LENGTH]; // leading 4 bytes stay zero
    System.arraycopy(nonce24, HChaCha20.NONCE_BYTES, innerNonce, 4, 8);
    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    cipher.init(mode, new SecretKeySpec(subkey, "ChaCha20"), new IvParameterSpec(innerNonce));
    return cipher;
  }

  /**
   * Extracts the 32 raw key bytes from the context's key. The engine reads the encoded bytes rather
   * than the {@link javax.crypto.SecretKey}'s declared algorithm, so a key a provider tagged for
   * one cipher (e.g. {@code AES}) is usable here too — the same 256-bit material backs both
   * reference engines.
   */
  private static byte[] material(EncryptionContext ctx) {
    if (!(ctx.key() instanceof LocalEncryptionKey local)) {
      throw new PayloadEncryptionException(
          "XChaCha20-Poly1305 requires in-process key material (LocalEncryptionKey), but got "
              + ctx.key().getClass().getName());
    }
    byte[] encoded = local.material().getEncoded();
    if (encoded == null || encoded.length != KEY_LENGTH) {
      throw new PayloadEncryptionException(
          "XChaCha20-Poly1305 requires a 256-bit (32-byte) key, but was "
              + (encoded == null ? "non-extractable" : encoded.length + " bytes"));
    }
    return encoded;
  }
}
