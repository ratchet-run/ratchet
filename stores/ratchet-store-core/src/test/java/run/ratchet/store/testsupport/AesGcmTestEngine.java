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
package run.ratchet.store.testsupport;

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
 * A real AES-256-GCM {@link PayloadEncryption} for tests. It is genuine AEAD — using the context
 * AAD and a random 96-bit nonce — so round-trip, tamper-detection, wrong-key, and header-swap tests
 * are meaningful in a way the old reversible string cipher never allowed. This is a test fixture
 * only; the production engine (with the deterministic NIST nonce construction and the algorithm
 * registry) is a separate reference-implementation deliverable.
 */
public final class AesGcmTestEngine implements PayloadEncryption {

  public static final String ALGORITHM_ID = "TEST-AES-256-GCM";

  private static final int NONCE_LENGTH = 12;
  private static final int TAG_BITS = 128;

  private final SecureRandom random = new SecureRandom();

  @Override
  public String algorithmId() {
    return ALGORITHM_ID;
  }

  @Override
  public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
    try {
      byte[] nonce = new byte[NONCE_LENGTH];
      random.nextBytes(nonce);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, material(ctx), new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      byte[] ct = cipher.doFinal(plaintext);
      byte[] out = new byte[NONCE_LENGTH + ct.length];
      System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
      System.arraycopy(ct, 0, out, NONCE_LENGTH, ct.length);
      return out;
    } catch (GeneralSecurityException e) {
      throw new PayloadEncryptionException("AES-GCM encryption failed", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
    if (ciphertext.length < NONCE_LENGTH) {
      throw new PayloadDecryptionException("Ciphertext too short to carry a nonce");
    }
    try {
      byte[] nonce = Arrays.copyOfRange(ciphertext, 0, NONCE_LENGTH);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, material(ctx), new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(ctx.additionalAuthenticatedData());
      return cipher.doFinal(ciphertext, NONCE_LENGTH, ciphertext.length - NONCE_LENGTH);
    } catch (GeneralSecurityException e) {
      // AEADBadTagException (tamper / wrong key / wrong AAD) and friends are poison, not transient.
      throw new PayloadDecryptionException("AES-GCM authentication failed", e);
    }
  }

  private static SecretKey material(EncryptionContext ctx) {
    return ((LocalEncryptionKey) ctx.key()).material();
  }
}
