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
package run.ratchet.ri.testsupport;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.store.converter.EncryptionHolder;

/**
 * Test helpers for exercising the RI's encryption wiring against a real AES-256-GCM engine. Install
 * with {@link #install(boolean)} / tear down with {@link EncryptionHolder#disable()}.
 */
public final class EncryptionTestKit {

  public static final String ALGORITHM_ID = "TEST-AES-256-GCM";

  private EncryptionTestKit() {}

  /** Installs a working AES-GCM engine + static key provider, with the given global switch. */
  public static void install(boolean globalEnabled) {
    EncryptionHolder.install(List.of(new AesGcmEngine()), ALGORITHM_ID, new Provider(), globalEnabled);
  }

  /** A real AES-256-GCM engine, so tamper and round-trip behave like production. */
  public static final class AesGcmEngine implements PayloadEncryption {
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
        throw new PayloadEncryptionException("encrypt failed", e);
      }
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, EncryptionContext ctx) {
      try {
        byte[] nonce = Arrays.copyOfRange(ciphertext, 0, NONCE_LENGTH);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, material(ctx), new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(ctx.additionalAuthenticatedData());
        return cipher.doFinal(ciphertext, NONCE_LENGTH, ciphertext.length - NONCE_LENGTH);
      } catch (GeneralSecurityException e) {
        throw new PayloadDecryptionException("authentication failed", e);
      }
    }

    private static SecretKey material(EncryptionContext ctx) {
      return ((LocalEncryptionKey) ctx.key()).material();
    }
  }

  /** A single deterministic AES-256 key. */
  public static final class Provider implements KeyProvider {
    private final LocalEncryptionKey key =
        new LocalEncryptionKey() {
          private final SecretKey material = deterministicKey();

          @Override
          public String keyId() {
            return "ri-test-key-1";
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
      if (key.keyId().equals(keyId)) {
        return key;
      }
      throw new KeyNotFoundException("No key for id: " + keyId);
    }

    private static SecretKey deterministicKey() {
      byte[] raw = new byte[32];
      for (int i = 0; i < raw.length; i++) {
        raw[i] = (byte) (i + 3);
      }
      return new SecretKeySpec(raw, "AES");
    }
  }
}
