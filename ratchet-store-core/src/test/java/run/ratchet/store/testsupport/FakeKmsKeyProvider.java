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
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.WrappedKeyProvider;

/**
 * A miniature in-memory KMS for tests: a single master key (KEK) that wraps a freshly generated
 * data-encryption key (DEK) per write using AES-256-GCM, exactly as an envelope-encryption {@link
 * WrappedKeyProvider} would. Exercises the framework's wrapped-key write/read seam without a real
 * key service.
 */
public final class FakeKmsKeyProvider implements WrappedKeyProvider {

  public static final String MASTER_KEY_ID = "kek-1";

  private static final int NONCE_LENGTH = 12;
  private static final int TAG_BITS = 128;

  private final SecretKey masterKey;
  private final SecureRandom random = new SecureRandom();

  public FakeKmsKeyProvider() {
    try {
      KeyGenerator gen = KeyGenerator.getInstance("AES");
      gen.init(256);
      this.masterKey = gen.generateKey();
    } catch (GeneralSecurityException e) {
      throw new PayloadEncryptionException("Could not generate fake KMS master key", e);
    }
  }

  @Override
  public WrappedKey currentWrappedKey() {
    byte[] dekBytes = new byte[32];
    random.nextBytes(dekBytes);
    byte[] wrapped = wrap(dekBytes);
    return new WrappedKey(localKey(new SecretKeySpec(dekBytes, "AES")), wrapped);
  }

  @Override
  public EncryptionKey unwrapKey(String keyId, byte[] wrappedKey) {
    if (!MASTER_KEY_ID.equals(keyId)) {
      throw new KeyNotFoundException("Unknown master key id: " + keyId);
    }
    return localKey(new SecretKeySpec(unwrap(wrappedKey), "AES"));
  }

  @Override
  public EncryptionKey currentKey() {
    throw new EncryptionConfigurationException(
        "FakeKmsKeyProvider uses envelope encryption; use currentWrappedKey()");
  }

  @Override
  public EncryptionKey keyById(String keyId) {
    throw new EncryptionConfigurationException(
        "FakeKmsKeyProvider resolves keys by unwrapping, not by id");
  }

  private LocalEncryptionKey localKey(SecretKey dek) {
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return MASTER_KEY_ID;
      }

      @Override
      public SecretKey material() {
        return dek;
      }
    };
  }

  private byte[] wrap(byte[] plaintext) {
    try {
      byte[] nonce = new byte[NONCE_LENGTH];
      random.nextBytes(nonce);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, nonce));
      byte[] ct = cipher.doFinal(plaintext);
      byte[] out = new byte[NONCE_LENGTH + ct.length];
      System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
      System.arraycopy(ct, 0, out, NONCE_LENGTH, ct.length);
      return out;
    } catch (GeneralSecurityException e) {
      throw new PayloadEncryptionException("Fake KMS wrap failed", e);
    }
  }

  private byte[] unwrap(byte[] wrapped) {
    try {
      byte[] nonce = Arrays.copyOfRange(wrapped, 0, NONCE_LENGTH);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, masterKey, new GCMParameterSpec(TAG_BITS, nonce));
      return cipher.doFinal(wrapped, NONCE_LENGTH, wrapped.length - NONCE_LENGTH);
    } catch (GeneralSecurityException e) {
      throw new KeyNotFoundException("Fake KMS unwrap failed (corrupt wrapped key)", e);
    }
  }
}
