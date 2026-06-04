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
package run.ratchet.testsuite.tck;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.PayloadEncryptionException;
import run.ratchet.spi.EncryptionContext;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;

/**
 * Enabled {@code @Priority} {@link PayloadEncryption} for the RI integration test: a real
 * AES-256-GCM engine that records every plaintext it encrypts. Installed alongside {@link
 * RecordingKeyProvider}, it makes the full reference implementation run with encryption-at-rest
 * genuinely active.
 */
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class RecordingPayloadEncryption implements PayloadEncryption {

  public static final String ALGORITHM_ID = "TCK-AES-256-GCM";

  static final List<String> ENCRYPTED_PLAINTEXTS = new CopyOnWriteArrayList<>();
  static final AtomicInteger ENCRYPT_COUNT = new AtomicInteger();
  static final AtomicInteger DECRYPT_COUNT = new AtomicInteger();

  private static final int NONCE_LENGTH = 12;
  private static final int TAG_BITS = 128;

  private final SecureRandom random = new SecureRandom();

  static void reset() {
    ENCRYPTED_PLAINTEXTS.clear();
    ENCRYPT_COUNT.set(0);
    DECRYPT_COUNT.set(0);
  }

  @Override
  public String algorithmId() {
    return ALGORITHM_ID;
  }

  @Override
  public byte[] encrypt(byte[] plaintext, EncryptionContext ctx) {
    ENCRYPTED_PLAINTEXTS.add(new String(plaintext, StandardCharsets.UTF_8));
    ENCRYPT_COUNT.incrementAndGet();
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
    DECRYPT_COUNT.incrementAndGet();
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
