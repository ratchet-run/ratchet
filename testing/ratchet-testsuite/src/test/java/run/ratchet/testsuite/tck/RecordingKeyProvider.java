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

import jakarta.enterprise.context.ApplicationScoped;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.LocalEncryptionKey;

/**
 * A {@link KeyProvider} holding a single deterministic AES-256 key, installed alongside {@link
 * RecordingPayloadEncryption} so the RI integration test runs end-to-end encryption.
 */
@ApplicationScoped
public class RecordingKeyProvider implements KeyProvider {

  private final LocalEncryptionKey key =
      new LocalEncryptionKey() {
        private final SecretKey material = deterministicKey();

        @Override
        public String keyId() {
          return "tck-it-key-1";
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
      raw[i] = (byte) (i * 5 + 2);
    }
    return new SecretKeySpec(raw, "AES");
  }
}
