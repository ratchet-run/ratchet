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
package run.ratchet.spring.boot.it.sharedtck.tck;

import java.security.SecureRandom;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import run.ratchet.encryption.XChaCha20Poly1305PayloadEncryption;
import run.ratchet.spi.EncryptionKey;
import run.ratchet.spi.LocalEncryptionKey;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.tck.api.AbstractPayloadEncryptionEngineContract;

/** Spring binding for {@link AbstractPayloadEncryptionEngineContract}. */
@SpringRatchetTck
class SpringPayloadEncryptionEngineTckTest extends AbstractPayloadEncryptionEngineContract {

  private static final SecureRandom RANDOM = new SecureRandom();

  private final EncryptionKey keyA = localKey("key-a");
  private final EncryptionKey keyB = localKey("key-b");

  @Override
  protected PayloadEncryption newEngine() {
    return new XChaCha20Poly1305PayloadEncryption();
  }

  @Override
  protected EncryptionKey keyA() {
    return keyA;
  }

  @Override
  protected EncryptionKey keyB() {
    return keyB;
  }

  private static EncryptionKey localKey(String id) {
    byte[] raw = new byte[32];
    RANDOM.nextBytes(raw);
    SecretKey material = new SecretKeySpec(raw, "ChaCha20");
    return new LocalEncryptionKey() {
      @Override
      public String keyId() {
        return id;
      }

      @Override
      public SecretKey material() {
        return material;
      }
    };
  }
}
