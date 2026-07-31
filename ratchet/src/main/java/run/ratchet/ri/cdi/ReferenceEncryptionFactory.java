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
package run.ratchet.ri.cdi;

import java.util.Optional;
import run.ratchet.spi.KeyProvider;
import run.ratchet.spi.PayloadEncryption;

/** CDI compatibility facade for the container-neutral reference encryption factory. */
final class ReferenceEncryptionFactory {

  static final String KEYS_ENV = run.ratchet.ri.core.internal.ReferenceEncryptionFactory.KEYS_ENV;
  static final String CURRENT_KEY_ENV =
      run.ratchet.ri.core.internal.ReferenceEncryptionFactory.CURRENT_KEY_ENV;
  static final String KEYS_PROPERTY =
      run.ratchet.ri.core.internal.ReferenceEncryptionFactory.KEYS_PROPERTY;
  static final String CURRENT_KEY_PROPERTY =
      run.ratchet.ri.core.internal.ReferenceEncryptionFactory.CURRENT_KEY_PROPERTY;

  record ReferenceEncryption(PayloadEncryption engine, KeyProvider keyProvider) {}

  private ReferenceEncryptionFactory() {}

  static Optional<ReferenceEncryption> fromEnvironment(long nodeEntropy) {
    return map(
        run.ratchet.ri.core.internal.ReferenceEncryptionFactory.fromEnvironment(nodeEntropy));
  }

  static Optional<ReferenceEncryption> build(
      String keysSpec, String currentKeyId, long nodeEntropy) {
    return map(
        run.ratchet.ri.core.internal.ReferenceEncryptionFactory.build(
            keysSpec, currentKeyId, nodeEntropy));
  }

  static long nodeEntropy(String nodeId) {
    return run.ratchet.ri.core.internal.ReferenceEncryptionFactory.nodeEntropy(nodeId);
  }

  private static Optional<ReferenceEncryption> map(
      Optional<run.ratchet.ri.core.internal.ReferenceEncryptionFactory.ReferenceEncryption>
          reference) {
    return reference.map(value -> new ReferenceEncryption(value.engine(), value.keyProvider()));
  }
}
