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
package run.ratchet.spi;

import javax.crypto.SecretKey;
import run.ratchet.api.Incubating;

/**
 * An {@link EncryptionKey} whose key material is available in-process as a {@link SecretKey}.
 *
 * <p>This is the key type that in-JVM AEAD engines — such as the AES-256-GCM reference engine —
 * require: they need the actual bytes to run the cipher locally. Local providers (a static key, an
 * environment variable, a JCA {@code KeyStore}) return this type. External key services (AWS KMS,
 * GCP KMS, HashiCorp Vault) that never expose raw material return a plain {@link EncryptionKey}
 * instead and pair with an engine that delegates the transform to the service; keeping {@link
 * #material()} off the base type is what lets those adapters drop in without the SPI ever exporting
 * key bytes they cannot produce.
 *
 * <p><b>Key lifecycle is the provider's, not the framework's.</b> The framework never calls {@link
 * javax.security.auth.Destroyable#destroy() destroy()} on the returned {@link SecretKey}: the
 * {@link KeyProvider} owns the key's lifecycle and may legitimately cache and reuse a single {@code
 * SecretKey} instance across many operations. An implementation that backs {@link #material()} with
 * a per-call clone, or that intends a key to be zeroized after use, must manage that itself — the
 * framework treats the returned material as borrowed and read-only.
 *
 * <p><b>Thread-safety:</b> as with {@link EncryptionKey}, implementations MUST be immutable or
 * otherwise thread-safe; a resolved key is shared across concurrent operations.
 */
@Incubating
public interface LocalEncryptionKey extends EncryptionKey {

  /**
   * Returns the in-process key material for local AEAD.
   *
   * <p>The framework treats the returned key as borrowed: it does not call {@code destroy()} and
   * does not retain it beyond the operation. Implementations MAY return a cached, shared instance.
   *
   * @return the secret key used to encrypt and decrypt; never {@code null}
   */
  SecretKey material();
}
