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

import run.ratchet.api.Incubating;

/**
 * An opaque handle to an encryption key, resolved and owned by a {@link KeyProvider}.
 *
 * <p>The base type deliberately exposes only an identifier, never key material. A key managed by an
 * external KMS or HSM never leaves the service — the bytes that perform the cryptography are not
 * available to the JVM at all — so the contract that every provider can satisfy is "name the key,"
 * not "hand over the key." Engines that perform AEAD in-process (the AES-GCM reference engine)
 * require the actual bytes and therefore accept only a {@link LocalEncryptionKey}; an engine that
 * delegates the transform to a remote service works from the {@link #keyId()} alone.
 *
 * <p>The framework writes {@link #keyId()} into the persisted envelope so a stored value can be
 * decrypted later under the same key, and into the indexed {@code encryption_key_id} column so the
 * key-rotation drain check is a single indexed query. The key id is not secret; projecting it leaks
 * nothing.
 *
 * <p><b>Thread-safety:</b> implementations MUST be immutable or otherwise thread-safe. A single
 * resolved key is shared across concurrent encrypt and decrypt operations.
 */
@Incubating
public interface EncryptionKey {

  /**
   * Returns the stable, opaque identifier for this key.
   *
   * <p>The identifier is recorded in the persisted envelope and the {@code encryption_key_id}
   * column, and is the argument later passed to {@link KeyProvider#keyById(String)} to resolve the
   * same key on a read. It MUST be stable for the lifetime of the key and unique within a provider.
   *
   * @return the key identifier; never {@code null} or blank
   */
  String keyId();
}
