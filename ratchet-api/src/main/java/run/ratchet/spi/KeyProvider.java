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
import run.ratchet.api.exception.EncryptionConfigurationException;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.KeyProviderUnavailableException;

/**
 * Owns key storage, the current (active) key, lookup by id, and the lifecycle that makes rotation
 * safe. This is the seam a deployment replaces to back encryption with a static key, an environment
 * variable, a JCA {@code KeyStore}, or an external key service (AWS KMS, GCP KMS, HashiCorp Vault).
 *
 * <p>The provider is one of the three owners in the encryption design: it never performs the AEAD
 * transform (that is {@link PayloadEncryption}) and it never decides which leaves to protect (that
 * is the framework). It answers exactly two questions — "which key do I write with now?" and "which
 * key does this id refer to?" — and is responsible for keeping old keys resolvable until every row
 * that references them has drained.
 *
 * <p><b>Failure classification is part of the contract,</b> because the framework routes on the
 * exception type. A transient outage of an external key service must be retried, not turned into a
 * lost job; a reference to a key the provider has permanently forgotten is poison data that no
 * retry can fix. Implementations MUST map their failures onto the typed exceptions below
 * accordingly.
 *
 * <p><b>Thread-safety:</b> implementations MUST be thread-safe. A single provider instance serves
 * concurrent poller worker threads on both the write (encrypt) and read (decrypt) paths.
 */
@Incubating
public interface KeyProvider {

  /**
   * Returns the key to use for new writes — the current, active key. New ciphertext is produced
   * under this key and its {@link EncryptionKey#keyId() id} is recorded in the envelope and the
   * {@code encryption_key_id} column.
   *
   * @return the current key; never {@code null}
   * @throws KeyProviderUnavailableException if the provider is transiently unreachable (for example
   *     a KMS/HSM timeout or 5xx). This is retryable: a short outage must not permanently lose
   *     jobs.
   * @throws EncryptionConfigurationException if the provider is misconfigured such that no current
   *     key can ever be produced (for example a missing or malformed key reference). This is a
   *     bootstrap-time error; the reference implementation's installer raises it to fail the node
   *     at startup rather than poll with encryption that cannot work.
   */
  EncryptionKey currentKey();

  /**
   * Resolves the key previously recorded under the given id, so a stored value can be decrypted
   * under the same key that wrote it. Old keys MUST stay resolvable until every row referencing
   * them has drained.
   *
   * @param keyId the key id read from a stored envelope; never {@code null}
   * @return the key for {@code keyId}; never {@code null}
   * @throws KeyNotFoundException if the id is unknown to the provider — typically a key retired
   *     before its rows drained. This is poison data, non-retryable: the value routes to the
   *     controlled failure path (DLQ), and the remediation is to re-add the key and replay.
   * @throws KeyProviderUnavailableException if the provider is transiently unreachable. This is
   *     retryable with backoff, distinct from an id that is genuinely absent.
   */
  EncryptionKey keyById(String keyId);
}
