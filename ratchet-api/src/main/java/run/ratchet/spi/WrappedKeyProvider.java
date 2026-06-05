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
 * A {@link KeyProvider} that backs encryption with <b>envelope encryption</b>: each write uses a
 * freshly generated data-encryption key (DEK), and a <em>wrapped</em> form of that DEK — encrypted
 * under a master key the provider controls — is stored alongside the ciphertext so the value can be
 * decrypted later without the provider retaining per-write state. This is the seam an external key
 * service (AWS KMS, GCP KMS, HashiCorp Vault) implements: the master key never leaves the service,
 * and a service round-trip wraps on write and unwraps on read.
 *
 * <p><b>Why envelope encryption.</b> External key services cap the plaintext they will encrypt
 * directly (AWS KMS {@code Encrypt} is 4&nbsp;KB) far below a job payload, so they cannot be the
 * cipher engine. Instead they wrap a local DEK: the engine performs the bulk AEAD in-process under
 * the DEK, and the service only ever sees the small DEK. The framework persists the wrapped DEK in
 * the envelope's authenticated wrapped-key field.
 *
 * <p><b>What the framework does with this.</b> On write it calls {@link #currentWrappedKey()},
 * encrypts under the returned DEK, and stores the returned wrapped blob in the envelope. On read it
 * reads the {@code keyId} and wrapped blob back from the envelope and calls {@link
 * #unwrapKey(String, byte[])} to recover the DEK. The {@code keyId} stamped in the envelope
 * identifies the <em>master key</em> used to unwrap, not the ephemeral DEK.
 *
 * <p><b>Failure classification (part of the contract).</b> Because the framework routes on
 * exception type, implementations MUST map service failures accordingly:
 *
 * <ul>
 *   <li>A transient outage (timeout, throttle, 5xx) from {@link #currentWrappedKey()} or {@link
 *       #unwrapKey(String, byte[])} MUST be a {@link KeyProviderUnavailableException} — retryable,
 *       so a short outage does not lose work.
 *   <li>An {@code unwrapKey} call for a master key the service has permanently removed MUST be a
 *       {@link KeyNotFoundException} — poison, not retryable.
 *   <li>The inherited {@link #keyById(String)} cannot serve a wrapped value (a DEK is recovered
 *       only by unwrapping, never by id). A pure envelope-encryption provider therefore SHOULD
 *       throw {@link EncryptionConfigurationException} from {@code keyById} rather than {@code
 *       UnsupportedOperationException}, keeping the failure inside the encryption exception
 *       taxonomy. The framework does not call {@code keyById} for a value that carries a wrapped
 *       key.
 * </ul>
 *
 * <p><b>Thread-safety:</b> as with {@link KeyProvider}, implementations MUST be thread-safe; a
 * single provider instance serves concurrent write and read paths.
 */
@Incubating
public interface WrappedKeyProvider extends KeyProvider {

  /**
   * A freshly generated data-encryption key for a write, paired with its wrapped form to persist in
   * the envelope.
   *
   * @param key the DEK the engine encrypts under; its {@link EncryptionKey#keyId() keyId} names the
   *     master key needed to unwrap it later
   * @param wrapped the wrapped DEK to store in the envelope's wrapped-key field; never {@code null}
   *     or empty
   */
  record WrappedKey(EncryptionKey key, byte[] wrapped) {}

  /**
   * Returns a fresh data-encryption key for a new write together with its wrapped form.
   *
   * @return the DEK and its wrapped blob; never {@code null}
   * @throws KeyProviderUnavailableException if the key service is transiently unreachable
   *     (retryable)
   * @throws EncryptionConfigurationException if the provider is misconfigured such that no key can
   *     ever be produced (fail-loud at startup)
   */
  WrappedKey currentWrappedKey();

  /**
   * Recovers the data-encryption key for a stored value by unwrapping its wrapped blob under the
   * named master key.
   *
   * @param keyId the master-key id read from the envelope (the value stamped by {@link
   *     #currentWrappedKey()})
   * @param wrappedKey the wrapped DEK read from the envelope's wrapped-key field; never {@code
   *     null} or empty
   * @return the recovered DEK; never {@code null}
   * @throws KeyNotFoundException if the master key has been permanently removed (poison,
   *     non-retryable)
   * @throws KeyProviderUnavailableException if the key service is transiently unreachable
   *     (retryable)
   */
  EncryptionKey unwrapKey(String keyId, byte[] wrappedKey);
}
