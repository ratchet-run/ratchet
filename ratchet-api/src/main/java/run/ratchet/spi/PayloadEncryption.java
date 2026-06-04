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
import run.ratchet.api.exception.PayloadDecryptionException;

/**
 * SPI for the keyed authenticated-encryption (AEAD) transform that protects sensitive job-data
 * values at rest.
 *
 * <p>This is a pure transform over bytes and nothing more. It holds no key state and performs no key
 * lookup — the key for the operation arrives already resolved on the {@link EncryptionContext}. The
 * framework decides <em>which</em> leaves to protect, selects the key, owns the persisted envelope
 * and its framing, and computes the additional-authenticated-data (AAD) bytes; this engine owns only
 * the algorithm. That split keeps the default engine small enough to audit and lets a future key
 * service replace key handling without touching the cipher, or replace the cipher without touching
 * key handling.
 *
 * <p><b>Authenticated, not merely reversible.</b> Unlike a plain reversible transform, an AEAD
 * cipher binds the ciphertext to the {@link EncryptionContext#additionalAuthenticatedData() AAD} and
 * detects tampering: decrypting corrupted bytes, or bytes whose AAD or key does not match, MUST fail
 * loudly with {@link PayloadDecryptionException} rather than return plausible-looking garbage. That
 * failure is what lets the framework route poison data to a controlled failure path instead of
 * executing a job on silently wrong arguments.
 *
 * <p><b>The engine owns the nonce.</b> {@link #encrypt(byte[], EncryptionContext)} returns an opaque
 * body that carries everything decryption needs except the key and AAD — for an AES-GCM engine that
 * is {@code nonce ∥ ciphertext ∥ tag}. The framework does not generate, supply, or interpret the
 * nonce; it wraps the returned body in its versioned envelope as opaque payload. Nonce uniqueness
 * per key is the engine's responsibility, and getting it wrong (nonce reuse under one key) is
 * catastrophic for GCM, so the construction is a security-critical detail of the implementation.
 *
 * <p><b>Algorithm dispatch.</b> {@link #algorithmId()} is recorded in the envelope and selects the
 * engine at read time, so a value is always decrypted by the same algorithm that wrote it. An id
 * whose engine is not installed is poison data the framework routes to the failure path; an
 * algorithm must therefore remain installed until every row that names it has drained.
 *
 * <p><b>Thread-safety:</b> implementations MUST be thread-safe. The framework holds a single engine
 * instance per deployment and invokes {@link #encrypt(byte[], EncryptionContext)} and {@link
 * #decrypt(byte[], EncryptionContext)} concurrently from poller worker threads and persistence
 * paths.
 */
@Incubating
public interface PayloadEncryption {

  /**
   * Returns the stable identifier of the algorithm this engine implements (for example {@code
   * AES-256-GCM}). The framework records it in the envelope and uses it to select the engine when
   * decrypting a stored value.
   *
   * @return the algorithm id; never {@code null} or blank, and stable across the engine's lifetime
   */
  String algorithmId();

  /**
   * Encrypts the given plaintext under the key on {@code ctx}, binding the ciphertext to {@code
   * ctx.additionalAuthenticatedData()}.
   *
   * @param plaintext the bytes to protect; never {@code null}
   * @param ctx the operation context carrying the resolved key and final AAD; never {@code null}
   * @return an opaque AEAD body that carries its own nonce (for AES-GCM, {@code nonce ∥ ciphertext ∥
   *     tag}); the framework stores it as opaque envelope payload. Never {@code null}.
   */
  byte[] encrypt(byte[] plaintext, EncryptionContext ctx);

  /**
   * Decrypts a body previously produced by {@link #encrypt(byte[], EncryptionContext)} under the
   * same algorithm, using the key on {@code ctx} and verifying {@code
   * ctx.additionalAuthenticatedData()}.
   *
   * @param ciphertext the opaque AEAD body the framework extracted from the envelope; never {@code
   *     null}
   * @param ctx the operation context carrying the resolved key and final AAD; never {@code null}
   * @return the recovered plaintext; never {@code null}
   * @throws PayloadDecryptionException if authentication fails — corrupted bytes, a mismatched key,
   *     or AAD that does not match the value the ciphertext was bound to. This is poison data, not a
   *     transient fault: the framework routes it to the controlled failure path, never to a retry.
   */
  byte[] decrypt(byte[] ciphertext, EncryptionContext ctx);
}
