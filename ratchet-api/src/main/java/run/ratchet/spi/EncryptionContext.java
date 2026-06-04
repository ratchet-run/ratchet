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

import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.Nullable;

/**
 * The per-operation context the framework hands to a {@link PayloadEncryption} engine.
 *
 * <p>It carries everything the engine needs and nothing it gets to decide. The framework has
 * already selected the key for this operation and computed the final additional-authenticated-data
 * (AAD) bytes; the engine consumes both verbatim. This is what makes encrypt/decrypt symmetric by
 * construction: the engine has no opportunity to resolve a different key or recompute AAD
 * differently on the read path than on the write path.
 *
 * <p>Instances are immutable. The constructor and {@link #additionalAuthenticatedData()} both copy
 * the AAD array, so neither the caller nor the engine can mutate the bytes the other relies on.
 */
@Incubating
public final class EncryptionContext {

  private final ProtectedSurface surface;
  private final UUID jobId;
  private final EncryptionKey key;
  private final byte[] additionalAuthenticatedData;

  /**
   * Creates an encryption context for a single AEAD operation.
   *
   * @param surface the protected surface being encrypted or decrypted; must not be {@code null}
   * @param jobId the owning job id, or {@code null} for a surface whose AAD does not bind to a job
   *     (see {@link ProtectedSurface#SIGNAL_PAYLOAD} and {@link
   *     ProtectedSurface#WORKFLOW_CONDITION_PREDICATE})
   * @param key the key the framework resolved for this operation — the current key on encrypt, the
   *     envelope's key on decrypt; must not be {@code null}
   * @param aad the final AAD bytes the framework computed for this surface; must not be {@code
   *     null}. The array is defensively copied; the engine MUST use these bytes verbatim and MUST
   *     NOT recompute AAD.
   */
  public EncryptionContext(
      ProtectedSurface surface, @Nullable UUID jobId, EncryptionKey key, byte[] aad) {
    this.surface = Objects.requireNonNull(surface, "surface must not be null");
    this.jobId = jobId;
    this.key = Objects.requireNonNull(key, "key must not be null");
    this.additionalAuthenticatedData =
        Objects.requireNonNull(aad, "aad must not be null").clone();
  }

  /**
   * Returns the protected surface this operation targets.
   *
   * @return the surface; never {@code null}
   */
  public ProtectedSurface surface() {
    return surface;
  }

  /**
   * Returns the owning job id, or {@code null} for a surface whose AAD binds to the surface alone.
   *
   * @return the job id, or {@code null}
   */
  public @Nullable UUID jobId() {
    return jobId;
  }

  /**
   * Returns the key resolved by the framework for this operation: the current key on encrypt, the
   * envelope's key on decrypt. The engine reads its material (or id) from here and performs no key
   * lookup of its own.
   *
   * @return the resolved key; never {@code null}
   */
  public EncryptionKey key() {
    return key;
  }

  /**
   * Returns a copy of the final AAD bytes the framework computed for this surface. The engine must
   * pass these to the cipher verbatim and never recompute them.
   *
   * @return a fresh copy of the AAD bytes; never {@code null} (may be empty)
   */
  public byte[] additionalAuthenticatedData() {
    return additionalAuthenticatedData.clone();
  }
}
