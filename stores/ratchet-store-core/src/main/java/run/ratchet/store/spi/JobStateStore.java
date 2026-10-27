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
package run.ratchet.store.spi;

import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.util.ExtensionValidation;

/**
 * Optional general per-job state used by core features and extensions. Each job and namespace owns
 * a mutable JSON blob with an independent version for optimistic CAS. State is encrypted at rest
 * when payload encryption is configured; versions cover the stored bytes.
 *
 * <p>State and properties follow the owning job through archiving when advertised by the store.
 * Callers probe via {@code jobStore.capability(JobStateStore.class)}.
 */
@Incubating
public interface JobStateStore {

  /**
   * Reads the extension state for one {@code (job, namespace)}.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace extension namespace; never {@code null} or blank, at most 64 characters
   * @return the decrypted state blob and its current version, or empty when absent
   */
  Optional<VersionedState> getState(UUID jobId, String namespace);

  /**
   * Creates the extension-state row for a {@code (job, namespace)} at version 0.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace extension namespace; never {@code null} or blank, at most 64 characters
   * @param initialState serialized JSON blob; never {@code null}
   * @throws IllegalStateException when a row already exists for the {@code (job, namespace)}, or
   *     when no job exists with {@code jobId}; the message for a missing job includes its id
   * @throws IllegalArgumentException if state exceeds {@link ExtensionValidation#MAX_STATE_BYTES}
   *     UTF-8 bytes
   */
  void initState(UUID jobId, String namespace, String initialState);

  /**
   * Compare-and-set update of the extension state for one {@code (job, namespace)}.
   *
   * <p>The write succeeds only when the stored version equals {@code expectedVersion}; on success
   * the version is incremented by one. Callers retry on a {@code false} return by re-reading the
   * current state.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace extension namespace; never {@code null}
   * @param newState serialized JSON blob replacing the stored one; never {@code null}
   * @param expectedVersion the version the caller read; must be non-negative
   * @return {@code true} when the CAS write applied, {@code false} on version conflict or when the
   *     row is absent
   * @throws IllegalArgumentException if state exceeds {@link ExtensionValidation#MAX_STATE_BYTES}
   *     UTF-8 bytes
   */
  boolean updateState(UUID jobId, String namespace, String newState, int expectedVersion);

  /**
   * One extension-state read: the decrypted JSON blob and the version that CAS updates must pass
   * back as {@code expectedVersion}.
   *
   * @param json the decrypted state blob; never {@code null}
   * @param version the row's current version; non-negative
   */
  record VersionedState(String json, int version) {}
}
