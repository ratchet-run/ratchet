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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Optional capability for generic per-job extension storage: write-once indexed scalar properties
 * and mutable per-namespace structured state.
 *
 * <p>Two surfaces back this capability, both keyed by the owning job:
 *
 * <ul>
 *   <li><b>Properties</b> ({@code scheduler_job_properties}) — flat {@code key → value} scalars,
 *       namespaced by key convention ({@code ratchet-blocks.block_name}, {@code
 *       ratchet-saga.compensation_target}, ...). Intended to be write-once at submission. Values
 *       are <b>plaintext by design</b> so the query layer can index-match them; secrets MUST NOT be
 *       written as properties — secret-capable data belongs in extension state, which is encrypted
 *       at rest when payload encryption is configured.
 *   <li><b>Extension state</b> ({@code scheduler_job_extension_state}) — one mutable JSON blob per
 *       {@code (job, namespace)} with its own per-row version for optimistic CAS. The version is
 *       independent of every other lock in the schema, so state updates never contend with the
 *       claim path or status transitions. The stored blob runs through the configured {@code
 *       PayloadEncryption} engine (encrypt before the CAS write, decrypt after the read); the
 *       version always covers the stored — possibly ciphertext — bytes.
 * </ul>
 *
 * <p>Both surfaces live in the hot store and follow the owning job through archiving: the archive
 * path copies them onto the archive row, and the hot rows are removed with the hot job row.
 *
 * <p>Stores opt in by implementing this interface; callers probe via {@code
 * jobStore.capability(JobExtensionStore.class)}.
 */
@Incubating
public interface JobExtensionStore {

  /**
   * Writes one property for a job, replacing any existing value for the key.
   *
   * <p>Properties are intended to be write-once at submission; nothing in this contract prevents
   * replacement, but values are designed to be stable for the job's lifetime.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId owning job id; never {@code null}
   * @param key property key, namespaced by convention; never {@code null} or blank, at most 255
   *     characters
   * @param value property value, plaintext by design (no secrets); may be {@code null}, at most
   *     1024 characters
   */
  void putProperty(UUID jobId, String key, String value);

  /**
   * Reads one property value for a job.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId owning job id; never {@code null}
   * @param key property key; never {@code null}
   * @return the stored value, or empty when the row is absent or holds a {@code null} value
   */
  Optional<String> getProperty(UUID jobId, String key);

  /**
   * Reads all properties of a job whose key starts with the given prefix.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId owning job id; never {@code null}
   * @param prefix key prefix (typically a namespace such as {@code "ratchet-blocks."}); never
   *     {@code null}, the empty prefix matches every property
   * @return matching {@code key → value} entries ({@code null} values omitted); never {@code null}
   */
  Map<String, String> getPropertiesByPrefix(UUID jobId, String prefix);

  /**
   * Reads the extension state for one {@code (job, namespace)}.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace extension namespace; never {@code null} or blank, at most 64 characters
   * @return the decrypted state blob and its current version, or empty when absent
   */
  Optional<ExtensionState> getState(UUID jobId, String namespace);

  /**
   * Creates the extension-state row for a {@code (job, namespace)} at version 0.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId owning job id; never {@code null}
   * @param namespace extension namespace; never {@code null} or blank, at most 64 characters
   * @param initialState serialized JSON blob; never {@code null}
   * @throws IllegalStateException when a row already exists for the {@code (job, namespace)}
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
   */
  boolean updateState(UUID jobId, String namespace, String newState, int expectedVersion);

  /**
   * One extension-state read: the decrypted JSON blob and the version that CAS updates must pass
   * back as {@code expectedVersion}.
   *
   * @param json the decrypted state blob; never {@code null}
   * @param version the row's current version; non-negative
   */
  record ExtensionState(String json, int version) {}
}
