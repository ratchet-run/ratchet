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
 * Optional indexed extension metadata for each job. Properties are intended to be write-once at
 * submission and are plaintext by design so queries can index-match them. Never store secrets in
 * properties; use encrypted per-job state instead.
 *
 * <p>State and properties follow the owning job through archiving when advertised by the store.
 * Callers probe via {@code jobStore.capability(JobPropertyStore.class)}.
 */
@Incubating
public interface JobPropertyStore {

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
}
