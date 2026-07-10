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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import run.ratchet.api.Incubating;

/**
 * Per-job context handed to the context-aware {@link PayloadMaskingPolicy#isSensitiveField(String,
 * MaskingContext)} overload, so a policy can decide sensitivity per job rather than by field name
 * alone (the same field name can be secret in one job and not in another).
 *
 * <p>{@code jobProperties()} carries the job's extension-property rows (read through the {@code
 * JobExtensionStore} capability when the store advertises it; empty otherwise). Extensions key
 * their identity here — for example {@code ratchet-blocks.block_name} — so a policy can look up
 * what the job is and which of its fields are declared secret.
 *
 * <p>Properties are loaded and copied to an immutable map on first access, then memoized. This
 * context is used single-threaded during masking, so the lazy memoization is intentionally
 * unsynchronized and instances do not provide record value semantics.
 */
@Incubating
public final class MaskingContext {

  private final UUID jobId;
  private final Supplier<Map<String, String>> jobPropertiesSupplier;
  private Map<String, String> jobProperties;

  public MaskingContext(UUID jobId, Map<String, String> jobProperties) {
    this(jobId, () -> jobProperties);
  }

  public MaskingContext(UUID jobId, Supplier<Map<String, String>> jobPropertiesSupplier) {
    this.jobId = Objects.requireNonNull(jobId, "jobId");
    this.jobPropertiesSupplier =
        Objects.requireNonNull(jobPropertiesSupplier, "jobPropertiesSupplier");
  }

  public UUID jobId() {
    return jobId;
  }

  public Map<String, String> jobProperties() {
    if (jobProperties == null) {
      Map<String, String> supplied = jobPropertiesSupplier.get();
      jobProperties = supplied == null ? Map.of() : Map.copyOf(supplied);
    }
    return jobProperties;
  }
}
