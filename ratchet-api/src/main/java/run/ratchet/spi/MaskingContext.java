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
import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Per-job context handed to the context-aware {@link PayloadMaskingPolicy#isSensitiveField(String,
 * MaskingContext)} overload, so a policy can decide sensitivity per job rather than by field name
 * alone (the same field name can be secret in one job and not in another).
 *
 * <p>{@code jobProperties} carries the job's extension-property rows (read through the {@code
 * JobExtensionStore} capability when the store advertises it; empty otherwise). Extensions key
 * their identity here — for example {@code ratchet-blocks.block_name} — so a policy can look up
 * what the job is and which of its fields are declared secret.
 *
 * @param jobId owning job id; never {@code null}
 * @param jobProperties the job's extension properties; never {@code null}, may be empty
 */
@Incubating
public record MaskingContext(UUID jobId, Map<String, String> jobProperties) {

  public MaskingContext {
    jobProperties = jobProperties == null ? Map.of() : Map.copyOf(jobProperties);
  }
}
