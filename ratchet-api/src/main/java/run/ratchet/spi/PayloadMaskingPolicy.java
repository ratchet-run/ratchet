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
 * SPI that decides which payload fields are sensitive and should be masked before a payload leaves
 * the framework — whether rendered into a log line or returned from a read API such as the {@code
 * params} on a job detail. Masking is applied only on these read and observability paths; the
 * durable store payload and anything a worker needs to execute are never altered.
 *
 * <p>The built-in policy matches a fixed set of common credential and PII field names (for example
 * {@code password}, {@code token}, {@code ssn}). Deployers that need a different field set produce
 * their own implementation; the reference implementation installs the produced bean in place of the
 * built-in default.
 */
@Incubating
public interface PayloadMaskingPolicy {

  /**
   * Reports whether a payload field with the given name holds sensitive data.
   *
   * @param fieldName the field or parameter name as it appears in the payload; never {@code null}
   * @return {@code true} if the field's value should be masked in read or log output, {@code false}
   *     otherwise
   */
  boolean isSensitiveField(String fieldName);

  /**
   * Context-aware variant consulted by masking surfaces that know which job a field belongs to. The
   * default delegates to the name-only check, so existing policies keep working unchanged; a policy
   * that needs per-job decisions (the same field name secret in one job, public in another)
   * overrides this and reads the job's identity from {@link MaskingContext#jobProperties()}.
   *
   * @param fieldName the field or parameter name as it appears in the payload; never {@code null}
   * @param context the owning job's masking context; never {@code null}
   * @return {@code true} if the field's value should be masked, {@code false} otherwise
   */
  default boolean isSensitiveField(String fieldName, MaskingContext context) {
    return isSensitiveField(fieldName);
  }
}
