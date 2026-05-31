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
import run.ratchet.api.RatchetOptions;

/** Supplies execution limits without exposing RI-only execution type enums to API consumers. */
@Incubating
public interface ExecutionTuningProvider {

  /**
   * Returns the pool a job runs on when it specifies no target of its own.
   *
   * @return the default threading mode; never {@code null}
   */
  RatchetOptions.ThreadingMode defaultThreadingMode();

  /**
   * Returns the maximum platform-thread concurrency for an execution type.
   *
   * @param executionTypeName non-null public execution type name such as {@code SINGLE} or {@code
   *     BATCH_CHILD}; unknown names should fall back to {@code defaultValue}
   * @param defaultValue value Ratchet would use if the provider does not override the type
   * @return effective maximum concurrency; should be positive
   */
  int maxConcurrency(String executionTypeName, int defaultValue);

  /**
   * Returns the virtual-thread concurrency limit for an execution type.
   *
   * @param executionTypeName non-null public execution type name such as {@code SINGLE} or {@code
   *     BATCH_CHILD}; unknown names should fall back to {@code defaultValue}
   * @param defaultValue value Ratchet would use if the provider does not override the type
   * @return effective virtual-thread limit; should be positive
   */
  int virtualThreadLimit(String executionTypeName, int defaultValue);
}
