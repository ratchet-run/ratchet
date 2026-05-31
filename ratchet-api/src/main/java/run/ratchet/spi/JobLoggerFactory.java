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
 * Creates the {@link JobLogger} bound into {@code JobContext} for each execution.
 *
 * @since 0.1
 */
@Incubating
public interface JobLoggerFactory {

  /**
   * Creates the logger bound to one job execution.
   *
   * <p>The RI calls this once per execution attempt before binding {@code JobContext}. Returning
   * {@code null} violates the SPI contract. Implementations may throw a runtime exception when a
   * required logging backend is unavailable; callers treat that as an execution setup failure.
   *
   * @param context immutable execution logging context; never {@code null}
   * @return logger for this execution attempt; never {@code null}
   */
  JobLogger create(JobLoggerContext context);
}
