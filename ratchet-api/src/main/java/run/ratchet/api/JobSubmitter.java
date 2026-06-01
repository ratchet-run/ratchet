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
package run.ratchet.api;

/**
 * Functional interface for submitting a configured job for execution.
 *
 * <p>This decouples {@link JobBuilder} from the concrete scheduler service. The reference
 * implementation implements {@code JobSubmitter} on its job-creation service.
 */
@FunctionalInterface
public interface JobSubmitter {

  /**
   * Submits the fully-configured job builder for persistence and scheduling.
   *
   * @param builder non-null builder to validate and persist
   * @return non-null handle for the persisted job, including its assigned UUIDv7 id
   * @throws NullPointerException if {@code builder} is null
   * @throws RuntimeException if validation, authorization, serialization, or persistence fails
   */
  JobHandle submit(JobBuilder builder);
}
