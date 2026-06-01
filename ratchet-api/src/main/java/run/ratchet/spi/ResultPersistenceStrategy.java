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

import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Serializes job return values before they are stored on the job row.
 *
 * @since 0.1
 */
@Incubating
public interface ResultPersistenceStrategy {

  /**
   * Serializes a completed job result.
   *
   * @param jobId job whose result is being persisted
   * @param result returned value; may be {@code null}
   * @return serialized representation; never {@code null}
   * @apiNote Returning {@code null} violates the SPI contract and will fail callers that persist
   *     the returned value without another null check.
   */
  SerializedJobResult serialize(UUID jobId, Object result);
}
