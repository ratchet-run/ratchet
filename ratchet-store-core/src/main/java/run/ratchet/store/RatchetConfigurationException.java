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
package run.ratchet.store;

/**
 * Thrown at startup when a Ratchet configuration is structurally incompatible with the store
 * implementation — for example, a host-supplied {@code MongoClient} configured with a non-STANDARD
 * {@code UuidRepresentation} that would silently corrupt UUIDv7 IDs.
 */
public class RatchetConfigurationException extends RuntimeException {

  public RatchetConfigurationException(String message) {
    super(message);
  }

  public RatchetConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
