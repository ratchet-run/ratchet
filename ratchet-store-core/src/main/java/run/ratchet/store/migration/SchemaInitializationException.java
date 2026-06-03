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
package run.ratchet.store.migration;

/**
 * Thrown when schema initialization at scheduler startup cannot continue safely.
 *
 * <p>Distinct from {@link SchemaMigrationException}: this type signals a fatal startup-time
 * configuration problem (no {@code DataSource} bound, or an unsupported database product) that
 * {@code RatchetLifecycle} re-throws to halt deployment rather than swallowing as a hook warning.
 */
public class SchemaInitializationException extends RuntimeException {

  public SchemaInitializationException(String message) {
    super(message);
  }

  public SchemaInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
