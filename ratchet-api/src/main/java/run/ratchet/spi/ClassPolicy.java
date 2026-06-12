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
 * Policy for controlling which classes may be deserialized during job payload restoration.
 *
 * <p>This interface is marked {@link Incubating} — the allow/deny API may be extended in future
 * releases without following the normal deprecation cycle.
 */
@Incubating
public interface ClassPolicy {

  /**
   * Returns whether a class name may be used during payload restoration or invocation.
   *
   * @param className fully qualified binary class name; {@code null} or blank input must be denied
   * @return {@code true} to allow the class, {@code false} to deny it
   */
  boolean isAllowed(String className);

  /**
   * Returns whether a class name may be instantiated when deserializing a persisted job result (the
   * {@code result_type} column) so a workflow condition can inspect the typed value.
   *
   * <p>This is deliberately separate from {@link #isAllowed(String)}. The invocation allowlist
   * gates classes that own a matching predicate method; result deserialization has no such
   * constraint, so any invocation-allowed class with a JSON-constructable shape could otherwise be
   * instantiated with attacker-supplied field values given write access to the result columns.
   * Implementations MUST therefore treat this as a strictly narrower control and default to deny.
   *
   * <p>When a result type is denied, the engine falls back to parsing the stored JSON into its
   * native representation (numbers, strings, maps, lists) rather than instantiating the named
   * class.
   *
   * @param className fully qualified binary class name; {@code null} or blank input must be denied
   * @return {@code true} to allow instantiating the class for result deserialization, {@code false}
   *     to deny it (the default)
   */
  default boolean isAllowedForResultType(String className) {
    return false;
  }
}
