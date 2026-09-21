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
package run.ratchet.spring.boot.autoconfigure;

import org.springframework.aot.hint.BindingReflectionHintsRegistrar;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;

/** Reflection used by the engine independently of application job discovery. */
public final class RatchetRuntimeHints implements RuntimeHintsRegistrar {
  @Override
  public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
    for (String name :
        new String[] {
          "run.ratchet.store.entity.JobPayload",
          "run.ratchet.ri.cdi.RecurringMethodInvoker",
          "run.ratchet.ri.util.JobPlaceholders",
          "java.util.concurrent.Executors",
          "org.eclipse.yasson.JsonBindingProvider",
          "org.eclipse.parsson.JsonProviderImpl"
        }) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(name),
              MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
              MemberCategory.INVOKE_DECLARED_METHODS,
              MemberCategory.DECLARED_FIELDS);
    }
    new BindingReflectionHintsRegistrar()
        .registerReflectionHints(hints.reflection(), run.ratchet.store.entity.JobPayload.class);
    hints.resources().registerPattern("META-INF/services/jakarta.json.bind.spi.JsonbProvider");
    hints.resources().registerPattern("META-INF/services/jakarta.json.spi.JsonProvider");
  }
}
