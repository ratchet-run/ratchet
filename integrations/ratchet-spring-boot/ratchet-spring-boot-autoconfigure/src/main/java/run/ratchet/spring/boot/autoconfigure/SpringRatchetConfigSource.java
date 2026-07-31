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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import run.ratchet.spi.RatchetConfigSource;

/** Reads canonical Ratchet configuration through Spring Boot's relaxed {@link Binder}. */
final class SpringRatchetConfigSource implements RatchetConfigSource {

  private static final String CLASS_POLICY_ALLOWED_PACKAGES =
      "ratchet.class-policy.allowed-packages";
  private static final String CLASS_POLICY_ALLOWED_RESULT_TYPE_PACKAGES =
      "ratchet.class-policy.allowed-result-type-packages";
  private static final Set<String> LIST_PROPERTIES =
      Set.of(CLASS_POLICY_ALLOWED_PACKAGES, CLASS_POLICY_ALLOWED_RESULT_TYPE_PACKAGES);

  private final Binder binder;

  SpringRatchetConfigSource(Environment environment) {
    this.binder = Binder.get(environment);
  }

  @Override
  public Optional<String> get(String propertyName, String environmentVariable) {
    if (propertyName == null || propertyName.isBlank()) {
      return Optional.empty();
    }
    if (LIST_PROPERTIES.contains(propertyName)) {
      var result =
          binder
              .bind(propertyName, Bindable.listOf(String.class))
              .map(SpringRatchetConfigSource::join);
      return result.isBound() ? Optional.of(result.get()) : Optional.empty();
    }
    var result = binder.bind(propertyName, String.class);
    return result.isBound() ? Optional.of(result.get()) : Optional.empty();
  }

  private static String join(List<String> values) {
    return String.join(",", values);
  }
}
