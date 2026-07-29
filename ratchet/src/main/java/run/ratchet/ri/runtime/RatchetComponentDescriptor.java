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
package run.ratchet.ri.runtime;

import java.util.List;
import java.util.Objects;

/**
 * Describes one container-managed Ratchet component without exposing its implementation types in
 * this package's public API.
 *
 * @param componentType component implementation type
 * @param constructorParameterTypes selected constructor parameter types, in declaration order
 * @param singletonScope whether the container should manage the component as a singleton
 * @param transactional whether the component declares transactional behavior
 */
public record RatchetComponentDescriptor(
    Class<?> componentType,
    List<Class<?>> constructorParameterTypes,
    boolean singletonScope,
    boolean transactional) {

  /** Creates an immutable component descriptor. */
  public RatchetComponentDescriptor {
    Objects.requireNonNull(componentType, "componentType");
    constructorParameterTypes =
        List.copyOf(Objects.requireNonNull(constructorParameterTypes, "constructorParameterTypes"));
  }
}
