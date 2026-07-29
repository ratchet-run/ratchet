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

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import run.ratchet.api.Incubating;

/**
 * Internal utility for Ratchet modules that adapt CDI {@link Instance}-backed principal sources.
 *
 * <p>This type is public only so Ratchet modules in different artifacts can share identical lookup
 * semantics. It is not an application SPI; applications should implement {@link PrincipalSource} or
 * {@link CallerPrincipalResolver} instead.
 */
@Incubating
public final class PrincipalSourceInstances {

  private PrincipalSourceInstances() {}

  /**
   * Resolves a principal from a CDI {@link Instance}, destroying dependent handles after use and
   * converting lookup failures to {@link Optional#empty()} after the caller logs them.
   */
  public static <T> Optional<String> currentPrincipal(
      Instance<T> instances,
      Function<T, Optional<String>> extractor,
      String missingInstanceMessage,
      Consumer<RuntimeException> lookupFailureLogger) {
    if (instances == null) {
      throw new IllegalStateException(missingInstanceMessage);
    }
    try {
      if (!instances.isResolvable()) {
        return Optional.empty();
      }
      Instance.Handle<T> handle = instances.getHandle();
      try {
        Optional<String> principal = extractor.apply(handle.get());
        return principal == null ? Optional.empty() : principal.filter(name -> !name.isEmpty());
      } finally {
        if (handle.getBean().getScope().equals(Dependent.class)) {
          handle.destroy();
        }
      }
    } catch (RuntimeException e) {
      lookupFailureLogger.accept(e);
      return Optional.empty();
    }
  }
}
