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
package run.ratchet.coordinator.common;

import jakarta.enterprise.inject.Instance;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;

/** Shared CDI {@link Instance} resolution helpers for the bundled cluster coordinators. */
public final class CoordinatorSupport {

  private static final Logger log = Logger.getLogger(CoordinatorSupport.class);

  private CoordinatorSupport() {}

  /**
   * Resolves a required dependency from a CDI {@link Instance}, throwing when unsatisfied and
   * warning (then taking the first match) when ambiguous.
   */
  public static <T> T resolveRequired(
      Instance<T> instance, String unsatisfiedMessage, String ambiguousMessage) {
    if (instance == null || instance.isUnsatisfied()) {
      throw new IllegalStateException(unsatisfiedMessage);
    }
    if (instance.isAmbiguous()) {
      log.warn(ambiguousMessage);
    }
    return instance.get();
  }

  /**
   * Resolves a coordinator config from its {@link Instance}, falling back to the supplied defaults
   * when no producer is present.
   */
  public static <C> C resolveConfigOrDefault(Instance<C> configInstance, Supplier<C> defaults) {
    return configInstance != null && configInstance.isResolvable()
        ? configInstance.get()
        : defaults.get();
  }

  /**
   * Resolves Ratchet options from CDI, falling back to compiled defaults when no producer exists.
   */
  public static RatchetOptions resolveOptionsOrDefault(Instance<RatchetOptions> optionsInstance) {
    return resolveConfigOrDefault(optionsInstance, RatchetOptions::defaults);
  }
}
