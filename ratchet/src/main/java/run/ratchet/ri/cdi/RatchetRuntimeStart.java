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
package run.ratchet.ri.cdi;

import org.jboss.logging.Logger;

/**
 * Startup signal that drives Ratchet's engine start on build-time-CDI runtimes.
 *
 * <p>On a full Jakarta EE server the engine auto-starts on
 * {@code @Initialized(ApplicationScoped.class)}, which the container fires only after the
 * persistence unit and JTA are ready. Build-time-CDI runtimes such as Quarkus/ArC fire
 * {@code @Initialized(ApplicationScoped.class)} during STATIC_INIT — before the {@code
 * EntityManager} exists — so the engine's database-touching startup cannot run there.
 *
 * <p>Such a runtime sets the system property {@value #DEFER_PROPERTY} to {@code true} (which makes
 * the engine's {@code @Initialized(ApplicationScoped.class)} startup observers no-op) and instead
 * fires this event from a later, post-persistence lifecycle event (for example Quarkus {@code
 * StartupEvent}). The engine's startup observers also observe this event, so firing it starts the
 * engine at the correct time. When the property is unset (the default, as on every Jakarta EE
 * server), behaviour is unchanged: the engine auto-starts on context initialization and this event
 * is never fired.
 */
public final class RatchetRuntimeStart {

  /** System property that defers context-initialization auto-start; see the class Javadoc. */
  public static final String DEFER_PROPERTY = "ratchet.lifecycle.defer-auto-start";

  /**
   * Environment-variable form of {@link #DEFER_PROPERTY}, for runtimes launched as a separate
   * process (e.g. a GraalVM native binary under integration test) where setting a JVM system
   * property is awkward.
   */
  public static final String DEFER_ENV_VAR = "RATCHET_LIFECYCLE_DEFER_AUTO_START";

  /** Returns {@code true} when context-initialization auto-start has been deferred. */
  public static boolean autoStartDeferred() {
    return Boolean.getBoolean(DEFER_PROPERTY)
        || "true".equalsIgnoreCase(System.getenv(DEFER_ENV_VAR));
  }

  /**
   * Common guard for {@code @Initialized(ApplicationScoped.class)} observers that defer their
   * startup work to this event: logs {@code message} and returns {@code true} when deferred, so the
   * caller can {@code return} immediately; returns {@code false} (logging nothing) otherwise.
   */
  public static boolean logIfDeferred(Logger log, String message) {
    if (!autoStartDeferred()) {
      return false;
    }
    log.info(message);
    return true;
  }
}
