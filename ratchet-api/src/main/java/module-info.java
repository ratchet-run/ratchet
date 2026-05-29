/**
 * Public API and SPI for Ratchet, the portable Jakarta EE-aligned job scheduler.
 *
 * <p>Many types in {@code run.ratchet.spi} are marked {@link
 * run.ratchet.api.Incubating @Incubating} — their method signatures and contracts may evolve
 * between minor releases. Module-path exports are an access-control mechanism, not a stability
 * promise. Treat {@code @Incubating} types as semver-exempt.
 *
 * <p>Ratchet runtime, store, metrics, and TCK artifacts also provide explicit module descriptors.
 * Classpath consumers see no change.
 */
module run.ratchet.api {
  // jakarta.cdi is `requires transitive` because Ratchet's API surface includes CDI annotations
  // (`@CircuitBreakerProtected` and the `@InterceptorBinding`-based binding type) that consumer
  // modules must be able to reference. Module-path consumers using ratchet-api inherit
  // jakarta.cdi automatically; classpath consumers see no change.
  requires transitive jakarta.cdi;

  // jakarta.interceptor is needed by `@CircuitBreakerProtected` (the @InterceptorBinding meta-
  // annotation). Not transitive — consumers using @CircuitBreakerProtected as a marker only need
  // ratchet-api on the module path, not interceptor itself.
  requires jakarta.interceptor;

  // Required transitively by jakarta.interceptor's own module-info; declared explicitly so the
  // resolver can satisfy interceptor when ratchet-api is the root module.
  requires jakarta.annotation;

  // java.util.logging is used by RatchetConfigKey to WARN on invalid values before falling back
  // to defaults. JUL is chosen over SLF4J/JBoss Logging to keep ratchet-api dependency-free.
  requires java.logging;

  exports run.ratchet.api;
  exports run.ratchet.api.event;
  exports run.ratchet.api.exception;
  // run.ratchet.api.internal is framework-internal. The qualified export limits visibility to
  // the Ratchet reference implementation, which needs to read JobBuilder state and bootstrap
  // the typed configuration chain. Applications must not depend on these types.
  exports run.ratchet.api.internal to
      run.ratchet.ri;
  // run.ratchet.spi is open to all module-path consumers. Every type in this package carries
  // @Incubating, which is the stability contract — method signatures and contracts may change
  // between minor releases without a semver major bump. The qualified allowlist was removed
  // because it blocked third-party store, retry-policy, and metrics implementations from
  // compiling against the module path, contradicting the spec-candidacy posture.
  // Classpath consumers are unaffected either way.
  exports run.ratchet.spi;
}
