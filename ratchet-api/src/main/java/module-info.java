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
  // Qualified export: spi is @Incubating and subject to change between minor releases.
  // Module-path consumers that want to ship their own SPI implementations must be listed here.
  // Classpath consumers are unaffected — the restriction applies only on the module path.
  exports run.ratchet.spi to
      run.ratchet.ri,
      run.ratchet.store.core,
      run.ratchet.store.mysql,
      run.ratchet.store.postgresql,
      run.ratchet.store.mongodb,
      run.ratchet.tck.store,
      run.ratchet.tck.api,
      run.ratchet.micrometer,
      run.ratchet.otel,
      ratchet.testsuite.jpms;
}
