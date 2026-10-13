# Ratchet Micronaut

The Micronaut integration runs Ratchet in Micronaut 4.10 applications on Java 17 or later and in
Micronaut 5 applications on Java 25 or later, with SQL or MongoDB stores, on the JVM and as a
GraalVM native image. One set of jars, compiled against Micronaut 4.10, serves both lines.

- [Micronaut guide](../../website/docs/deployment/micronaut.md): installation, a first job,
  transactions and schema, MongoDB, configuration, events, circuit breakers, and native images.
- [Consumer verification](consumer-tests/README.md): run the independent application reactor
  against staged artifacts in both lanes, on the JVM and as native executables.

The four module coordinates are:

- `ratchet-micronaut`: the engine wiring, configuration, and lifecycle.
- `ratchet-micronaut-jpa`: SQL stores through the application's Micronaut Hibernate JPA
  datasource, session factory, and transaction manager.
- `ratchet-micronaut-mongodb`: the MongoDB store through the application's `micronaut-mongo-sync`
  client.
- `ratchet-micronaut-processor`: the annotation processor that binds `@CircuitBreakerProtected`
  on application beans.

Every Ratchet bean is a default. An application bean of the same type replaces it. The engine's
`jakarta.transaction.Transactional` rules run through Micronaut's transaction manager with their
JTA meaning.
