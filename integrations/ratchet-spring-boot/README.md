# Ratchet Spring Boot

The SQL and MongoDB starters run Ratchet in Spring Boot 3.5 and 4.1 applications on Java 17 and 21.
The starters are published to Maven Central with the other Ratchet modules and aligned by the Ratchet BOM.

- [Spring Boot guide](../../website/docs/deployment/spring-boot.md): installation, a first job,
  database configuration, transactions, virtual threads, customization, and support limits.
- [Consumer verification](consumer-tests/README.md): run the independent application reactor
  against staged artifacts, including packaged applications and process recovery.

The five module coordinates are `ratchet-spring-boot-autoconfigure`,
`ratchet-spring-boot-autoconfigure-jpa`, `ratchet-spring-boot-autoconfigure-mongodb`,
`ratchet-spring-boot-starter`, and `ratchet-spring-boot-starter-mongodb`.

`RatchetEngineAutoConfiguration` imports configurations for policy/security, submission/queries,
execution/scheduling, and runtime/maintenance. Engine constructors receive explicit dependencies;
batch completion remains separately transaction-advised. Job targets and lifecycle hooks share
managed Spring bean handles. JPA metadata uses one delegating view, and provider adaptations are
isolated in the Spring JPA module. The runtime ownership restriction is documented in the guide.
