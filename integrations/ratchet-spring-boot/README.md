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

## Native images

Native support is verified with Spring Boot **3.5.16 and 4.1.1**, **GraalVM 25**,
and PostgreSQL, MySQL, Oracle, SQL Server, and MongoDB. SQL images use Hibernate;
EclipseLink remains a JVM configuration. Java 17/21 JVM coverage remains in CI.
The ten native CI cells build real executables and run them against real databases.
See [consumer verification](consumer-tests/README.md) for reproducible commands and evidence levels.

Spring AOT discovers classes under Boot's application package roots, including nested,
local, and anonymous submitters, and registered application bean types outside those roots.
It registers invocation and payload reflection, serializable lambda metadata, and submitter
bytecode for Ratchet's lambda analyzer. Discovery does not construct job beans.

Opt dependency libraries into registration on an application configuration class:

```java
@Configuration(proxyBeanMethods = false)
@RegisterRatchetTypes(
    value = {LibrarySubmitter.class, LibraryJob.class, LibraryPayload.class},
    basePackageClasses = LibraryJobsPackage.class)
class JobsConfiguration {}
```

Import `run.ratchet.spring.boot.autoconfigure.RegisterRatchetTypes`. Explicit types include
their nested classes; package markers include their package and subpackages. Prefer explicit
types for large libraries. Registration controls executable contents only: configure
`ratchet.allowed-packages` or a `ClassPolicy` bean separately to authorize library jobs.
Result-type authorization also remains separate.

Install exactly one Ratchet SQL store artifact when building an image. AOT selects its factory,
JPA model, vendor XML mapping, and transaction proxy metadata without connecting to the database.
Runtime startup checks that the actual database matches the selected store, then performs schema
initialization and recurring registration. Application entities, converters, and application-owned
`META-INF/orm.xml` are retained; AOT copies that mapping under a unique resource name.
MongoDB continues to use Boot's configured client and database.

Profiles and properties that select bean definitions must be supplied during `process-aot`.
Database credentials, job arguments, encryption keys, and other runtime settings should be
supplied when launching the executable. Rebuild when changing stores, Boot versions, profiles,
or available job types. Use `clean` when changing the build configuration to remove old AOT output.
