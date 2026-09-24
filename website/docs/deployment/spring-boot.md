---
title: Spring Boot
description: Run persistent Ratchet jobs in Spring Boot 3.5 and 4.1 with SQL or MongoDB, Java 17 or 21, application transactions, and optional virtual threads.
---

# Spring Boot

<!-- spring-starter-unreleased:start -->
The SQL and MongoDB starters are unreleased in this checkout. Build and stage the current source
tree before using these examples; no Ratchet release BOM currently supplies Spring Boot starter
coordinates.

```bash
staged_repo="$(mktemp -d)"
mvn -B -ntp -Dmaven.repo.local="$staged_repo" \
  -pl :ratchet-bom,:ratchet-spring-boot-starter,:ratchet-spring-boot-starter-mongodb,:ratchet-store-postgresql -am \
  install -Pgithub -DskipTests -Dspotbugs.skip=true -Dspotless.skip=true
```

The command stages the BOM, both starters, and the PostgreSQL store used by this quickstart;
`-am` adds their reactor dependencies. The checked-out version below must match the root project
POM.
<!-- spring-starter-unreleased:end -->

Ratchet runs in Spring Boot 3.5 and 4.1 applications on Java 17 or later. The SQL starter uses the application's existing datasource, entity-manager factory, and transaction manager. The MongoDB starter uses Boot's configured Mongo client and database factory.

## SQL quickstart

Start with a normal Spring Boot application and its Boot parent or BOM. The starter POMs default to
**4.1.1**. An application BOM still selects its own Boot version; **3.5.16** is a separate
compatibility target. Set the Ratchet version in your application's Maven properties:

```xml
<properties>
  <java.version>17</java.version>
  <ratchet.version>0.5.0-SNAPSHOT</ratchet.version>
</properties>
```

### Add the dependencies

Import the Ratchet BOM, then add the SQL starter, exactly one Ratchet SQL store, and the JDBC driver selected by your Boot BOM. For example, PostgreSQL applications need:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>${ratchet.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-spring-boot-starter</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

Use the same pattern with `ratchet-store-mysql`, `ratchet-store-oracle`, or `ratchet-store-sqlserver` and the matching database driver. The SQL starter already includes Spring Data JPA, Spring ORM, and Boot's default Hibernate
provider. You do not need to add `spring-boot-starter-data-jpa` separately. Your application's
Boot BOM selects Spring and Hibernate versions. Ratchet adds its own entity metadata without
replacing application entities, converters, mappings, or JPA properties.

For deployment, select a Boot BOM with the security fixes your application requires. Boot 4.1.1
resolves Spring Framework 7.0.9 and Spring Data JPA 4.1.1, including fixes for
[Sort validation](https://spring.io/security/cve-2026-47834/),
[SpEL exponentiation](https://spring.io/security/cve-2026-47886/),
[data-binding list growth](https://spring.io/security/cve-2026-59282/), and
[SpEL compilation](https://spring.io/security/cve-2026-59283/).
The Boot 3.5.16 compatibility baseline resolves affected versions 6.2.19 and 3.5.13;
the corresponding fixes, Framework 6.2.20 and Data JPA 3.5.14, require Spring Enterprise access.
Ratchet does not override your application's Boot BOM to supply those fixes.

### Connect a database

For a local PostgreSQL demo, start a disposable database:

```bash
docker run --rm --name ratchet-postgres -p 5432:5432 \
  -e POSTGRES_DB=orders -e POSTGRES_USER=orders -e POSTGRES_PASSWORD=local-demo \
  postgres:16
```

Configure `application.properties` as an ordinary Boot application:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/orders
spring.datasource.username=orders
spring.datasource.password=local-demo
spring.main.keep-alive=true
```

The demo has no web server, so `spring.main.keep-alive=true` keeps the application running
while Ratchet executes background jobs. Use it for standalone worker applications on either
Java version, including applications that enable virtual threads.

| Database | Ratchet store | Boot-managed runtime driver |
| --- | --- | --- |
| PostgreSQL | `ratchet-store-postgresql` | `org.postgresql:postgresql` |
| MySQL | `ratchet-store-mysql` | `com.mysql:mysql-connector-j` |
| Oracle | `ratchet-store-oracle` | `com.oracle.database.jdbc:ojdbc11` |
| SQL Server | `ratchet-store-sqlserver` | `com.microsoft.sqlserver:mssql-jdbc` |

### Submit a first job

Place both files under your Boot application's package, here `example.jobs`. The job target is a
Spring bean with a public method. Ratchet resolves that bean when it executes the persisted job.

`GreetingJobs.java`:

```java
package example.jobs;

import org.springframework.stereotype.Component;

@Component
public class GreetingJobs {
  public void greet(String name) {
    System.out.println("Hello, " + name + " from a persisted Ratchet job");
  }
}
```

`DemoApplication.java`:

```java
package example.jobs;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import run.ratchet.api.JobSchedulerService;

@SpringBootApplication
public class DemoApplication {
  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }

  @Bean
  ApplicationRunner submitGreeting(JobSchedulerService scheduler, GreetingJobs jobs) {
    return args -> {
      var job = scheduler.enqueue(() -> jobs.greet("Ada")).submit();
      System.out.println("Submitted job " + job.id());
    };
  }
}
```

Run the application with your normal Boot Maven plugin:

```bash
mvn -Dmaven.repo.local="$staged_repo" spring-boot:run
```

Startup prepares the Ratchet schema, starts the scheduler, and prints the greeting when the job
runs. This demo submits a new job on every application start. Real job handlers should tolerate
retries: execution is at least once. See [job lifecycle](/concepts/job-lifecycle).

No CDI container, `beans.xml`, `persistence.xml`, or Ratchet enable annotation is required.
Constructor-inject `JobSchedulerService` into your application services in the same way.

## Transactions and schema

For atomic application writes and job submission, call the scheduler from a Spring bean method
advised with Spring's `@Transactional`. The submitted SQL job joins the surrounding Spring transaction. A rollback removes both the application write and the submitted job. Ratchet wakeups run after a successful commit.

The starter migrates Ratchet's schema by default before Hibernate validates the persistence unit. To keep the schema under an external migration tool, apply Ratchet's supplied migrations yourself and use:

```properties
ratchet.schema.auto-migrate=false
```

That mode validates compatibility without changing the schema. An externally managed schema may omit
Ratchet's migration ledger; the supplied scripts create an empty `ratchet_schema_version` table, so
an external migration manager should drop that empty table after applying them. If the ledger is
populated, it must record every bundled migration with its matching checksum; do not delete a
populated ledger to bypass a validation mismatch. `ratchet.enabled=false` disables migrations,
discovery, workers, and Ratchet runtime installation.

Ratchet's entities are part of the selected JPA persistence unit, so Hibernate's
`spring.jpa.hibernate.ddl-auto` also acts on Ratchet tables. In production, set it to `none` or
`validate` and let Ratchet's migrator or the external migration tool own Ratchet DDL. Do not use
`create`, `create-drop`, or `update` for a durable Ratchet schema: they can recreate queued-job
tables or make Hibernate-managed DDL disagree with Ratchet migrations. Embedded test databases may
use Boot's `create-drop` default only when losing the Ratchet job data is intentional.

With `spring.jpa.defer-datasource-initialization=true`, Ratchet runs detected Flyway and Liquibase
migrations before Hibernate so its schema exists when the selected persistence unit initializes.
Those migrations must not seed tables that Hibernate creates. Put that seed data in Boot's deferred
`data.sql` initialization or in an application step that runs after JPA starts. Ratchet leaves a
custom database initializer's existing JPA dependency unchanged.

MySQL supports its default `REPEATABLE_READ` as well as `READ_COMMITTED`; the store validates the selected isolation level when the `JobStore` is created, after schema initialization. To select `READ_COMMITTED`, configure it at the pool or database according to your operating standard, for example:

```properties
spring.datasource.hikari.transaction-isolation=TRANSACTION_READ_COMMITTED
spring.datasource.hikari.data-source-properties.connectionTimeZone=UTC
```

Oracle and SQL Server require UTC JDBC timestamp binding. The starter checks actual timestamp binding through read-only JPA queries on the selected persistence unit, including historical, winter, and summer dates. Set it explicitly as recommended below; otherwise Hibernate uses the JVM default, which must be UTC-equivalent:

```properties
spring.jpa.properties.hibernate.jdbc.time_zone=UTC
```

Ratchet compares these stored timestamps with the database's UTC clock. This setting applies to the selected persistence unit, including application entities, so that unit must use a compatible UTC timestamp policy. The starter leaves this prerequisite under application control and scopes its vendor mappings to Ratchet entities.

## MongoDB

Use the Mongo starter with Boot's usual Mongo configuration:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-spring-boot-starter-mongodb</artifactId>
</dependency>
```

### Keep the MongoDB driver modules aligned

The current MongoDB compatibility baseline requires driver **5.11.1**. Boot 4.1.1
selects 5.8.1 and Boot 3.5.16 selects 5.5.2 by default, so applications using either
Boot BOM or parent must import the official MongoDB driver BOM in their
`dependencyManagement`. It manages `bson`, `bson-record-codec`, `mongodb-driver-core`,
`mongodb-driver-sync`, `mongodb-driver-reactivestreams`, and `mongodb-crypt` together.
The BOM manages versions only and does not add unused driver modules.

Applications that use the Spring Boot parent can add this import:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.mongodb</groupId>
      <artifactId>mongodb-driver-bom</artifactId>
      <version>5.11.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Applications that import `spring-boot-dependencies` instead of using the parent must list
`mongodb-driver-bom` **before** that Boot BOM in the same `dependencyManagement` block. Maven
keeps the first imported management entry for a shared coordinate; reversing the order leaves
Boot's 5.8.1 driver selection in place. Confirm the resolved versions with
`mvn dependency:tree -Dincludes=org.mongodb` after applying application-specific BOMs.

### Connection configuration

Spring Boot 4.1 uses the `spring.mongodb` prefix:

```properties
spring.mongodb.uri=mongodb://localhost:27017/orders
spring.mongodb.representation.uuid=standard
```

Spring Boot 3.5 uses the `spring.data.mongodb` prefix:

```properties
spring.data.mongodb.uri=mongodb://localhost:27017/orders
spring.data.mongodb.uuid-representation=standard
```

No Ratchet-specific Mongo URI, credentials, client, or database name is required. Ratchet uses Boot's `MongoDatabaseFactory`; Mongo submission semantics retain the store's existing atomicity guarantees and do not imply SQL-style participation in an application transaction. Deploy MongoDB as a replica set or sharded cluster: Ratchet uses multi-document transactions for operations such as signals, permits, and batch/workflow state changes.

With `ratchet.schema.auto-migrate=false`, Mongo startup validates the complete declared collection and
named-index shape. Enabled startup can warn and continue when it cannot create an optional
performance index, but that warning does not make an incompatible same-name index valid; repair or
drop the conflicting index before using validation-only startup.

## Configuration and overrides

The Spring-specific properties are:

| Property | Default | Purpose |
| --- | --- | --- |
| `ratchet.enabled` | `true` | Enables Ratchet initialization and workers. |
| `ratchet.schema.auto-migrate` | `true` | Applies SQL migrations or initializes MongoDB collections and indexes; false validates existing storage. |
| `ratchet.shutdown-timeout` | `30s` | Maximum worker drain time during application shutdown. |
| `ratchet.allowed-packages` | Boot application packages | Allows persisted invocation classes. |
| `ratchet.allowed-result-type-packages` | empty | Adds result-materialization packages. |

Existing Ratchet option names and payload formats remain unchanged. A custom `ClassPolicy` or the relevant Ratchet SPI bean takes precedence over the default. Auto-configuration uses `@ConditionalOnMissingBean`, so applications can provide a supported Ratchet SPI or service bean when they need to replace it.

Ratchet starts after persistence initialization and stops before application-owned datasource or Mongo resources close. One active Ratchet runtime is supported per classloader; close a prior application context before starting another in the same test JVM. Jobs should honor interruption: if a job continues beyond the shutdown deadline, Ratchet keeps the runtime ownership fence until that execution exits and rejects a conflicting restart. Spring proxies are invoked through their exposed methods, so transaction advice continues to apply to scheduled and `@Recurring` work.

Install one Ratchet store starter for each scheduler context: use the SQL starter for the selected
JPA-backed SQL store or the MongoDB starter for Boot's `MongoDatabaseFactory`. A context that
needs both database technologies should keep one scheduler and choose the store that owns its
Ratchet jobs. Installing both Ratchet starters in the same context is unsupported; choose one
explicitly.

Keep Ratchet enabled in the context that owns the scheduler; secondary contexts that do not need
their own scheduler can use `ratchet.enabled=false`. Supporting concurrent independent runtimes
requires replacing the shared serializer, encryption, and masking state with dependencies owned
by each runtime and persistence backend. Removing the ownership check alone is unsafe.

With multiple datasources or persistence units, mark a coherent `DataSource`,
`LocalContainerEntityManagerFactoryBean`/`EntityManagerFactory`, and `JpaTransactionManager`
selection as `@Primary`. The selected transaction manager must manage the selected factory,
and that factory must use the selected datasource. A mismatched or ambiguous selection fails
startup with a diagnostic. The Spring SQL integration currently requires `JpaTransactionManager`.

Applications should inject the public scheduling/query services and replace supported SPI beans
through `@Bean` definitions. Direct construction of implementation services requires all explicit
constructor dependencies and the appropriate transaction advice.

Spring receives Ratchet lifecycle events through its application event publisher; use Spring's
`@EventListener` for application observers. Ratchet discovers `@Recurring` methods on Spring beans
and invokes scheduled work through Spring proxies, preserving transaction advice in either proxy
mode. See the shared [annotations](/api-reference/annotations) and
[scheduling](/concepts/scheduling) reference for the job API.

When a `FactoryBean` produces a job target that owns resources, declare `@PreDestroy` on the
product. Factory destroy metadata can suppress a product's plain `DisposableBean.destroy()` or
inferred bare `AutoCloseable.close()`, so neither is guaranteed. For non-singleton pooled products,
use a singleton-scoped `SmartFactoryBean` that reports `isPrototype() == false`. Pooling from a
prototype- or custom-scoped producing factory is unsupported. A `SmartFactoryBean` that returns
independent instances must override `isPrototype()` to return `true`. Ratchet may also apply the
producing factory's destroy metadata, including inferred `close()` or `shutdown()`, to its product.
If that metadata names the product's `@PreDestroy` method, such as `close()` on an `AutoCloseable`
product, the method can run twice. Make overlapping cleanup idempotent.

To capture an authenticated submitter, supply a `PrincipalSource` bean for your application's
security framework. The starter does not automatically install a Spring Security adapter. It
captures the returned principal name; it does not restore a request or authentication context on
the worker. Use `JobAuthorizationPolicy` for application authorization rules and a `ClassPolicy`
bean when the default package allowlist is insufficient.

## JPA provider portability

The SQL starter includes `spring-boot-starter-data-jpa`, so Hibernate is the default provider.
Shared engine and store Java code uses standard JPA/JDBC APIs. Database selection, transaction
participation, persistence-unit metadata, and the UTC timestamp probe do not require Hibernate.
The probe compares a database-formatted timestamp with its expected UTC value; reading a timestamp
back through the same driver could conceal a symmetric timezone shift. It writes no data.

Provider-specific mappings belong to the Spring integration artifact, under
`META-INF/ratchet/hibernate/`. A single optional `HibernateJpaMappings` adapter selects them only
when the selected persistence provider is Hibernate. MySQL and Oracle overrides use standard
Jakarta ORM syntax; SQL Server requires Hibernate 6/7 mapping syntax to keep Ratchet UUIDs in
canonical `BINARY(16)` form without changing application UUID mappings. These exceptions remain
isolated from the shared stores. Boot auto-configuration ordering also names its Hibernate
configuration classes.

An application selecting another provider supplies its own `LocalContainerEntityManagerFactoryBean`
and matching `JpaTransactionManager`; Ratchet uses that provider and the shared database mappings.
The independent consumer reactor has an EclipseLink profile that excludes Hibernate completely.
Its PostgreSQL run exercises the same contracts, packaged application, and process recovery.
That application uses EclipseLink 5 / JPA 3.2 with native `Instant` support and disables
shared entity caching for its unit. Its PostgreSQL datasource also uses `stringtype=unspecified`
for native-query null binding, matching the Jakarta EE portability suite. Those settings are
application-owned. Ratchet does not require weaving: batch metrics map their foreign-key
identifier directly and do not traverse a job relationship. Persistence metadata uses class names
without loading the entities, leaving optional class enhancement to the application and provider.
That coverage does not establish support for every provider/database combination. In particular,
the existing non-default binary UUID mappings use identifier converters whose behavior varies
between providers; they are not a blanket portability guarantee.

## Virtual threads and Java versions

The same artifacts run on Java 17 and Java 21. Ratchet follows Spring Boot's virtual-thread
setting:

```properties
spring.threads.virtual.enabled=true
```

On Java 21+, this enables Ratchet's virtual pool and makes it the default for jobs without an
explicit target. When the property is false or absent, or when running on Java 17, Ratchet uses
platform threads. This matches Boot's Java-version gating; no separate Ratchet enable flag is
needed.

To keep Ratchet's default on platform threads while virtual threads are enabled application-wide,
set `ratchet.worker.default-threading-mode=platform`. Use `.virtual()` or `.platform()` on a job
builder to select its target explicitly. With no virtual pool configured, `.virtual()` falls back
to the platform pool. An application-provided `RatchetOptions` bean retains its explicit defaults.

Ratchet owns its executors for job cancellation and runtime shutdown; it does not borrow Boot's
shared application executor. Ratchet's concurrency limits still apply. Application-provided
`ExecutorProvider` beans control their own executor behavior and must supply the `virtual` target
when that pool is enabled. `ratchet.worker.virtual-executor-jndi` configures a managed executor
in Jakarta EE and does not enable Spring's virtual pool.

The Spring consumer CI covers both Java versions and both Boot versions. Its Java 21 SQL tests
verify actual virtual threads, explicit platform routing, transaction advice, and committed
application writes using only Boot's setting. Java 17 runs verify platform execution and fallback
with the same setting.

## Native images

The native CI matrix targets Boot 3.5.16 and 4.1.1 on GraalVM 25 for all five stores.
Ratchet's generated JVM AOT and native-image SQL metadata both use Hibernate. Install exactly one
Ratchet SQL store artifact when running `process-aot`, building a native image, or enabling
`spring.aot.enabled`; the executable still validates the actual database vendor at startup.
EclipseLink remains supported for ordinary JVM deployments, but EclipseLink generated JVM AOT and
native images are outside this support matrix.

If a MongoDB application also uses JPA, application-owned ORM XML mappings require the
application to supply Hibernate/JAXB native hints. The MongoDB starter does not supply those
provider-specific hints.

Spring AOT discovers application package classes and registered application bean types, including
nested, local, and anonymous lambda submitters. It includes target reflection, payload binding,
lambda serialization metadata, and the class resources Ratchet's lambda analyzer reads.
Ordinary application jobs need no registration annotation. Classes whose optional dependencies are absent are skipped during package scanning; missing dependencies of registered beans or explicitly registered types still fail the build. Dependency-library types can opt in:

```java
import run.ratchet.spring.boot.autoconfigure.RegisterRatchetTypes;

@Configuration(proxyBeanMethods = false)
@RegisterRatchetTypes(value = {LibrarySubmitter.class, LibraryPayload.class},
    basePackageClasses = LibraryJobsPackage.class)
class LibraryJobsConfiguration {}
```

Registration does not change `ClassPolicy`. Authorize library job packages separately with
`ratchet.allowed-packages` or a policy bean. Result deserialization authorization remains separate.
The marker includes its package and subpackages; explicit classes also include their nested types.

Supply profiles and conditions selecting beans during Boot's `process-aot` phase. Database
credentials, encryption keys, and job parameters remain runtime configuration. AOT does not connect
to a database, migrate schemas, start workers, or instantiate lazy/prototype jobs. Rebuild from
`clean` when changing stores, profiles, framework versions, or available job types.

The [independent consumer reactor](https://github.com/ratchet-run/ratchet/blob/main/integrations/ratchet-spring-boot/consumer-tests/README.md#aot-and-native-verification)
has `aot` and `native` profiles. After staging current artifacts, each native cell runs through
one `mvn -Pnative clean verify` invocation with the selected Boot version and store. The native
profile explicitly binds AOT, reachability metadata, compilation, and executable integration tests;
applications that do not inherit Boot's parent need these bindings too. Testcontainers and test
assertions remain in the JVM process.

## Compatibility and verification

| Runtime | Java | Databases | Execution |
| --- | --- | --- | --- |
| Boot 3.5.16 | 17 and 21 | PostgreSQL, MySQL, Oracle, SQL Server, MongoDB | JVM |
| Boot 4.1.1 | 17 and 21 | PostgreSQL, MySQL, Oracle, SQL Server, MongoDB | JVM |
| Boot 3.5.16 and 4.1.1 | GraalVM 25 | PostgreSQL, MySQL, Oracle, SQL Server, MongoDB | Native executable CI matrix |
| EclipseLink 5.0.1 / JPA 3.2, both Boot versions | 17 and 21 | PostgreSQL | JVM; Hibernate excluded |

MongoDB entries in this matrix select driver **5.11.1**, using the required
[MongoDB BOM configuration](#keep-the-mongodb-driver-modules-aligned) above; they do not qualify
the older drivers selected by the Boot BOMs.

The independent consumer reactor imports its own Boot BOM and resolves the same staged Ratchet
artifacts for every combination. Each full `verify` includes real databases, applicable API/store
contracts, an executable Boot jar in a separate JVM, and recovery after a forced process exit.
SQL checks also cover commit/rollback, transaction propagation, host entities and converters,
non-UTC operation, schema/migration ordering, SQL Server UUIDs, and virtual/platform thread routing.

Each run retains four explicit existing contract skips: three require a controllable clock, and
one tests an unsupported CRUD stale-write fixture contract. Real-clock delay checks remain enabled.
These consumer results are separate from the Jakarta Runtime conformance matrix; they do not
claim CDI/JTA conformance for Spring.

The [consumer README](https://github.com/ratchet-run/ratchet/blob/main/integrations/ratchet-spring-boot/consumer-tests/README.md)
contains reproducible Maven commands. The
[CI workflow](https://github.com/ratchet-run/ratchet/blob/main/.github/workflows/ci.yml)
runs the matrix and retains test reports.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| SQL AOT reports no store or multiple installed stores | Include exactly one SQL store artifact when generating AOT metadata. For ordinary JVM use, install the store matching the selected datasource; use one Ratchet store starter per context. |
| Ambiguous persistence beans | Select a coherent datasource, entity-manager factory, and JPA transaction manager with `@Primary`. |
| MySQL startup rejects isolation | Configure `TRANSACTION_READ_COMMITTED` on the selected pool. |
| Oracle or SQL Server rejects timestamp binding | Configure UTC JDBC timestamp binding for the selected unit; with Hibernate, use `hibernate.jdbc.time_zone=UTC`. |
| Schema validation fails with auto-migration disabled | Apply every supplied Ratchet migration. If the migration ledger exists, keep all versions and checksums consistent. |
| Job target rejected by policy | Put targets under the Boot application package or set `ratchet.allowed-packages` / provide `ClassPolicy`. |
| Standalone application exits after submission | Set `spring.main.keep-alive=true` so the JVM stays alive to execute background jobs. |
| Another runtime already owns the classloader | Close the previous context and let its jobs terminate, or disable Ratchet in secondary contexts. |
| Java 21 jobs still use platform threads | Enable Boot's virtual-thread property and check explicit Ratchet threading defaults or a custom `ExecutorProvider`. |
