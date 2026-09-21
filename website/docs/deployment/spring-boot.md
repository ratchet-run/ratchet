---
title: Spring Boot
description: Run persistent Ratchet jobs in Spring Boot 3.5 and 4.1 with SQL or MongoDB, Java 17 or 21, application transactions, and optional virtual threads.
---

# Spring Boot

The SQL and MongoDB starters are published to Maven Central with the other Ratchet modules,
starting with Ratchet **0.4.0**. Import the Ratchet BOM to keep their versions aligned.

Ratchet runs in Spring Boot 3.5 and 4.1 applications on Java 17 or later. The SQL starter uses the application's existing datasource, entity-manager factory, and transaction manager. The MongoDB starter uses Boot's configured Mongo client and database factory.

## SQL quickstart

Start with a normal Spring Boot application and its Boot parent or BOM. The tested versions are
**3.5.16** and **4.1.1**. Set the Ratchet version in your application's Maven properties:

```xml
<properties>
  <java.version>17</java.version>
  <ratchet.version>0.4.0</ratchet.version>
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
mvn spring-boot:run
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

That mode validates compatibility without changing the schema. An externally managed schema may omit Ratchet's migration ledger; if the ledger exists, it must record every bundled migration with its matching checksum. `ratchet.enabled=false` disables migrations, discovery, workers, and Ratchet runtime installation.

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

Spring Boot 3.5 uses the `spring.data.mongodb` prefix:

```properties
spring.data.mongodb.uri=mongodb://localhost:27017/orders
spring.data.mongodb.uuid-representation=standard
```

Spring Boot 4.1 uses the renamed `spring.mongodb` prefix:

```properties
spring.mongodb.uri=mongodb://localhost:27017/orders
spring.mongodb.representation.uuid=standard
```

No Ratchet-specific Mongo URI, credentials, client, or database name is required. Ratchet uses Boot's `MongoDatabaseFactory`; Mongo submission semantics retain the store's existing atomicity guarantees and do not imply SQL-style participation in an application transaction. Deploy MongoDB as a replica set or sharded cluster: Ratchet uses multi-document transactions for operations such as signals, permits, and batch/workflow state changes.

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

Native support is verified with Boot 3.5.16 and 4.1.1 on GraalVM 25 for all five stores.
SQL native images use Hibernate. Install exactly one Ratchet SQL store artifact when building;
the executable validates the actual database vendor at startup. EclipseLink native images are
outside this support matrix.

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
| No store or multiple stores selected | Use the SQL starter with exactly one SQL store, or use the MongoDB starter. |
| Ambiguous persistence beans | Select a coherent datasource, entity-manager factory, and JPA transaction manager with `@Primary`. |
| MySQL startup rejects isolation | Configure `TRANSACTION_READ_COMMITTED` on the selected pool. |
| Oracle or SQL Server rejects timestamp binding | Configure UTC JDBC timestamp binding for the selected unit; with Hibernate, use `hibernate.jdbc.time_zone=UTC`. |
| Schema validation fails with auto-migration disabled | Apply every supplied Ratchet migration. If the migration ledger exists, keep all versions and checksums consistent. |
| Job target rejected by policy | Put targets under the Boot application package or set `ratchet.allowed-packages` / provide `ClassPolicy`. |
| Standalone application exits after submission | Set `spring.main.keep-alive=true` so the JVM stays alive to execute background jobs. |
| Another runtime already owns the classloader | Close the previous context and let its jobs terminate, or disable Ratchet in secondary contexts. |
| Java 21 jobs still use platform threads | Enable Boot's virtual-thread property and check explicit Ratchet threading defaults or a custom `ExecutorProvider`. |
