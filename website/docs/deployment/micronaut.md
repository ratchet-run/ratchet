---
title: Micronaut
description: Run persistent Ratchet jobs in Micronaut 4.10 and 5 with SQL or MongoDB, using the application's datasource, Hibernate session factory, and transactions.
---

# Micronaut

Ratchet runs in Micronaut applications. One set of integration jars supports two lines:

| Micronaut | Java | Hibernate | Notes |
| --- | --- | --- | --- |
| 4.10.x | 17 or later | 6.6 | The jars are compiled against 4.10. |
| 5.x | 25 or later | 7.x | Micronaut 5 itself needs Java 25 to build and run. |

The SQL integration uses the application's Micronaut datasource, Hibernate session factory, and transaction manager. The MongoDB integration uses the application's `micronaut-mongo-sync` client.

## SQL quickstart

### Add the dependencies

Import the Ratchet BOM next to your Micronaut platform, then add the core integration, the JPA integration, exactly one Ratchet SQL store, Micronaut Hibernate JPA, a connection pool, and the JDBC driver. For PostgreSQL:

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
    <artifactId>ratchet-micronaut</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-micronaut-jpa</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>
  <dependency>
    <groupId>io.micronaut.sql</groupId>
    <artifactId>micronaut-hibernate-jpa</artifactId>
  </dependency>
  <dependency>
    <groupId>io.micronaut.sql</groupId>
    <artifactId>micronaut-jdbc-hikari</artifactId>
  </dependency>
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

The other SQL stores are `ratchet-store-mysql`, `ratchet-store-oracle`, and `ratchet-store-sqlserver`.

### Configure

```yaml
datasources:
  default:
    url: jdbc:postgresql://localhost:5432/app
    username: app
    password: secret

jpa:
  default:
    entity-scan:
      packages:
        - com.example

ratchet:
  allowed-packages: com.example
```

`ratchet.allowed-packages` lists the packages that may contain job targets. Ratchet refuses to start without it, unless you supply your own `ClassPolicy` bean.

Ratchet registers its own entities, converters, and vendor mapping on the selected persistence unit. You do not add Ratchet packages to `entity-scan`.

To use a datasource other than `default`, set `ratchet.jpa.datasource` to its name. Ratchet then uses that datasource, its session factory, and its transaction manager.

### Submit a first job

Inject `JobSchedulerService` and submit a method reference:

```java
@Singleton
public class InvoiceService {
  private final JobSchedulerService scheduler;

  public InvoiceService(JobSchedulerService scheduler) {
    this.scheduler = scheduler;
  }

  @Transactional
  public void issue(String invoiceId) {
    // ... save the invoice ...
    scheduler.enqueue(() -> send(invoiceId)).submit();
  }

  public void send(String invoiceId) {
    // runs later, on a Ratchet worker
  }
}
```

The job row commits or rolls back with the surrounding transaction. `@Recurring` methods on Micronaut beans are discovered at startup.

## Transactions and schema

Ratchet's engine declares its transaction rules with `jakarta.transaction.Transactional`. The integration applies them through Micronaut's transaction manager with the same meaning they have under JTA:

- `REQUIRED` joins a real transaction, or starts one. This holds even when the caller runs under `NOT_SUPPORTED` or `SUPPORTS`, where Micronaut Data on its own would join the empty scope.
- `REQUIRES_NEW` suspends the caller and commits on its own.
- `MANDATORY` fails when no real transaction is active.

After-commit work, such as waking the poller, runs only after the surrounding transaction commits.

Schema setup is off by default. Set `RATCHET_SCHEMA_AUTO_MIGRATE=true` to let Ratchet create or migrate its tables before Hibernate starts. Otherwise Ratchet validates the schema and fails fast if it is missing or old. See [Database setup](/deployment/database-setup).

## MongoDB

Add the core integration, the MongoDB integration, the Ratchet MongoDB store, and Micronaut's synchronous Mongo client:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micronaut</artifactId>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micronaut-mongodb</artifactId>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-mongodb</artifactId>
</dependency>
<dependency>
  <groupId>io.micronaut.mongodb</groupId>
  <artifactId>micronaut-mongo-sync</artifactId>
</dependency>
```

```yaml
mongodb:
  uri: mongodb://localhost:27017/app

ratchet:
  allowed-packages: com.example
```

Ratchet uses the database named in `ratchet.mongodb.database`, or else the database in `mongodb.uri`.

Ratchet's MongoDB store is built against MongoDB Java driver 5.11. Pin `bson`, `mongodb-driver-core`, and `mongodb-driver-sync` to that version in `dependencyManagement`, after the Micronaut platform import. Ratchet's own test lanes do this on both Micronaut lines.

## Metrics and tracing

With `micronaut-micrometer` and a `MeterRegistry` bean present, Ratchet records metrics through Micrometer. With a Micrometer `Tracer` present, it records spans. Otherwise it uses no-op collectors.

## Configuration and overrides

Every Ratchet bean is a default. Define your own bean of the same type and Ratchet does not create its own. For example, a custom `ClassPolicy`, `ExecutorProvider`, `PayloadSerializer`, or `MetricsCollector` replaces Ratchet's.

Set `ratchet.enabled=false` to switch the integration off.

`ratchet.shutdown-timeout` bounds how long shutdown waits for running jobs before the datasource closes.

The shared settings, such as poll intervals and retry defaults, are listed in the [configuration reference](/deployment/configuration-reference).

## Things that differ from other runtimes

- **Converters and mapping files.** Micronaut's entity scan only registers `@Entity` classes. An application `@Converter(autoApply = true)` is never found: name it on the attribute with `@Convert(converter = ...)`. Micronaut also never loads `META-INF/orm.xml`. List your mapping files under `jpa.<name>.mapping-resources`. Ratchet's own entities and mappings need neither.
- **Failed startup in tests.** If an `ApplicationContext` fails to start, Micronaut does not destroy the beans it created. Ratchet detects this and lets the next context on the same thread start normally.
- **Native image.** Not supported yet.
- **`@CircuitBreakerProtected` on application beans.** Not supported, as on Spring Boot. Breakers configured on jobs work as usual.

## Compatibility and verification

Each release is tested in two lanes against the same Ratchet jars:

- Micronaut 4.10 on Java 17 with Hibernate 6.6.
- Micronaut 5 on Java 25 with Hibernate 7. The test application is compiled with Micronaut 5's annotation processors, so it also proves that 4.10-built Ratchet beans work inside a Micronaut 5 application, including bean replacement and qualifiers.

Both lanes run the store and API conformance suites against PostgreSQL, MySQL, Oracle, SQL Server, and MongoDB in containers.
