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

Ratchet creates or migrates its tables by default, before Hibernate starts. To manage the schema with your own migration tool, apply Ratchet's supplied migrations and set `ratchet.schema.auto-migrate=false`. Ratchet then only validates the schema, and fails fast if it is missing or out of date. See [Database setup](/deployment/database-setup).

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

## Circuit breakers on application beans

To use `@CircuitBreakerProtected` on Micronaut application beans, add
`ratchet-micronaut-processor` to your application's annotation processor path after the Micronaut
processors. It supplies the compile-time AOP binding for method and class annotations. The same
processor artifact supports Micronaut 4.10 and 5.x.

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-compiler-plugin</artifactId>
  <configuration>
    <annotationProcessorPaths>
      <!-- Keep your Micronaut inject and data processor entries here. -->
      <path>
        <groupId>run.ratchet</groupId>
        <artifactId>ratchet-micronaut-processor</artifactId>
        <version>${ratchet.version}</version>
      </path>
    </annotationProcessorPaths>
  </configuration>
</plugin>
```

An open circuit rejects the call before transaction advice runs. Protection passes through when
circuit breakers are disabled or Ratchet's breaker policy beans are absent. Breakers configured on
job methods work through the scheduler's resilience strategy without this processor. When a job
method also has application bean advice, both wrappers apply, matching CDI behavior.

Micronaut does not pass class-level advice from a superclass to its subclasses. Under CDI, a
subclass of an annotated class is protected too. On Micronaut, repeat the annotation on the
subclass.

## Things that differ from other runtimes

- **Converters and mapping files.** Micronaut's entity scan only registers `@Entity` classes. An application `@Converter(autoApply = true)` is never found: name it on the attribute with `@Convert(converter = ...)`. Micronaut also never loads `META-INF/orm.xml`. List your mapping files under `jpa.<name>.mapping-resources`. Ratchet's own entities and mappings need neither.
- **Failed startup in tests.** If an `ApplicationContext` fails to start, Micronaut does not destroy the beans it created. Ratchet detects this and lets the next context on the same thread start normally.
## Native image

Ratchet's jars activate their native Features automatically. At build time, the Features register
the engine, the selected store, and your application's job submitters and targets. Job code needs
no extra annotations for this discovery.

Build with GraalVM 25. Configure the application plugin with the reachability metadata repository.
It supplies native registrations for third-party libraries.

```xml
<plugin>
  <groupId>org.graalvm.buildtools</groupId>
  <artifactId>native-maven-plugin</artifactId>
  <version>1.1.8</version>
  <configuration>
    <mainClass>example.Application</mainClass>
    <classesDirectory>${project.build.outputDirectory}</classesDirectory>
    <metadataRepository><enabled>true</enabled></metadataRepository>
    <buildArgs>
      <buildArg>--no-fallback</buildArg>
      <buildArg>-H:+ReportExceptionStackTraces</buildArg>
      <!-- SQL applications only -->
      <buildArg>-H:+UnlockExperimentalVMOptions</buildArg>
      <buildArg>-H:ServiceLoaderFeatureExcludeServices=org.hibernate.bytecode.spi.BytecodeProvider</buildArg>
      <!-- SQL Server applications also need -H:+AddAllCharsets. -->
    </buildArgs>
  </configuration>
  <executions>
    <execution>
      <id>add-reachability-metadata</id>
      <phase>process-classes</phase>
      <goals><goal>add-reachability-metadata</goal></goals>
    </execution>
    <execution>
      <id>native-package</id>
      <phase>package</phase>
      <goals><goal>compile-no-fork</goal></goals>
    </execution>
  </executions>
</plugin>
```

`--no-fallback` requires a real native executable. The stack-trace option helps diagnose build
failures. SQL applications exclude Hibernate's bytecode provider service. Hibernate 6.6 and 7
otherwise select Byte Buddy, which cannot generate classes inside a native executable. With the
service excluded, Hibernate uses its no-op provider. The unlock option enables this experimental
GraalVM option. SQL Server also needs `-H:+AddAllCharsets` for its JDBC driver's character sets.

Your application owns reflection metadata for its entities, embeddables, converters, and value
types. This includes types listed in mapping XML. Put metadata under
`src/main/resources/META-INF/native-image/<group>/<artifact>/reachability-metadata.json`:

```json
{
  "reflection": [
    {
      "type": "example.ApplicationEntity",
      "allDeclaredConstructors": true,
      "allDeclaredMethods": true,
      "allDeclaredFields": true
    },
    {
      "type": "example.ApplicationConverter",
      "allDeclaredConstructors": true,
      "allDeclaredMethods": true,
      "allDeclaredFields": true
    }
  ],
  "resources": [
    {"glob": "application*.yml"},
    {"glob": "META-INF/application-orm.xml"}
  ]
}
```

The `application*.yml` entry matters if you build without the Micronaut Maven plugin. Without it,
the native executable has no configuration file and starts with defaults.

Repeat the reflection entry for each application type that persistence needs. Include your mapping
resources and list them in `jpa.<name>.mapping-resources`. Ratchet registers its own persistence
types and mappings. Mongo applications may need metadata for their own serialized job arguments.

Ratchet builds objects created inside the submitting lambda, such as
`() -> jobs.order(new Order(customerId))`, when you call `submit()`. In a native executable, each
such type needs a reachable constructor. Ratchet's Feature registers the constructors it finds in
your lambdas, but a JDK or library type may need its own reachability metadata.
A command-line app without an HTTP server also needs a non-daemon thread to keep it alive while
jobs run. Ratchet's worker threads are daemons. Micronaut registers a shutdown hook only when an
embedded server runs, so such an app must also close the context on SIGTERM. Otherwise Ratchet
never drains in-flight jobs:

```java
ApplicationContext context = Micronaut.run(Application.class, args);
Runtime.getRuntime().addShutdownHook(new Thread(context::close));
```

## Compatibility and verification

Each release is tested in two lanes against the same Ratchet jars:

- Micronaut 4.10 on Java 17 with Hibernate 6.6.
- Micronaut 5 on Java 25 with Hibernate 7. The test application is compiled with Micronaut 5's annotation processors, so it also proves that 4.10-built Ratchet beans work inside a Micronaut 5 application, including bean replacement and qualifiers.

Both lanes run the store and API conformance suites against PostgreSQL, MySQL, Oracle, SQL Server, and MongoDB in containers.

The independent consumer reactor also defines a native CI matrix for both lanes and all five stores
on GraalVM 25. Its process ITs run against either application classes with runtime dependencies or
the native executable. They check job discovery, captured arguments, recurring work, SQL transaction
and circuit advice, SIGTERM drain, crash recovery, and SQL migration from an empty database. See the
consumer harness README at `integrations/ratchet-micronaut/consumer-tests/README.md`
for commands. Native source changes require successful CI or local builds before runtime support is
considered verified.
