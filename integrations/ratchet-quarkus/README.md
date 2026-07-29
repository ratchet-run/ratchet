# Ratchet Quarkus extension

Run the Ratchet job scheduler on Quarkus, on the JVM and as a GraalVM native image.

The extension wires Ratchet into Quarkus so an application adds a few dependencies and a datasource,
then submits jobs through the usual `JobSchedulerService`. Ratchet's own persistence unit is
supplied by the extension, so there is no persistence-unit configuration to write. It handles the
parts of Ratchet that assume a Jakarta EE server: it starts the engine once the runtime is ready,
supplies a JNDI-free executor, binds the stores to their own persistence unit, and registers the
reflection and serialization metadata a native image needs.

> **Status:** validated on the JVM and as a native image, but not yet published to Maven Central.
> Build it from source until a release is cut:
> `mvn -f integrations/ratchet-quarkus/pom.xml install`

## Requirements

- Quarkus 3.20 or later (built against 3.20.0)
- JDK 21 for JVM mode; a GraalVM or Mandrel distribution for native
- A store. For the SQL flavor: PostgreSQL, MySQL, Oracle, or SQL Server, where the JPA provider is
  Hibernate ORM (EclipseLink is a Jakarta EE feature and does not apply on Quarkus). For NoSQL, the
  separate `ratchet-quarkus-mongodb` flavor runs on MongoDB with no JPA provider at all.

## Dependencies

Add the extension, a store, and the matching Quarkus JDBC driver.

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-quarkus</artifactId>
  <version>0.2.2-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
  <version>0.2.2-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-jdbc-postgresql</artifactId>
</dependency>
```

For MySQL, swap in `ratchet-store-mysql` and `quarkus-jdbc-mysql`. When a request has a Quarkus
`SecurityIdentity`, Ratchet captures that principal automatically at job submission.

## Configuration

Ratchet runs on its own extension-supplied persistence unit, named `ratchet`, so its entities and
schema settings stay out of your application's default unit. Configure your datasource as usual:

```properties
# Your datasource. Ratchet rides on it too.
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=ratchet
quarkus.datasource.password=ratchet
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/ratchet
```

For SQL Server, add the Hibernate overrides below on the `ratchet` persistence unit:

```properties
quarkus.hibernate-orm."ratchet".unsupported-properties."hibernate.type.preferred_uuid_jdbc_type"=BINARY
quarkus.hibernate-orm."ratchet".unsupported-properties."hibernate.type.preferred_instant_jdbc_type"=TIMESTAMP
```

The UUID override is required because Ratchet's SQL Server schema stores UUIDs as canonical
`BINARY(16)`, while Hibernate's SQL Server dialect otherwise binds UUIDs through SQL Server
`UNIQUEIDENTIFIER` semantics. The byte orders differ, so JPA-managed rows can fail foreign-key checks
against rows written by Ratchet's native SQL. The Instant override matches SQL Server's zoneless
`DATETIME2(6)` columns.

The extension binds the `ratchet` unit to the default datasource, scopes it to Ratchet's entities,
sets `database.generation=none`, and keeps Ratchet's `META-INF/orm.xml` off the default unit.

### When your application has its own entities

A named persistence unit switches off Quarkus's automatic default unit, so you still declare yours
explicitly:

```properties
quarkus.hibernate-orm.packages=com.example.myapp
quarkus.hibernate-orm.database.generation=drop-and-create
```

Ratchet's extension defaults keep its mapping file on the `ratchet` unit only, so your default unit
does not need a defensive `mapping-files=no-file` line.

If your application has no entities of its own, you can skip this section. The `ratchet` unit is the
only one, and there is no default unit to protect.

## Schema

Ratchet does not create its tables by default. You have two options.

For development, CI, and embedded use, set `ratchet.schema.auto-migrate=true` and Ratchet applies
its bundled migrations on startup — this is what `examples/quarkus` does, and it works in native
images as well:

```properties
ratchet.schema.auto-migrate=true
```

For production, many teams prefer to apply schema changes out of band and leave `auto-migrate` off.
Each store ships a consolidated script and per-version migrations under `src/main/resources/ddl/`,
for example `ddl/postgresql-schema.sql`. Apply it with whatever you already use: Flyway, Liquibase,
or plain `psql`.

## Submitting jobs

Inject `JobSchedulerService` and submit a method reference:

```java
@ApplicationScoped
public class Reports {
  public void rebuild() { /* ... */ }
}

@Path("/reports")
public class ReportResource {
  @Inject JobSchedulerService scheduler;
  @Inject Reports reports;

  @POST
  public void schedule() {
    scheduler.enqueueNow(reports::rebuild);
  }
}
```

Recurring jobs, signals, batches, and the rest of the API behave the same as on a Jakarta EE server.

## Native image

```
mvn package -Pnative -Dquarkus.native.container-build=false
```

Build on a host GraalVM or Mandrel. The extension registers the reflection, runtime-init, and
lambda-serialization metadata Ratchet needs, so method-reference jobs run in native. Inline lambdas
such as `() -> svc.work(arg)` are JVM-only; for a job you want to run in native, pass a method
reference or a bean method instead.

## What the extension handles for you

- Starts the engine on Quarkus's `StartupEvent`, after persistence is ready. Ratchet's Jakarta EE
  auto-start fires at static-init, which is too early on Quarkus.
- Supplies the JNDI-free `StandaloneExecutorProvider`, since Quarkus has no Jakarta Concurrency
  managed executor.
- Binds the stores to the `ratchet` persistence unit.
- Keeps Ratchet's beans from being pruned by ArC, and registers the native metadata (the UUID entity
  listener, the payload record, the UUID factory, and lambda-capturing submitters).

## Known limitations

- Snapshot only. Not yet on Maven Central.
- EclipseLink is not available on Quarkus. This is the Hibernate ORM cell.
- The native lambda-capturing scan keys off classes that inject `JobSchedulerService`. If you submit
  jobs from a class that does not inject it directly, register that class for serialization yourself.
