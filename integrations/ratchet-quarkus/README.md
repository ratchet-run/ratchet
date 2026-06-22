# Ratchet Quarkus extension

Run the Ratchet job scheduler on Quarkus, on the JVM and as a GraalVM native image.

The extension wires Ratchet into Quarkus so an application adds a few dependencies and some
persistence config, then submits jobs through the usual `JobSchedulerService`. It handles the parts
of Ratchet that assume a Jakarta EE server: it starts the engine once the runtime is ready, supplies
a JNDI-free executor, binds the stores to their own persistence unit, and registers the reflection
and serialization metadata a native image needs.

> **Status:** validated on the JVM and as a native image, but not yet published to Maven Central.
> Build it from source until a release is cut:
> `mvn -f integrations/ratchet-quarkus/pom.xml install`

## Requirements

- Quarkus 3.20 or later (built against 3.20.0)
- JDK 21 for JVM mode; a GraalVM or Mandrel distribution for native
- A SQL store, PostgreSQL or MySQL. This is the Hibernate ORM cell. EclipseLink is a Jakarta EE
  feature and does not apply on Quarkus.

## Dependencies

Add the extension, a store, and the matching Quarkus JDBC driver.

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-quarkus</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-jdbc-postgresql</artifactId>
</dependency>
<!-- The engine references jakarta.security.enterprise.SecurityContext. Add the API until the
     extension declares it transitively. -->
<dependency>
  <groupId>jakarta.security.enterprise</groupId>
  <artifactId>jakarta.security.enterprise-api</artifactId>
</dependency>
```

For MySQL, swap in `ratchet-store-mysql` and `quarkus-jdbc-mysql`.

## Configuration

Ratchet runs on its own persistence unit, named `ratchet`, so its entities and schema settings stay
out of your application's default unit. Put this in `application.properties`:

```properties
# Your datasource. Ratchet rides on it too.
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=ratchet
quarkus.datasource.password=ratchet
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/ratchet

# Ratchet's persistence unit.
quarkus.hibernate-orm."ratchet".packages=run.ratchet.store.entity
quarkus.hibernate-orm."ratchet".datasource=<default>
quarkus.hibernate-orm."ratchet".database.generation=none
```

Three things about that block are easy to get wrong:

- `datasource=<default>` is required. A named persistence unit does not fall back to the default
  datasource on its own, and the build fails without it. `<default>` is the literal token for the
  default datasource; name a different datasource here if you want Ratchet on a separate database.
- `database.generation=none` because Ratchet ships its own schema (see [Schema](#schema)). Hibernate
  leaves the tables alone.
- The packages line scopes the unit to Ratchet's entities. Leave it as written.

### When your application has its own entities

A named persistence unit switches off Quarkus's automatic default unit, so you declare yours
explicitly and keep Ratchet's mapping file out of it:

```properties
quarkus.hibernate-orm.packages=com.example.myapp
quarkus.hibernate-orm.mapping-files=no-file
```

The `mapping-files=no-file` line is the important one. Ratchet ships a `META-INF/orm.xml`, and Quarkus
attaches it to every persistence unit by default. Without `no-file`, your default unit also picks up
Ratchet's entities, and a default unit set to `drop-and-create` will drop Ratchet's tables. `no-file`
keeps that mapping on Ratchet's unit alone.

If your application has no entities of its own, you can skip this section. The `ratchet` unit is the
only one, and there is no default unit to protect.

## Schema

Ratchet does not create its tables, which is what `generation=none` above means. Apply the DDL before
the first run. Each store ships a consolidated script and per-version migrations under
`src/main/resources/ddl/`, for example `ddl/postgresql-schema.sql`. Apply it with whatever you
already use: Flyway, Liquibase, or plain `psql`.

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
