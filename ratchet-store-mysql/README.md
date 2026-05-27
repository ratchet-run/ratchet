# Ratchet MySQL store

MySQL 8.0+ persistence implementation for the Ratchet scheduler.

## Schema

Authoritative DDL lives under `src/main/resources/ddl/`:

- `mysql-schema.sql` — clean-install schema applied by integration tests via Testcontainers.
- `migrations/V###__*.sql` — ordered upgrade scripts tracked through `ratchet_schema_version` by external migration tooling (or the opt-in `SchemaMigrator` utility).
- `views/vw_jobs.sql` — operator-only views (see below). Not loaded by the application or tests.

## Operator debugging

UUIDv7 IDs are stored as `BINARY(16)`. Raw `SELECT * FROM scheduler_job` returns 16-byte binary values that display as control characters in `mysql` CLI clients.

Use the read-only views shipped in `ddl/views/vw_jobs.sql`:

```sql
SELECT * FROM vw_jobs WHERE business_key = 'foo';
SELECT * FROM vw_job_queue WHERE status = 'PENDING';
SELECT * FROM vw_job_execution WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654';
```

The views call `BIN_TO_UUID(col)` (no swap flag). The store writes UUIDs in standard byte order, so reading without the flag round-trips correctly with Java's `UUID.toString()`. Passing `BIN_TO_UUID(col, 1)` would apply MySQL's v1-time-reorder swap on read and produce values that do not match any stored row.

Apply the views once after schema load:

```bash
mysql -u <user> -p ratchet < ratchet-store-mysql/src/main/resources/ddl/views/vw_jobs.sql
```

Tools that handle binary IDs correctly (Hibernate, DataGrip's UUID-aware viewer, JDBC `getObject(..., UUID.class)`) can query the underlying tables directly.

## JPA mapping (production wiring)

How you wire `ratchet-store-mysql` into a `persistence.xml` depends on the JPA provider, because UUID columns are stored as `BINARY(16)`. Give Ratchet its own persistence unit — its entity list is self-contained (`<exclude-unlisted-classes>true</exclude-unlisted-classes>`), and the `RatchetEntityManagerProvider` SPI lets a multi-unit application point Ratchet at it. Keeping Ratchet in its own unit means none of the provider settings below can touch your application's own entities or their columns.

**EclipseLink, OpenJPA (any non-Hibernate provider):** reference `META-INF/orm-mysql.xml` so UUID columns route through `UuidByteArrayConverter`. These providers default to a 36-character hyphenated representation that overflows `BINARY(16)` with MySQL strict-mode error 1406 ("Data too long for column"); the mapping file forces the byte representation.

```xml
<persistence-unit name="ratchet">
  <jta-data-source>java:/jdbc/MyDS</jta-data-source>
  <mapping-file>META-INF/orm-mysql.xml</mapping-file>
  <class>run.ratchet.store.entity.JobEntity</class>
  ...
</persistence-unit>
```

**Hibernate (ORM 6 or 7):** do NOT reference `orm-mysql.xml`. Hibernate maps `UUID` to `BINARY(16)` natively on MySQL by default (MySQL has no native UUID type), and Hibernate 7 rejects an `AttributeConverter` on an `@Id` attribute (`org.hibernate.AnnotationException`) — so referencing the mapping file would fail deployment. Use no mapping file at all:

```xml
<persistence-unit name="ratchet">
  <jta-data-source>java:/jdbc/MyDS</jta-data-source>
  <class>run.ratchet.store.entity.JobEntity</class>
  ...
</persistence-unit>
```

PostgreSQL's `ratchet-store-postgresql` does NOT need a mapping file — PostgreSQL's native `uuid` column type round-trips `java.util.UUID` directly through JDBC.

## Storage characteristics

UUIDv7 PKs add ~8 bytes per row over the previous 8-byte BIGINT. At 100M rows with ~5 secondary indexes the delta is roughly **4.8 GB additional index space**. Bounded but worth budgeting for.
