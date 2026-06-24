# Ratchet Oracle store

Oracle Database 23ai+ persistence implementation for the Ratchet scheduler.

## Schema

Authoritative DDL lives under `src/main/resources/ddl/`:

- `oracle-schema.sql` — clean-install schema applied by integration tests via Testcontainers.
- `migrations/V###__*.sql` — ordered upgrade scripts tracked through `ratchet_schema_version` by external migration tooling (or the opt-in `SchemaMigrator` utility). The Oracle migrator holds its lock on a second pooled connection, so the DataSource pool maximum must be at least 2.
- `views/vw_jobs.sql` — operator-only views (see below). Not loaded by the application or tests.

## Operator debugging

UUIDv7 IDs are stored as `RAW(16)`. Raw `SELECT * FROM scheduler_job` returns 16-byte values that display as 32-character hex (not hyphenated UUIDs) in `sqlplus`/SQLcl clients.

Use the read-only views shipped in `ddl/views/vw_jobs.sql`:

```sql
SELECT * FROM vw_jobs WHERE business_key = 'foo';
SELECT * FROM vw_job_queue WHERE status = 'PENDING';
SELECT * FROM vw_job_execution WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654';
```

The views render `RAW(16)` columns with `RAWTOHEX` + `REGEXP_REPLACE`, re-inserting the canonical 8-4-4-4-12 dashes and lowercasing the result. The store writes UUIDs in standard big-endian byte order, so the formatted value round-trips with Java's `UUID.toString()`.

Apply the views once after schema load:

```bash
sqlplus <user>/<pass>@<service> @stores/ratchet-store-oracle/src/main/resources/ddl/views/vw_jobs.sql
```

Tools that handle binary IDs correctly (Hibernate, DataGrip's UUID-aware viewer, JDBC `getObject(..., UUID.class)`) can query the underlying tables directly.

## JPA mapping (production wiring)

How you wire `ratchet-store-oracle` into a `persistence.xml` depends on the JPA provider, because UUID columns are stored as `RAW(16)`. Give Ratchet its own persistence unit — its entity list is self-contained (`<exclude-unlisted-classes>true</exclude-unlisted-classes>`), and the `RatchetEntityManagerProvider` SPI lets a multi-unit application point Ratchet at it. Keeping Ratchet in its own unit means none of the provider settings below can touch your application's own entities or their columns.

**EclipseLink, OpenJPA (any non-Hibernate provider):** reference `META-INF/orm-oracle.xml` so UUID columns route through `UuidRawConverter`. These providers default to a 36-character hyphenated representation that a `RAW(16)` column cannot store (the hyphenated text is neither 16 bytes nor valid hex); the mapping file forces the byte representation.

```xml
<persistence-unit name="ratchet">
  <jta-data-source>java:/jdbc/MyDS</jta-data-source>
  <mapping-file>META-INF/orm-oracle.xml</mapping-file>
  <class>run.ratchet.store.entity.JobEntity</class>
  ...
</persistence-unit>
```

**Hibernate (ORM 6 or 7):** do NOT reference `orm-oracle.xml`. Hibernate maps `UUID` through its `BINARY` JDBC type, which lands in a `RAW(16)` column on Oracle (Oracle has no native UUID type), and Hibernate 7 rejects an `AttributeConverter` on an `@Id` attribute (`org.hibernate.AnnotationException`) — so referencing the mapping file would fail deployment. Use no mapping file at all:

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
