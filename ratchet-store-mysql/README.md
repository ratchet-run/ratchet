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

The views call `BIN_TO_UUID(col, 1)` — the `, 1` argument is required for byte order to match Java's `UUID.toString()`. Without it, MySQL applies a timestamp-shift transform that does NOT round-trip with the canonical (Java/PostgreSQL) representation Hibernate writes.

Apply the views once after schema load:

```bash
mysql -u <user> -p ratchet < ratchet-store-mysql/src/main/resources/ddl/views/vw_jobs.sql
```

Tools that handle binary IDs correctly (Hibernate, DataGrip's UUID-aware viewer, JDBC `getObject(..., UUID.class)`) can query the underlying tables directly.

## JPA mapping (production wiring)

When wiring `ratchet-store-mysql` into a Jakarta application's `persistence.xml`, the persistence-unit must reference `META-INF/orm-mysql.xml` so UUID columns route through `UuidByteArrayConverter`:

```xml
<persistence-unit name="my-app">
  <jta-data-source>java:/jdbc/MyDS</jta-data-source>
  <mapping-file>META-INF/orm-mysql.xml</mapping-file>
  <class>run.ratchet.store.entity.JobEntity</class>
  ...
</persistence-unit>
```

Hibernate's built-in UUID handler already produces standard-byte-order `BINARY(16)`, so the converter is idempotent there. EclipseLink and OpenJPA default to a 36-character hyphenated representation that overflows `BINARY(16)` with MySQL strict mode error 1406 ("Data too long for column"); the mapping file forces the byte representation on every JPA provider.

PostgreSQL's `ratchet-store-postgresql` does NOT need a mapping file — PostgreSQL's native `uuid` column type round-trips `java.util.UUID` directly through JDBC.

## Storage characteristics

UUIDv7 PKs add ~8 bytes per row over the previous 8-byte BIGINT. At 100M rows with ~5 secondary indexes the delta is roughly **4.8 GB additional index space**. Bounded but worth budgeting for — The storage, operator-ergonomics, and portability tradeoffs drove the UUIDv7 migration decision.
