---
title: SQL Server Deployment
---

# SQL Server Deployment

Ratchet on Microsoft SQL Server 2022.

## Prerequisites

- SQL Server 2022 or later. The claim path orders by effective priority with `GREATEST`, a function that arrived in SQL Server 2022, so earlier engines (including the SQL Server 2019-based Azure SQL Edge) do not pass the claim contracts.
- `READ_COMMITTED_SNAPSHOT` enabled on the Ratchet database (see [Row-versioning](#row-versioning-is-required)). Ratchet's claim path assumes the non-blocking read semantics of an MVCC engine; SQL Server's default lock-based `READ COMMITTED` takes shared read locks and deadlocks concurrent claim/cancel paths without it.
- The Microsoft JDBC driver (`mssql-jdbc`). Ratchet does not bundle it: the store keeps it test-scoped so you control the driver version, and you supply your own at runtime. Add it to your application or server module path.
- `sqlcmd` (or Azure Data Studio / SSMS) to apply the schema.

## Schema setup

### Create the database with row-versioning

```sql
CREATE DATABASE ratchet;
ALTER DATABASE ratchet SET READ_COMMITTED_SNAPSHOT ON;
ALTER DATABASE ratchet SET ALLOW_SNAPSHOT_ISOLATION ON;
```

### Apply DDL

```bash
sqlcmd -S localhost,1433 -U ratchet -P 'secret' -d ratchet -C \
  -i stores/ratchet-store-sqlserver/src/main/resources/ddl/sqlserver-schema.sql
```

The SQL Server store was added after the `0.1.1` release, so there is no published store JAR to
extract at that version. Build the current source tree and use the DDL path above. Starting with
`0.2.0`, the store JAR also contains the file at `ddl/sqlserver-schema.sql`.

Or copy it into your migration tool's versioned scripts:

```bash
cp ddl/sqlserver-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### Verify installation

```sql
SELECT name FROM sys.tables WHERE name LIKE 'scheduler_%' ORDER BY name;
```

You should see the same core tables as the other SQL stores: `scheduler_job`, `scheduler_job_queue`, `scheduler_business_key_reservation`, `scheduler_job_tag`, `scheduler_job_execution`, `scheduler_job_log`, `scheduler_job_archive`, `scheduler_batch`, `scheduler_batch_metrics`, `scheduler_node`, `scheduler_lock`, `scheduler_resource_limit`, `scheduler_resource_permit`, `scheduler_workflow_condition`, `scheduler_recurring_job`, and `scheduler_recurring_job_archive`, plus the `ratchet_schema_version` ledger.

## Configuration

### DataSource

Point a JTA data source at your SQL Server instance and list the Ratchet entities in your persistence unit:

```xml
<!-- persistence.xml -->
<persistence-unit name="your-application-pu" transaction-type="JTA">
  <jta-data-source>java:/RatchetDS</jta-data-source>
  <!-- EclipseLink (and other non-Hibernate providers): round-trip UUIDs through the BINARY(16)
       converter so every provider writes the same canonical big-endian bytes. Omit on Hibernate,
       which maps UUID to BINARY(16) natively and rejects an AttributeConverter on an @Id attribute. -->
  <mapping-file>META-INF/orm-sqlserver.xml</mapping-file>
  <class>run.ratchet.store.entity.JobEntity</class>
  <class>run.ratchet.store.entity.JobExecutionEntity</class>
  <class>run.ratchet.store.entity.ResourceLimitEntity</class>
  <class>run.ratchet.store.entity.BatchMetricsEntity</class>
  <class>run.ratchet.store.entity.WorkflowConditionEntity</class>
  <class>run.ratchet.store.entity.ArchivedJobEntity</class>
  <class>run.ratchet.store.entity.NodeEntity</class>
  <class>run.ratchet.store.entity.JobLogEntity</class>
  <class>run.ratchet.store.entity.ResourcePermitEntity</class>
  <class>run.ratchet.store.entity.BatchEntity</class>
  <exclude-unlisted-classes>true</exclude-unlisted-classes>
  <properties>
    <!-- Hibernate only (no-op elsewhere): the timestamp columns hold UTC wall-clock, so keep the
         JDBC session in UTC. -->
    <property name="hibernate.jdbc.time_zone" value="UTC" />
  </properties>
</persistence-unit>
```

The SQL Server store does not require a fixed persistence-unit name. If your application has multiple persistence units, provide a CDI alternative for `RatchetEntityManagerProvider` the same way the [PostgreSQL guide](/deployment/postgresql#datasource) shows.

### WildFly CLI

Register the driver as a module pointing at your `mssql-jdbc.jar`, then add the data source:

```bash
/subsystem=datasources/jdbc-driver=sqlserver:add( \
    driver-name=sqlserver, \
    driver-module-name=com.microsoft.sqlserver, \
    driver-class-name=com.microsoft.sqlserver.jdbc.SQLServerDriver)

/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=sqlserver, \
    connection-url=jdbc:sqlserver://localhost:1433;databaseName=ratchet;encrypt=true;trustServerCertificate=true, \
    user-name=ratchet, \
    password=secret, \
    min-pool-size=5, \
    max-pool-size=20, \
    transaction-isolation=TRANSACTION_READ_COMMITTED)
```

### Open Liberty isolation level

Open Liberty's SQL Server data-store helper overrides an unset data source to `TRANSACTION_REPEATABLE_READ`. Under that default Ratchet's startup isolation check fails, so pin the level explicitly:

```xml
<dataSource id="RatchetDS" jndiName="jdbc/RatchetDS" transactional="true"
            isolationLevel="TRANSACTION_READ_COMMITTED">
  ...
</dataSource>
```

WildFly sets the level during datasource setup, and GlassFish and Payara default SQL Server to `READ COMMITTED`, so no extra configuration is needed there.

### Connection string

```
jdbc:sqlserver://localhost:1433;databaseName=ratchet;encrypt=true;trustServerCertificate=true
```

`mssql-jdbc` 12+ encrypts connections by default. Use a properly trusted certificate in production; `trustServerCertificate=true` accepts a self-signed certificate for local development only.

## Dialect notes

The SQL Server store keeps the same data model as the MySQL and PostgreSQL stores; the differences are in how columns are typed and how the claim path is expressed.

### Row-versioning is required

The Ratchet database must have `READ_COMMITTED_SNAPSHOT ON`. SQL Server's default lock-based `READ COMMITTED` takes shared read locks — unlike the MVCC PostgreSQL and MySQL engines Ratchet targets — so concurrent claim and cancel paths deadlock. Row-versioning snapshot reads restore the non-blocking semantics the store assumes. The startup isolation check verifies the level is `READ COMMITTED`.

### UUIDs as BINARY(16)

Job identifiers are time-ordered UUIDv7 values stored as `BINARY(16)` holding the canonical big-endian bytes, **not** the native `UNIQUEIDENTIFIER` type. `UNIQUEIDENTIFIER` uses .NET-Guid mixed-endian storage that EclipseLink 5.0 (the GlassFish 8 / EE 11 reference implementation) reads byte-swapped from native queries; raw bytes read identically across every provider. On EclipseLink the `orm-sqlserver.xml` mapping routes UUIDs through `UuidByteArrayConverter`; Hibernate maps `UUID` to `BINARY(16)` natively. See the [store README](https://github.com/ratchet-run/ratchet/blob/main/stores/ratchet-store-sqlserver/README.md) for how to query the binary IDs by hand with `0x` hex literals.

### JSON payloads as NVARCHAR(MAX)

Payloads and other JSON columns are `NVARCHAR(MAX)`: SQL Server JSON is text plus the `JSON_VALUE`/`OPENJSON` functions rather than a stored type. The indexed `target_class` and `method_name` columns are `PERSISTED` computed columns over `JSON_VALUE(...)` cast to a bounded `VARCHAR`, because SQL Server cannot index `NVARCHAR(MAX)` directly.

### Time zone

Timestamp columns are zoneless `DATETIME2(6)` holding UTC wall-clock, and the claim path compares them against `SYSUTCDATETIME()`. `mssql-jdbc` binds `java.sql.Timestamp` to `DATETIME2` using the JVM's default zone, so run the application JVM in UTC (or set `hibernate.jdbc.time_zone=UTC` on Hibernate). A non-UTC JVM shifts stored timestamps and stalls claims.

### Effective-priority claim (SQL Server 2022+)

The claim selects due rows with `WITH (UPDLOCK, READPAST, ROWLOCK)` so concurrent claimers skip already-locked rows, ordered by an effective priority that boosts overdue jobs with `GREATEST`. `GREATEST` is a SQL Server 2022 function — this is why 2022 is the floor. Upserts (resource limits, workflow conditions, the version ledger) use `MERGE`.

## Auto-migration

Like the other SQL stores, the bundled startup migrator supports SQL Server. Set `ratchet.schema.auto-migrate=true` (and supply a `DataSource`) and Ratchet applies the bundled DDL during startup. The migration lock is a session-scoped `sp_getapplock` (`@LockOwner = 'Session'`), which is tied to the connection rather than a transaction and so survives the migrator's per-script commits **without** a dedicated lock connection — a single-connection pool is sufficient. A second migrator that cannot acquire the lock within 30 seconds fails loudly. See [Database Setup](/deployment/database-setup#auto-migration).

## See also

- [PostgreSQL Deployment](/deployment/postgresql)
- [Oracle Deployment](/deployment/oracle)
- [Database Setup](/deployment/database-setup)
- [Runtime setup](/deployment/installation)
