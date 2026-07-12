---
title: Oracle Deployment
---

# Oracle Deployment

Ratchet on Oracle Database 23ai.

## Prerequisites

- Oracle Database 23ai or later. The schema uses the native `BOOLEAN` type and `CREATE TABLE IF NOT EXISTS`, both of which arrived in 23ai.
- The Oracle JDBC driver (`ojdbc11`). Ratchet does not bundle it: Oracle publishes it under the Free Use Terms and Conditions, which is not an OSI-approved license, so the store keeps it test-scoped and you supply your own at runtime. Add it to your application or server module path.
- `sqlplus` (or SQLcl) to apply the schema.

## Schema setup

### Apply DDL

```bash
sqlplus ratchet/secret@//localhost:1521/FREEPDB1 \
  @stores/ratchet-store-oracle/src/main/resources/ddl/oracle-schema.sql
```

The Oracle store was added after the `0.1.1` release, so there is no published store JAR to
extract at that version. Build the current source tree and use the DDL path above. Starting with
`0.1.2`, the store JAR also contains the file at `ddl/oracle-schema.sql`.

Or copy it into your migration tool's versioned scripts:

```bash
cp ddl/oracle-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### Verify installation

```sql
SELECT table_name FROM user_tables WHERE table_name LIKE 'SCHEDULER\_%' ESCAPE '\' ORDER BY table_name;
```

You should see the same core tables as the other SQL stores: `scheduler_job`, `scheduler_job_queue`, `scheduler_business_key_reservation`, `scheduler_job_tag`, `scheduler_job_execution`, `scheduler_job_log`, `scheduler_job_archive`, `scheduler_batch`, `scheduler_batch_metrics`, `scheduler_node`, `scheduler_lock`, `scheduler_resource_limit`, `scheduler_resource_permit`, `scheduler_workflow_condition`, `scheduler_recurring_job`, and `scheduler_recurring_job_archive`, plus the `ratchet_schema_version` ledger.

## Configuration

### DataSource

Point a JTA data source at your Oracle instance and list the Ratchet entities in your persistence unit:

```xml
<!-- persistence.xml -->
<persistence-unit name="your-application-pu" transaction-type="JTA">
  <jta-data-source>java:/RatchetDS</jta-data-source>
  <!-- EclipseLink (and other non-Hibernate providers): route UUIDs through the RAW(16)
       converter and quote the LEVEL reserved word. Omit on Hibernate, which maps UUID to
       RAW(16) natively and rejects an AttributeConverter on an @Id attribute. -->
  <mapping-file>META-INF/orm-oracle.xml</mapping-file>
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
    <!-- Hibernate only (no-op elsewhere): the timestamp columns hold UTC wall-clock, so map
         Instant to plain TIMESTAMP rather than TIMESTAMP WITH TIME ZONE (which raises ORA-18716). -->
    <property name="hibernate.type.preferred_instant_jdbc_type" value="TIMESTAMP" />
    <property name="hibernate.jdbc.time_zone" value="UTC" />
  </properties>
</persistence-unit>
```

The Oracle store does not require a fixed persistence-unit name. If your application has multiple persistence units, provide a CDI alternative for `RatchetEntityManagerProvider` the same way the [PostgreSQL guide](/deployment/postgresql#datasource) shows.

### WildFly CLI

Register the driver as a module pointing at your `ojdbc11.jar`, then add the data source:

```bash
/subsystem=datasources/jdbc-driver=oracle:add( \
    driver-name=oracle, \
    driver-module-name=com.oracle, \
    driver-class-name=oracle.jdbc.OracleDriver)

/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=oracle, \
    connection-url=jdbc:oracle:thin:@//localhost:1521/FREEPDB1, \
    user-name=ratchet, \
    password=secret, \
    min-pool-size=5, \
    max-pool-size=20, \
    transaction-isolation=TRANSACTION_READ_COMMITTED)
```

### Connection string

```
jdbc:oracle:thin:@//localhost:1521/FREEPDB1
```

## Dialect notes

The Oracle store keeps the same data model as the MySQL and PostgreSQL stores; the differences are in how columns are typed and how the claim path is expressed.

### Time zone

Timestamp columns are plain `TIMESTAMP(6)` holding UTC wall-clock, and the claim path compares them against `CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)`, which is session-zone independent. Run the application JVM in UTC (or set `hibernate.jdbc.time_zone=UTC` on Hibernate) so the JDBC driver writes and reads the zone-less columns as UTC. A non-UTC JVM shifts stored timestamps and stalls claims.

### UUIDs as RAW(16)

Job identifiers are time-ordered UUIDv7 values stored as `RAW(16)`. On EclipseLink the `orm-oracle.xml` mapping routes them through `UuidRawConverter`; Hibernate maps `UUID` to `RAW(16)` natively.

### JSON payloads as CLOB

Payloads and other JSON columns are `CLOB`, not the native `JSON` type: encrypted payloads are not valid JSON, and native `JSON` reorders keys, which would break the encryption framing. The indexed `target_class`, `method_name`, and `trace_id_extracted` columns are virtual columns computed with `JSON_VALUE` over the CLOB.

Because the hot paths read these CLOBs back as strings through a pooled connection, set the Oracle JDBC LOB prefetch high so the driver returns the content inline rather than a LOB locator that can go stale when the pool recycles a connection under load. Set it on the driver (a generous default works for Ratchet's small payloads):

```
-Doracle.jdbc.defaultLobPrefetchSize=1048576
```

or per data source as a connection property of the same name.

### Two-phase claim

Oracle rejects `FETCH FIRST` combined with `FOR UPDATE SKIP LOCKED` (ORA-02014), so the claim runs in two phases: an unlocked top-N candidate select ordered by effective priority, then a `FOR UPDATE SKIP LOCKED` lock over just those candidate ids. A compare-and-set `UPDATE` remains the authoritative claim, so a candidate taken by another node between the phases is simply dropped, never double-claimed.

### CHECK constraints and BOOLEAN

Enum-like columns use `VARCHAR2` with `CHECK` constraints, mirroring the PostgreSQL store. Flag columns use the native Oracle 23ai `BOOLEAN` type.

## Auto-migration

Like the other SQL stores, the bundled startup migrator supports Oracle. Set `ratchet.schema.auto-migrate=true` (and supply a `DataSource`) and Ratchet applies the bundled DDL during startup. Oracle has no grant-free session-level advisory lock and its DDL auto-commits, so the migrator serializes concurrent migrators with an `EXCLUSIVE` lock on a dedicated `ratchet_schema_lock` table held on a second connection. **Size the connection pool maximum at 2 or more**: one connection runs the migration while the other holds the lock. See [Database Setup](/deployment/database-setup#auto-migration).

## See also

- [PostgreSQL Deployment](/deployment/postgresql)
- [MySQL Deployment](/deployment/mysql)
- [Database Setup](/deployment/database-setup)
- [Installation](/deployment/installation)
