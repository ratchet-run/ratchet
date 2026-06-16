---
sidebar_position: 4
title: Database Setup
description: "Setting up MySQL, PostgreSQL, or MongoDB for Ratchet: schema application, DataSource configuration, and connection pooling."
---

# Database Setup

Ratchet requires a database to persist jobs, execution history, and scheduling metadata. This guide covers setup for all three supported stores.

SQL stores ship DDL as plain SQL files bundled inside each SQL store module JAR. There is no Flyway or Liquibase dependency: apply the schema using whatever mechanism your team prefers, **or** opt in to Ratchet's built-in startup migrator (see [Auto-migration](#auto-migration) below). MongoDB initializes collections and indexes at startup unconditionally; its named indexes are referenced by claim queries, so initialization is correctness-critical, not optional.

## PostgreSQL

### Create the Database

```bash
# Connect as superuser
psql -U postgres

# Create the database and user
CREATE USER ratchet WITH PASSWORD 'your-secure-password';
CREATE DATABASE ratchet OWNER ratchet ENCODING 'UTF8';
GRANT ALL PRIVILEGES ON DATABASE ratchet TO ratchet;

# Connect to the new database
\c ratchet

# Grant schema privileges
GRANT ALL ON SCHEMA public TO ratchet;
```

### Apply the Schema

The DDL file is at `stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql` in the source, or `ddl/postgresql-schema.sql` inside the JAR.

```bash
# From the source tree
psql -U ratchet -d ratchet -f stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql

# Or extract from the JAR
jar xf ratchet-store-postgresql-0.1.1-SNAPSHOT.jar ddl/postgresql-schema.sql
psql -U ratchet -d ratchet -f ddl/postgresql-schema.sql
```

Using a migration framework:

```bash
# Copy the DDL into your migration directory
cp ddl/postgresql-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql

# Run with Flyway
flyway migrate

# Or with Liquibase
liquibase update
```

### Verify Installation

```sql
\dt scheduler_*
\dt ratchet_schema_version
```

You should see these scheduler tables plus `ratchet_schema_version`:

| Table | Purpose |
|-------|---------|
| `scheduler_job` | Cold job metadata, payload, and terminal state |
| `scheduler_job_queue` | Hot executable queue state |
| `scheduler_business_key_reservation` | Active business-key reservation guard |
| `scheduler_job_tag` | Tags for categorization |
| `scheduler_job_execution` | Per-attempt execution history |
| `scheduler_job_log` | Optional per-job log entries if your application persists `JobLogLine` events |
| `scheduler_batch` | Batch progress tracking |
| `scheduler_batch_metrics` | Batch performance metrics |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_recurring_job` | Recurring job masters |
| `scheduler_recurring_job_archive` | Archived recurring job masters |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_lock` | Distributed locks |
| `scheduler_resource_limit` | Resource concurrency config |
| `scheduler_resource_permit` | Active resource permits |
| `scheduler_workflow_condition` | Workflow branching conditions |
| `scheduler_dlq_alerts` | DLQ alert tracking |
| `ratchet_schema_version` | Applied schema migration/checksum tracking |

### DataSource Configuration

#### WildFly

```bash
# WildFly CLI
/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=postgresql, \
    connection-url=jdbc:postgresql://localhost:5432/ratchet, \
    user-name=ratchet, \
    password=your-secure-password, \
    min-pool-size=5, \
    max-pool-size=20, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.postgres.PostgreSQLValidConnectionChecker)
```

#### Open Liberty

```xml
<!-- server.xml -->
<dataSource id="RatchetDS" jndiName="java:/RatchetDS">
  <jdbcDriver libraryRef="postgresLib"/>
  <properties.postgresql
      serverName="localhost"
      portNumber="5432"
      databaseName="ratchet"
      user="ratchet"
      password="your-secure-password"/>
  <connectionManager minPoolSize="5" maxPoolSize="20"/>
</dataSource>

<library id="postgresLib">
  <fileset dir="${shared.resource.dir}/jdbc" includes="postgresql-*.jar"/>
</library>
```

#### Payara / GlassFish

```bash
# asadmin
create-jdbc-connection-pool \
    --datasourceclassname=org.postgresql.ds.PGSimpleDataSource \
    --restype=javax.sql.DataSource \
    --property=serverName=localhost:portNumber=5432:databaseName=ratchet:user=ratchet:password=your-secure-password \
    RatchetPool

create-jdbc-resource --connectionpoolid=RatchetPool java:/RatchetDS
```

### PostgreSQL-Specific Notes

- Ratchet uses `SKIP LOCKED` for lock-free job claiming across multiple nodes
- Generated columns extract `target_class` and `method_name` from the JSON payload
- Business key uniqueness for active jobs is enforced by the `scheduler_business_key_reservation` table, not by an index on `scheduler_job`
- The payload column uses `JSONB`, so you can query parameters directly: `payload ->> 'target'`

## MySQL

### Create the Database

```bash
mysql -u root -p

CREATE DATABASE ratchet CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'ratchet'@'%' IDENTIFIED BY 'your-secure-password';
GRANT ALL PRIVILEGES ON ratchet.* TO 'ratchet'@'%';
FLUSH PRIVILEGES;
```

### Apply the Schema

```bash
# From the source tree
mysql -u ratchet -p ratchet < stores/ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql

# Or extract from the JAR
jar xf ratchet-store-mysql-0.1.1-SNAPSHOT.jar ddl/mysql-schema.sql
mysql -u ratchet -p ratchet < ddl/mysql-schema.sql
```

### Verify Installation

```sql
SHOW TABLES LIKE 'scheduler_%';
SHOW TABLES LIKE 'ratchet_schema_version';
```

You should see the same core tables as PostgreSQL, with MySQL-specific column types (ENUM, JSON, GENERATED ALWAYS columns).

### DataSource Configuration

#### WildFly

```bash
/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=mysql, \
    connection-url=jdbc:mysql://localhost:3306/ratchet, \
    user-name=ratchet, \
    password=your-secure-password, \
    min-pool-size=5, \
    max-pool-size=20, \
    transaction-isolation=TRANSACTION_READ_COMMITTED, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.mysql.MySQLValidConnectionChecker)
```

#### Open Liberty

```xml
<!-- server.xml -->
<dataSource id="RatchetDS" jndiName="java:/RatchetDS" isolationLevel="TRANSACTION_READ_COMMITTED">
  <jdbcDriver libraryRef="mysqlLib"/>
  <properties.mysql
      serverName="localhost"
      portNumber="3306"
      databaseName="ratchet"
      user="ratchet"
      password="your-secure-password"/>
  <connectionManager minPoolSize="5" maxPoolSize="20"/>
</dataSource>
```

:::caution MySQL Isolation Level
MySQL defaults to `REPEATABLE READ`, which acquires gap locks on `SELECT ... FOR UPDATE` that block concurrent inserts. This causes lock wait timeouts under production load. **Always** configure `READ COMMITTED` isolation via one of:

- **DataSource property**: `transaction-isolation=TRANSACTION_READ_COMMITTED`
- **JDBC URL parameter**: `?sessionVariables=transaction_isolation='READ-COMMITTED'`
- **persistence.xml**: `<property name="hibernate.connection.isolation" value="2"/>`
- **WildFly `-ds.xml`**: `<transaction-isolation>TRANSACTION_READ_COMMITTED</transaction-isolation>`

Verify the effective level on a live connection with `SELECT @@transaction_isolation;` (MySQL) or `SHOW default_transaction_isolation;` (PostgreSQL, which already defaults to `READ COMMITTED`).
:::

### MySQL-Specific Notes

- Uses `ENUM` types for status, job type, and backoff policy columns
- Uses `JSON` column type for payload, params, and result data
- `GENERATED ALWAYS AS ... STORED` columns extract `target_class` and `method_name` from payload JSON
- Business key uniqueness for active jobs is enforced by the `scheduler_business_key_reservation` table, not by a column on `scheduler_job`
- All tables use `InnoDB` engine with `utf8mb4_unicode_ci` collation

## MongoDB

### Create the Database

```bash
mongosh

use ratchet

db.createUser({
  user: "ratchet",
  pwd: "your-secure-password",
  roles: [{ role: "readWrite", db: "ratchet" }]
});
```

### Initialize Collections and Indexes

MongoDB does not require a DDL file; the store module creates collections and indexes automatically on startup. You can pre-create the same collections and indexes for faster initial startup:

```javascript
// mongosh
use ratchet;

db.createCollection("scheduler_job");
db.createCollection("scheduler_batch");
db.createCollection("scheduler_batch_metrics");
db.createCollection("scheduler_job_execution");
db.createCollection("scheduler_job_log");
db.createCollection("scheduler_job_archive");
db.createCollection("scheduler_recurring_job");
db.createCollection("scheduler_recurring_job_archive");
db.createCollection("scheduler_node");
db.createCollection("scheduler_lock");
db.createCollection("scheduler_workflow_condition");
db.createCollection("scheduler_dlq_alerts");
db.createCollection("scheduler_resource_permit");
db.createCollection("scheduler_resource_limit");

// Key indexes
db.scheduler_job.createIndex({ status: 1, priority: -1, scheduled_time: 1 }, { name: "idx_job_poll_composite" });
db.scheduler_job.createIndex({ status: 1, job_type: 1, priority: -1, scheduled_time: 1, _id: 1 }, { name: "idx_job_claim_exec" });
db.scheduler_job.createIndex({ idempotency_key: 1 }, { name: "idx_job_idempotency_key", unique: true });
db.scheduler_job.createIndex(
  { business_key: 1 },
  {
    name: "idx_job_active_business_key",
    unique: true,
    partialFilterExpression: {
      status: { $in: ["PENDING", "RUNNING", "PAUSED", "WAITING"] },
      business_key: { $type: "string" }
    }
  }
);
db.scheduler_job.createIndex({ tags: 1 }, { name: "idx_job_tags" });
db.scheduler_job_archive.createIndex({ original_job_id: 1 }, { name: "idx_archive_original_job_id" });
db.scheduler_job_execution.createIndex({ job_id: 1 }, { name: "idx_execution_job_id" });
db.scheduler_node.createIndex({ heartbeat_ts: 1 }, { name: "idx_node_heartbeat" });
db.scheduler_lock.createIndex({ expires_at: 1 }, { name: "idx_lock_ttl", expireAfterSeconds: 0 });
db.scheduler_job_log.createIndex({ job_id: 1, ts: 1 }, { name: "idx_log_job_ts" });
```

### Connection Configuration

Ratchet does not define its own MongoDB URI property. Configure the connection through your application runtime and expose a `MongoDatabase` bean. For example:

```java
@Produces
@ApplicationScoped
public MongoDatabase mongoDatabase() {
    return MongoClients.create("mongodb://ratchet:password@localhost:27017")
        .getDatabase("ratchet");
}
```

## Connection Pool Sizing

The connection pool should be sized based on the number of executor threads plus overhead for the polling engine and administrative queries.

### Formula

```
pool_size = worker_threads + polling_threads + admin_overhead
```

A reasonable starting point:

| Executor Threads | Recommended Pool Size |
|-----------------|----------------------|
| 20 (default) | 15-20 |
| 16 | 25-30 |
| 32 | 40-50 |
| 64+ | executor threads * 1.5 |

### PostgreSQL Connection Limits

Check the server's max connections:

```sql
SHOW max_connections;  -- Default: 100
```

Increase if needed:

```sql
ALTER SYSTEM SET max_connections = 200;
-- Restart PostgreSQL
```

### MySQL Connection Limits

```sql
SHOW VARIABLES LIKE 'max_connections';  -- Default: 151
```

```ini
# my.cnf
[mysqld]
max_connections = 200
```

## Schema Upgrades

Ratchet uses `CREATE TABLE IF NOT EXISTS` in its DDL on both MySQL and PostgreSQL. On PostgreSQL it also uses `CREATE INDEX IF NOT EXISTS`; on MySQL indexes are declared inline within `CREATE TABLE IF NOT EXISTS` (MySQL has no partial indexes), so MySQL re-run safety relies on `CREATE TABLE IF NOT EXISTS` across the 18 tables. This means you can safely re-run the schema file against an existing database, and it will create any missing tables or indexes without modifying existing ones.

For schema changes between Ratchet versions:

1. Check the release notes for migration instructions
2. Back up your database
3. Apply any migration SQL provided in the release
4. Re-run the full schema DDL to create any new tables/indexes

Since Ratchet does not bundle a Flyway/Liquibase runtime dependency, you are free to manage schema changes using whatever tool your team already uses (Flyway, Liquibase, manual scripts, container init scripts, etc.).

## Auto-migration

For dev, CI, and embedded deployments where running DBA-grade migration tooling is overkill, Ratchet ships a built-in startup migrator that applies `ddl/migrations/V###__description.sql` from the SQL store JARs. **It is OFF by default.** Production deployments typically keep the default and run migrations through their existing pipelines; dev/CI flips a single env var and gets a "just-works" bootstrap.

### Enable

```bash
RATCHET_SCHEMA_AUTO_MIGRATE=true
```

Or via configuration:

```properties
ratchet.schema.auto-migrate=true
```

When enabled, `SchemaMigrationLifecycleHook` runs during scheduler startup (before the poller is initialized), acquires an advisory lock (`GET_LOCK` on MySQL, `pg_advisory_lock` on PostgreSQL), records each applied script in `ratchet_schema_version`, and verifies SHA-256 checksums on subsequent runs. Concurrent startups converge: exactly one node applies migrations while the others wait on the lock.

### DataSource binding

Auto-migration requires a CDI-discoverable `javax.sql.DataSource`. Most application servers expose this automatically; if yours doesn't, produce one explicitly:

```java
@ApplicationScoped
class RatchetDataSourceProducer {
  @Resource(lookup = "java:/RatchetDS")
  private DataSource dataSource;

  @Produces
  @ApplicationScoped
  DataSource dataSource() {
    return dataSource;
  }
}
```

If no `DataSource` bean is available when `auto-migrate=true`, deployment fails fast with a clear error.

### Supported dialects

| Database | `ratchet.schema.migration-dialect` value | Auto-detected? |
|----------|------------------------------------------|----------------|
| MySQL ≥ 8 | `mysql` | yes |
| MariaDB | `mysql` | yes |
| PostgreSQL | `postgresql` | yes |
| Anything else (incl. CockroachDB) | unsupported | no |

The dialect is auto-detected from `DatabaseMetaData.getDatabaseProductName()`. Look-alike products such as **CockroachDB** report a PostgreSQL wire protocol but lack `pg_advisory_lock`, so they are explicitly rejected even though the wire is compatible. Override the auto-detected value with `RATCHET_SCHEMA_MIGRATION_DIALECT=mysql` (or `postgresql`) only if you have verified your driver-product combination.

### Enabling auto-migrate on a database that already has the schema

The bundled migrations are idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`), so `auto-migrate=true` is safe to enable against a database whose `scheduler_*` tables already exist (for example, one provisioned directly from the consolidated `*-schema.sql`). On the first run the migrator re-applies each script as a no-op and records it in `ratchet_schema_version`; every subsequent run skips by checksum. If you would rather manage the schema entirely through external tooling, keep `auto-migrate=false`.

### `CREATE INDEX CONCURRENTLY`

PostgreSQL rejects `CREATE INDEX CONCURRENTLY` inside a transaction block, and the auto-migrator wraps each script in a JDBC transaction. The bundled migrations therefore use plain `CREATE INDEX`. Operators who need to avoid the brief table lock during a large-table reindex can apply the migration script manually with `CONCURRENTLY` before flipping `auto-migrate=true`; `ratchet_schema_version` records the version regardless of how the DDL ran.

### MongoDB

MongoDB does not participate in `auto-migrate`; its collections and named indexes are created unconditionally during store startup. The `auto-migrate` flag is JDBC-only by contract.

## Backup Strategy

### PostgreSQL

```bash
# Logical backup
pg_dump -Fc -v ratchet > ratchet-backup.dump

# Restore
pg_restore -d ratchet ratchet-backup.dump
```

### MySQL

```bash
# Logical backup
mysqldump -u ratchet -p ratchet > ratchet-backup.sql

# Restore
mysql -u ratchet -p ratchet < ratchet-backup.sql
```

### MongoDB

```bash
# Backup
mongodump --db ratchet --out /backup/

# Restore
mongorestore --db ratchet /backup/ratchet/
```

For all databases, schedule regular backups and test restoration periodically. In production, consider point-in-time recovery using WAL archiving (PostgreSQL), binary log (MySQL), or oplog (MongoDB).

## See Also

- [PostgreSQL Deployment](/deployment/postgresql) -- PostgreSQL-specific tuning and monitoring
- [MySQL Deployment](/deployment/mysql) -- MySQL-specific tuning and monitoring
- [Configuration](/deployment/configuration) -- Full configuration reference
- [Deployment Overview](/deployment/overview) -- General deployment guidance
