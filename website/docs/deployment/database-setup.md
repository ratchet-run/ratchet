---
sidebar_position: 4
title: Database Setup
description: Setting up MySQL, PostgreSQL, or MongoDB for Ratchet — schema application, DataSource configuration, and connection pooling.
---

# Database Setup

Ratchet requires a database to persist jobs, execution history, and scheduling metadata. This guide covers setup for all three supported stores.

Ratchet ships DDL as plain SQL files bundled inside each store module JAR. There is no Flyway or Liquibase dependency — you apply the schema using whatever mechanism your team prefers.

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

The DDL file is at `ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql` in the source, or `ddl/postgresql-schema.sql` inside the JAR.

```bash
# From the source tree
psql -U ratchet -d ratchet -f ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql

# Or extract from the JAR
jar xf ratchet-store-postgresql-0.1.0-SNAPSHOT.jar ddl/postgresql-schema.sql
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
```

You should see 13 tables:

| Table | Purpose |
|-------|---------|
| `scheduler_job` | Job definitions, status, payload |
| `scheduler_job_tag` | Tags for categorization |
| `scheduler_job_execution` | Per-attempt execution history |
| `scheduler_job_log` | Per-job structured log entries |
| `scheduler_batch` | Batch progress tracking |
| `scheduler_batch_metrics` | Batch performance metrics |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_lock` | Distributed locks |
| `scheduler_resource_limit` | Resource concurrency config |
| `scheduler_resource_permit` | Active resource permits |
| `scheduler_workflow_condition` | Workflow branching conditions |
| `scheduler_dlq_alerts` | DLQ alert tracking |

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
- A partial unique index enforces business key uniqueness for active jobs only (`PENDING`, `RUNNING`, `PAUSED`)
- JSONB is not used for the payload column (it uses TEXT), but you can query parameters via casting: `payload::jsonb ->> 'target'`

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
mysql -u ratchet -p ratchet < ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql

# Or extract from the JAR
jar xf ratchet-store-mysql-0.1.0-SNAPSHOT.jar ddl/mysql-schema.sql
mysql -u ratchet -p ratchet < ddl/mysql-schema.sql
```

### Verify Installation

```sql
SHOW TABLES LIKE 'scheduler_%';
```

You should see the same 13 tables as PostgreSQL, with MySQL-specific column types (ENUM, JSON, GENERATED ALWAYS columns).

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
:::

### MySQL-Specific Notes

- Uses `ENUM` types for status, job type, and backoff policy columns
- Uses `JSON` column type for payload, params, and result data
- `GENERATED ALWAYS AS ... STORED` columns extract `target_class` and `method_name` from payload JSON
- `active_business_key` is a generated column that enforces uniqueness only for active jobs
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

MongoDB does not require a DDL file — the store module creates collections and indexes automatically on startup. However, you can pre-create indexes for faster initial startup:

```javascript
// mongosh
use ratchet;

db.createCollection("scheduler_jobs");
db.createCollection("scheduler_job_executions");
db.createCollection("scheduler_job_logs");
db.createCollection("scheduler_nodes");
db.createCollection("scheduler_locks");

// Key indexes
db.scheduler_jobs.createIndex({ status: 1, scheduledTime: 1 });
db.scheduler_jobs.createIndex({ status: 1, priority: -1, scheduledTime: 1 });
db.scheduler_jobs.createIndex({ idempotencyKey: 1 }, { unique: true });
db.scheduler_jobs.createIndex({ jobType: 1, status: 1, nextFire: 1 });
db.scheduler_jobs.createIndex({ "tags": 1 });

db.scheduler_nodes.createIndex({ heartbeatTs: 1 });
db.scheduler_locks.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
```

### Connection Configuration

```properties
ratchet.store.mongodb.uri=mongodb://ratchet:your-secure-password@localhost:27017/ratchet
```

Or in your application server's configuration, use the MongoDB client URI:

```java
@Produces
@ApplicationScoped
public MongoClient mongoClient() {
    return MongoClients.create("mongodb://ratchet:password@localhost:27017/ratchet");
}
```

## Connection Pool Sizing

The connection pool should be sized based on the number of executor threads plus overhead for the polling engine and administrative queries.

### Formula

```
pool_size = ratchet.executor.threads + polling_threads + admin_overhead
```

A reasonable starting point:

| Executor Threads | Recommended Pool Size |
|-----------------|----------------------|
| 8 (default) | 15-20 |
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

Ratchet uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` in its DDL. This means you can safely re-run the schema file against an existing database — it will create any missing tables or indexes without modifying existing ones.

For schema changes between Ratchet versions:

1. Check the release notes for migration instructions
2. Back up your database
3. Apply any migration SQL provided in the release
4. Re-run the full schema DDL to create any new tables/indexes

Since Ratchet does not bundle a migration framework, you are free to manage schema changes using whatever tool your team already uses (Flyway, Liquibase, manual scripts, etc.).

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

- [PostgreSQL Deployment](/docs/deployment/postgresql) — PostgreSQL-specific tuning and monitoring
- [MySQL Deployment](/docs/deployment/mysql) — MySQL-specific tuning and monitoring
- [Configuration](/docs/deployment/configuration) — Full configuration reference
- [Deployment Overview](/docs/deployment/overview) — General deployment guidance
