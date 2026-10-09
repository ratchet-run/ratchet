# Micronaut consumer tests

This is an independent application reactor with no Ratchet parent. It resolves installed Ratchet
artifacts from a Maven repository and owns its Micronaut platform and annotation processors.
Nothing in this reactor imports integration implementation classes or Ratchet RI internals.
The public scheduler entry point is `JobSchedulerService` (there is no `JobScheduler` type).

The same integration jars, compiled with Micronaut 4.10, are consumed by both lanes. In particular,
`mn5` compiles the application and test bean definitions with the **5.x processors** and runs
against the **5.x runtime**, while Ratchet's bean definitions remain 4.10-generated. Rebuilding
Ratchet with a 5.x processor would bypass the mixed-generation compatibility proof.

| Lane | Platform BOM | Inject processor | Data processor | Build JDK |
| --- | --- | --- | --- | --- |
| `mn4` (active by default) | 4.10.20 | 4.10.30 | 4.14.5 | 17+ |
| `mn5` | 5.2.2 | 5.2.15 | 5.2.2 | 25+ (enforced) |

Select one lane per invocation. Version overrides are `mn4.platform.version`, `mn4.inject.version`,
`mn4.data.version`, `mn5.platform.version`, `mn5.inject.version`, and `mn5.data.version`.
The compiler uses Java 17 source/API compatibility in both lanes, with explicit annotation
processing enabled on JDK 25. Hibernate is selected by the lane's platform BOM (6.6 on MN4).

The Testcontainers BOM (`testcontainers.version=2.0.5`) is imported **before** the Micronaut BOM.
Micronaut 4.10 manages Testcontainers 1.x, which cannot communicate with Docker 29. MongoDB driver
components are explicitly managed at `mongodb-driver.version=5.11.1`: `ratchet-store-mongodb`
uses that version through the root POM, whereas Micronaut 4.10 manages 4.11.5. This keeps BSON,
core, sync, record codecs, and reactive driver components aligned. `ratchet.version` defaults to
`0.5.1-SNAPSHOT`, the current Ratchet project version, and can be overridden for staged releases.

## Stage artifacts

Run these commands from the Ratchet repository root. Use JDK 17 or later for staging, and keep the
integration's 4.10 processor configuration.
The additional store selections make all four SQL cells available in the isolated repository.

```bash
staged_repo="$(mktemp -d)"
mvn -B -ntp -Dmaven.repo.local="$staged_repo" \
  -pl :ratchet-micronaut,:ratchet-micronaut-jpa,:ratchet-micronaut-mongodb,:ratchet-tck-api,:ratchet-tck-store,:ratchet-store-postgresql,:ratchet-store-mysql,:ratchet-store-oracle,:ratchet-store-sqlserver,:ratchet-store-mongodb \
  -am install -Pgithub -DskipTests -Dspotbugs.skip=true -Dspotless.skip=true
```

The consumer commands below use the isolated repository and the independent reactor. Docker must
be available. Maven needs access to any lane dependencies not already cached in that repository.

## Run both lanes

On **JDK 17**, run MN4 SQL and MongoDB:

```bash
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Pmn4 -Dstore=postgresql \
  -pl :sql-consumer -am clean verify

mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Pmn4 \
  -pl :mongodb-consumer -am clean verify
```

On **JDK 25**, consume the **same staged Ratchet jars** with MN5:

```bash
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Pmn5 -Dstore=postgresql \
  -pl :sql-consumer -am clean verify

mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Pmn5 \
  -pl :mongodb-consumer -am clean verify
```

Repeat each SQL command with `-Dstore=mysql`, `-Dstore=oracle`, and `-Dstore=sqlserver`.
Omit `-pl ... -am` to run all modules for one lane. Always use `clean` when switching lanes so
previously generated application bean definitions cannot mask processor incompatibilities.
The Spotless configuration and `verify` format check are copied from the Spring consumer reactor.
MN4 uses its formatter 1.24.0; MN5 overrides it to 1.35.0 for JDK 25 javac compatibility.
When running MN4 on JDK 25, also pass `-Dgoogle-java-format.version=1.35.0`.
To apply formatting, use the same independent POM and selected lane with `spotless:apply`.

| Store | Configuration | Prerequisites |
| --- | --- | --- |
| PostgreSQL (default) | `application-postgresql.yml` | Ordinary default datasource. |
| MySQL | `application-mysql.yml` | `TRANSACTION_READ_COMMITTED`; UTC connection timezone. |
| Oracle | `application-oracle.yml` | UTC Hibernate JDBC timezone; Oracle host-table schema. |
| SQL Server | `application-sqlserver.yml` | UTC Hibernate JDBC timezone; dedicated `ratchet` database with `READ_COMMITTED_SNAPSHOT` and `ALLOW_SNAPSHOT_ISOLATION`. |
| MongoDB | `mongodb.uri`, `ratchet.mongodb.database` | Replica-set container; Micronaut-owned sync client, selected Ratchet database. |

`SqlDatabase` retains the Spring container fixture's SQL Server provisioning, images, and storage
handling. `RATCHET_TEST_TMPFS=true` uses a 16 GiB local-driver tmpfs volume for PostgreSQL, MySQL,
and SQL Server. For Oracle, set `RATCHET_TEST_DATA_DIRECTORY` to an existing writable host directory;
the fixture owns a unique child, preserves image-seeded files, and removes it after stopping.
Oracle rejects tmpfs-only configuration. `RATCHET_MSSQL_IMAGE` can override the SQL Server image.
MongoDB's copied fixture uses an opt-in `/data/db` tmpfs and a replica set. The application
adds `uuidRepresentation=standard` to `mongodb.uri`, matching the store's UUID codec prerequisite.

The host owns its application schema: `SqlApplication` provisions the copied `schema-<store>.sql`
files before starting Micronaut, using `/` separators for Oracle PL/SQL. The application configures
`datasources.default.*`, `jpa.default.*`, and `ratchet.jpa.datasource=default`; Hibernate validates
rather than generating tables. Ratchet migrations must register and provision Ratchet's own
entities independently of the application entity/converter scan.

## What the suites cover

- `SqlTransactionsTest`: outer rollback preserves the inner REQUIRES_NEW application row and job;
  NOT_SUPPORTED submission commits; rollback-only and checked `rollbackOn` remove both rows;
  batch children and parent reach success; registration failure suppresses synchronous wakeup but
  polling still executes the durable job; platform/virtual targets retain transaction advice.
- `PostgresqlConsumerTest` (runs on every selected SQL vendor): application/job commit and rollback,
  normal work, workflow order, signals, and a Micronaut `@Recurring` bean.
- `MicronautOverridesTest`: `@Replaces(ExecutorProvider.class)`, an application `ClassPolicy` without
  `@Replaces`, named injection of the replacement Ratchet SPI bean, public scheduler injection,
  and actual work using the replacements. This must pass in both processor generations.
- `SqlHostCoexistenceTest`: application UUID, auto-apply converter, and Instant round trip outside
  UTC, including SQL Server's application-owned `uniqueidentifier` mapping.
- `SqlSchemaLifecycleTest`: fresh migration, external-schema validation without creating a ledger,
  empty/incomplete schema rejection, missing unique index/principal column detection, and recovery
  after failed migration discovery.
- `SqlLifecycleTest`: sequential contexts, fatal startup recovery, disabled integration with usable
  host JPA and no Ratchet schema, and shutdown that drains a blocked job before datasource closure.
- `SqlStoreContractTest` / `MongoStoreContractTest`: all 21 nested store contract groups are retained.
  The `fixtures` module supplies inert scheduling timers only for `consumer.store-contract=true`;
  the actual integration-created store bean and transaction proxy remain under test.
- `SqlApiContractTest` / `MongoApiContractTest`: the shared API TCK remains enabled, including
  real-clock delayed scheduling. Reset closes/drains the entire application, clears persisted
  scheduler data, then boots a fresh context/probe; this also resets engine circuit breakers without
  reaching into RI internals. SQL resets share one transaction; MongoDB resets preserve indexes.
- `MongoConsumerTest`: one Micronaut-managed Mongo client, actual job execution in the selected
  database, and validation-only behavior with existing or missing collections/indexes. Mongo-only
  transaction interception must work without a host JPA manager.

The inherited TCK's unsupported controllable-clock and stale-write cases remain its own skips;
no consumer exclusions are added. Review Surefire counts and skip reasons rather than treating
compilation as runtime proof. Consumer tests do not directly invoke an integration factory or
construct Ratchet engine/store implementations.

## Intentionally not ported yet

Native/AOT compilation and native lifecycle/migration tests, EclipseLink/provider portability,
`src/runtime` multi-process/web/fault-injection/encryption/multiple-persistence-unit scenarios,
packaged-JAR/recovery ITs, and their process/TCP proxy harnesses are outside this pass. Spring-only
SQL initialization/deferred-data ordering and direct concurrent `SchemaMigrator` tests are also
not reproduced: this consumer owns host-table provisioning and checks migration through startup.
The broad Spring lifecycle suite's prototype/JDK-proxy-specific and runtime-fencing edge cases
remain separate follow-up work. This reactor provides JVM consumer and TCK evidence only.
