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
  -pl :ratchet-micronaut-processor,:ratchet-micronaut,:ratchet-micronaut-jpa,:ratchet-micronaut-mongodb,:ratchet-tck-api,:ratchet-tck-store,:ratchet-store-postgresql,:ratchet-store-mysql,:ratchet-store-oracle,:ratchet-store-sqlserver,:ratchet-store-mongodb \
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

EclipseLink/provider portability,
`src/runtime` multi-process/web/fault-injection/encryption/multiple-persistence-unit scenarios,
the broad runtime TCP proxy harness is outside this pass. Spring-only
SQL initialization/deferred-data ordering and direct concurrent `SchemaMigrator` tests are also
not reproduced: this consumer owns host-table provisioning and checks migration through startup.
The broad Spring lifecycle suite's prototype/JDK-proxy-specific and runtime-fencing edge cases
remain separate follow-up work. Process verification is described below; native execution needs a successful native build.

The SQL suite also compiles application beans using `ratchet-micronaut-processor` in both lanes.
`CircuitBreakerBeanTest` covers generated proxies, method and class bindings, inherited class
bindings, default service names, ignored failures, open-circuit rejection, half-open recovery and
reopening, and rejection before transaction advice. The transaction probe includes a positive
control that observes a real transaction on admitted calls. No Mongo circuit-breaker smoke test is
required.

## JVM and native process ITs

The launcher lives in `contracts`. It uses `target/classes` and the runtime dependency classpath
written at `package`. Test classes and test dependencies are excluded. With `-Pnative`, it launches
`target/sql-consumer` or `target/mongodb-consumer` instead. Failsafe runs the same ITs in both modes.
The application's main-source verification beans activate only for `consumer.native-check=true`.
They hold a non-daemon thread, print `RATCHET_CHECK <name>` per check, and exit with a failure reason
on an exception or timeout. Existing unit tests leave this property unset.

After staging as above, run all process ITs with these exact commands. JVM MN4 needs JDK 17+;
JVM MN5 needs JDK 25. Both native lanes need GraalVM 25. The formatter override lets MN4 run on 25.
`-Dtest=NoJvmTests` skips unit tests without skipping Failsafe.

```bash
# MN4 JVM SQL and Mongo
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn4 -Dstore=postgresql -pl :sql-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn4 -pl :mongodb-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
# MN5 JVM SQL and Mongo
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn5 -Dstore=postgresql -pl :sql-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn5 -pl :mongodb-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
# MN4 native SQL and Mongo
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn4,native -Dgoogle-java-format.version=1.35.0 -Dstore=postgresql -pl :sql-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn4,native -Dgoogle-java-format.version=1.35.0 -pl :mongodb-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
# MN5 native SQL and Mongo
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn5,native -Dstore=postgresql -pl :sql-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -ntp -f integrations/ratchet-micronaut/consumer-tests/pom.xml -Dmaven.repo.local="$staged_repo" -Pmn5,native -pl :mongodb-consumer -am clean verify -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false
```

Replace `-Dstore=postgresql` with `mysql`, `oracle`, or `sqlserver` for the other SQL stores.
On JDK 25, append `-Dgoogle-java-format.version=1.35.0` to MN4 JVM commands too.
To run a single IT, append the following selector to the relevant command above:

| IT | Selector | Checks |
| --- | --- | --- |
| SQL native consumer | `-Dit.test=SqlNativeConsumerIT -Dfailsafe.failIfNoSpecifiedTests=false` | Every discovery marker, captured record/POJO, recurring twice, REQUIRES_NEW, circuit advice. |
| SQL lifecycle | `-Dit.test=SqlNativeLifecycleIT -Dfailsafe.failIfNoSpecifiedTests=false` | Actual SIGTERM during work; empty allowed-packages fails startup. |
| SQL recovery | `-Dit.test=SqlRecoveryIT -Dfailsafe.failIfNoSpecifiedTests=false` | Kill worker; second node executes the original persisted job ID. |
| SQL migration | `-Dit.test=SqlNativeMigrationIT -Dfailsafe.failIfNoSpecifiedTests=false` | Empty DB, no init script, migration ledger and successful job. |
| Mongo native consumer | `-Dit.test=MongoNativeConsumerIT -Dfailsafe.failIfNoSpecifiedTests=false` | Every discovery and argument marker, recurring twice. |
| Mongo lifecycle | `-Dit.test=MongoNativeLifecycleIT -Dfailsafe.failIfNoSpecifiedTests=false` | Actual SIGTERM during work; empty allowed-packages fails startup. |
| Mongo recovery | `-Dit.test=MongoRecoveryIT -Dfailsafe.failIfNoSpecifiedTests=false` | Kill worker; original document identity reaches success after restart. |

Native applications enable third-party reachability metadata and disable fallback images. SQL excludes
Hibernate's BytecodeProvider service so Hibernate 6.6 and 7 use their no-op provider instead of Byte
Buddy. SQL Server includes all charsets. App metadata covers both SQL entities, the converter and its
value record, the captured record and POJO, and `META-INF/consumer-orm.xml`. Mongo only needs the
captured argument metadata. The Feature registrations are automatic and are not changed by this
harness. Construct arguments before submitting lambdas.

Recovery uses a one-second heartbeat, two-second orphan grace, and the minimum supported one-minute
orphan scan. The second process gets 100 seconds to observe success. Drain uses a five-second job and
a 20-second shutdown timeout. Process logs and Failsafe reports are retained under each module's
`target`. Runtime and native-image results must be reviewed separately from source checks.
