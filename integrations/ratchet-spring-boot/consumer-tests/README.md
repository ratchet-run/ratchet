# Spring Boot consumer tests

This reactor deliberately has no Ratchet parent POM. It represents a normal application that imports its own Spring Boot BOM and resolves Ratchet from a Maven repository. The SQL application uses Boot datasource settings and one selected Ratchet store. The MongoDB application uses Boot's normal MongoDB settings.

The SQL consumer profiles are selected with `-Dstore`:

| Store | Command suffix | Application profile | Prerequisites shown by the example |
| --- | --- | --- | --- |
| PostgreSQL | `-Dstore=postgresql` | `postgresql` | Default SQL example. |
| MySQL | `-Dstore=mysql` | `mysql` | `READ_COMMITTED`; the example also uses a UTC connection timezone. |
| Oracle | `-Dstore=oracle` | `oracle` | UTC Hibernate JDBC timezone. |
| SQL Server | `-Dstore=sqlserver` | `sqlserver` | UTC Hibernate JDBC timezone; no database-wide isolation change. |

Each run starts the selected real Testcontainers database and supplies only the usual Boot datasource URL, username, and password. The profile selects the matching application schema because Oracle and SQL Server do not accept the PostgreSQL/MySQL `CREATE TABLE IF NOT EXISTS` syntax.

On a host with limited Docker overlay storage, set `RATCHET_TEST_TMPFS=true` to put PostgreSQL, MySQL, or SQL Server data in a 16 GiB tmpfs-backed anonymous Docker volume. Oracle's image-seeded files require persistent storage across container creation and startup: set `RATCHET_TEST_DATA_DIRECTORY` to an existing writable host directory instead. The Oracle fixture creates a unique child directory, mounts it through a local-driver volume with image-data copy enabled, and removes that child after stopping the container. These options affect only test storage; they do not change database configuration.

To run against locally staged artifacts, first install Ratchet into an isolated Maven repository from the repository root:

```bash
staged_repo="$(mktemp -d)"
mvn -B -ntp -Dmaven.repo.local="$staged_repo" \
  -pl :ratchet-spring-boot-starter,:ratchet-spring-boot-starter-mongodb,:ratchet-tck-api,:ratchet-tck-store -am \
  install -Pgithub -DskipTests -Dspotbugs.skip=true -Dspotless.skip=true
```

Then run the independent consumer reactor. These commands do not use Ratchet's root parent:

```bash
mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=3.5.16 -Dstore=postgresql \
  -pl :sql-consumer -am verify

mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=4.1.1 -Dstore=mysql \
  -pl :sql-consumer -am verify

mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=4.1.1 \
  -pl :mongodb-consumer -am verify
```

Repeat the SQL command for `oracle` and `sqlserver`. The tests cover normal submissions, transaction commit and rollback behavior, recurring work, batches, workflows, signals, lifecycle handling, and the applicable API contracts. `verify` also runs the packaged-jar proof in a fresh JVM against the selected real database.

## Alternative JPA provider

Run the same PostgreSQL consumer with EclipseLink and Hibernate excluded:

```bash
mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=4.1.1 -Dstore=postgresql \
  -P eclipselink,postgresql -pl :sql-consumer -am clean verify
```

Repeat with Boot 3.5.16 and Java 17/21. The profile adds application-owned provider configuration
from `src/eclipselink` to both the test runtime and executable jar. Startup asserts Hibernate is
absent. It runs the existing contracts without excluding tests; the application disables shared
entity caching because Ratchet also uses native SQL. It selects EclipseLink 5 / JPA 3.2,
which supports `Instant` directly on both supported Java versions. Its datasource sets the PostgreSQL driver property
`stringtype=unspecified` for native-query null binding, matching the Jakarta EE suite. Ratchet
does not install these application-wide policies. The command explicitly selects the PostgreSQL
dependency profile with `-Dstore=postgresql`; the `eclipselink` profile adds only the
application-owned provider configuration.

The four existing skips in each consumer run are three contracts requiring a controllable clock
and one unsupported CRUD stale-write contract. Real-clock delay tests remain enabled. SQL runs
also verify transaction propagation, application entity/converter coexistence, metadata
forwarding, schema validation, and actual thread selection on Java 17/21.

CI runs the five stores on Boot 3.5.16 and 4.1.1 with Java 17 and 21. Four additional PostgreSQL
cells select EclipseLink. The root `unit-tests` job also runs the Spring auto-configuration tests
and architecture rules. Both jobs feed the required `CI required` check. The consumer matrix runs
on code PRs, pushes to `main`, and release verification; it skips docs-only PRs and the merge queue.
See [the CI workflow](../../../.github/workflows/ci.yml) for the commands
and uploaded Surefire/Failsafe reports. Run logs and one-off refactor measurements are review
artifacts, rather than a second copy of the user documentation.

## AOT and native verification

Use GraalVM 25 with `native-image` on `PATH` and set `JAVA_HOME` to that installation.
Stage the current Ratchet artifacts as above. The independent reactor explicitly binds Boot's
`process-aot`, GraalVM reachability metadata, native compilation, and Failsafe; it does not inherit
Boot's parent or its native profile.

```bash
# Generated Spring application running on the JVM, including the packaged/recovery harness.
mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=3.5.16 -Dstore=postgresql \
  -pl :sql-consumer -am -Paot clean verify

# Actual executable, with Testcontainers and assertions remaining in the JVM harness.
mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=3.5.16 -Dstore=postgresql \
  -pl :sql-consumer -am -Pnative clean verify

mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dmaven.repo.local="$staged_repo" -Dspring-boot.version=4.1.1 \
  -pl :mongodb-consumer -am -Pnative clean verify
```

Repeat SQL with `mysql`, `oracle`, and `sqlserver`, and both applications with each Boot version.
The required `spring-boot-native` CI matrix contains all ten combinations. Its logs, AOT metadata,
process logs, and Failsafe reports are uploaded even after failure. A native compilation alone is
not a passing database cell; every executable scenario must pass (six SQL process tests and four
MongoDB process tests per cell). The existing Java 17/21 consumer
matrix remains a separate JVM compatibility gate.

The evidence layers are distinct:

1. Starter unit tests inspect deterministic hints, dependency opt-in, nested submitters, disabled
   configuration, optional stores, unavailable optional application dependencies, and mapping
   ownership without constructing jobs or stores. Missing dependencies on registered beans or
   explicitly registered types remain errors.
2. `-Paot` launches the generated application with `spring.aot.enabled=true` in a fresh JVM.
3. `-Pnative` launches `target/sql-consumer` or `target/mongodb-consumer`. It checks native
   submissions and recovery after forced process termination, retaining the original persisted job.

Both AOT modes include the `verification` profile at build time; SQL also includes the selected
store profile. Shared verification code lives in an ordinary dependency JAR, exercising both
explicit-class and package-marker opt-in. Each application also contains unannotated, unmanaged
submitters under its own Boot package root, including nested, local, and anonymous classes.
The runtime `consumer.full-verify` switch chooses the broad scenario suite, while recovery uses
`consumer.verify-id`, `consumer.verify-submit`, and `consumer.block-job-id`. No database credentials
or worker process are needed during AOT. Lifecycle scenarios use `consumer.scenario` at runtime;
they are compiled into the same executable. Test encryption uses an explicitly non-secret fixture key.
Executables exercise method references, capturing and nested/local/anonymous submitters, records,
POJOs, inherited targets, recurring annotations, lifecycle hooks, lazy/prototype jobs, batch and
workflow jobs, results, signals, encrypted payloads, policy rejection, and virtual threads. SQL
also checks transactions, after-commit submission, capability proxies, and host UUID/time/converter
mappings. Feature assertions retain original job IDs and require durable success, including batch
children and recurring executions. Retry tests require persisted failure/success attempt history;
exhausted retries must retain the terminal failure and error. Policy rejection must have the
expected security exception and leave no stored job. The JVM harness checks raw ciphertext and
key identifiers; recovery repeats these checks before and after restarting the encrypted job.

Lifecycle tests force a startup-hook failure, then start a second application context in the same
process to detect leaked runtime ownership. Shutdown tests block a running job, prove close waits
without interruption, release the job, and verify its durable success after process exit. SQL also
covers JDK and class transaction proxies, prototype destruction, advised recurring methods, and
startup rejection of an unexposed recurring method behind a JDK proxy. The migration test installs
only the shipped V001 schema, seeds host and Ratchet data, then launches the application twice to
verify upgrade, checksums, data preservation, and idempotence.

## Spring runtime failure scenarios

The `runtime` profile adds real application tests for the Spring integration. It uses PostgreSQL
for JPA and both PostgreSQL and MongoDB for process recovery and encryption. These tests have a
separate four-cell Java 17/21 and Boot 3.5.16/4.1.1 CI gate; the existing five-store JVM and native
matrices remain in place. `runtime-boot4` supplies Boot 4's separate Flyway/Liquibase auto-configuration
modules. Use both profiles on Boot 4 and just `runtime` on Boot 3:

```bash
mvn -B -ntp -f integrations/ratchet-spring-boot/consumer-tests/pom.xml \
  -Dspring-boot.version=4.1.1 -Dstore=postgresql -Pruntime,runtime-boot4 verify \
  -Dtest=NoJvmTests -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dit.test=*RuntimeIT' -Dfailsafe.failIfNoSpecifiedTests=false
```

After staging current artifacts, this runs 20 PostgreSQL and four MongoDB scenarios. Omit the test
selectors to also run the existing JVM contracts and packaged application tests. The profile adds
web and migration-tool dependencies only to the JVM test classpath; additional Boot application
processes use that classpath. Packaged-JAR and native verification remain separate evidence.
The runtime SQL scenarios require PostgreSQL and Hibernate; use the existing `eclipselink` profile
for the provider portability suite.

The scenarios verify:

- Two real databases, persistence units, and transaction managers with a nonstandard primary bean
  name: selected metadata, migration isolation, application/job commit and rollback, secondary-unit
  writes, and explicit rejection of incoherent or ambiguous selections.
- Actual Flyway, Liquibase, and Boot SQL initialization from the shipped SQL scripts, validation-only
  Ratchet startup requested before other singletons, repeated startup, and initialization failure
  preventing worker startup. The fixture removes Ratchet's empty ledger from the exported SQL;
  Flyway and Liquibase use their own ledgers, while plain SQL provisioning runs only once.
- Lazy, prototype, and `FactoryBean` targets behind JDK proxies: no early construction, one durable
  non-retryable failure for an unexposed method, and exactly-once target destruction.
- HTTP transaction commit/rollback and an actual SIGTERM while work is running: the process waits
  for completion, persists success, and retains a usable datasource through the scheduler's stop hook.
- Simultaneous application processes: an idempotency race may return the documented duplicate-key
  conflict, but all subsequent submissions resolve to one original job. SQL also verifies one shared
  recurring definition and non-overlapping execution under a resource limit. After killing one
  worker, the already-running peer recovers the original job and produces one idempotent business
  effect; execution attempts may repeat under the at-least-once contract.
- Real TCP connection loss and reconnection without restarting Spring, bounded connection-pool
  exhaustion, and successful subsequent work. The SQL completion test also proves that a brief
  database interruption does not rerun a successfully executed payload. Success finalization retains
  its existing bounded retry policy. If both finalization paths are exhausted during a longer outage,
  the job remains `RUNNING` for recovery; monitor `ratchet.store.finalization.stuck`.
- Custom authorization, principal, masking, and serializer beans controlling actual job behavior;
  principal capture across worker threads; create/execute denial; key rotation across processes;
  and missing-key dead-lettering followed by explicit replay after key restoration.

Process logs are written to `target/runtime-*.log`. CI retains them alongside the build log and
Failsafe reports, including deliberate failure diagnostics. Expected migration, authorization,
missing-key, and duplicate-key errors are checked by scenario rather than treated as blanket log failures.
