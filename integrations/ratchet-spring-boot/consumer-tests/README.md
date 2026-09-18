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
  install -DskipTests -Dspotbugs.skip=true -Dspotless.skip=true
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
does not install these application-wide policies. The explicit PostgreSQL profile is necessary
because activating another Maven profile disables its `activeByDefault` selection.

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
