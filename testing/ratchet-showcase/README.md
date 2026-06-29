# Ratchet Showcase

`ratchet-showcase` is a runnable Jakarta EE WAR that demonstrates Ratchet with an order-fulfillment dashboard. It includes a seeded order stream, scenario buttons, fraud-review signals, retryable failures, resource limits, Ratchet activity views, job details, and Prometheus-format metrics.

## Build

Use `-am` when building this module from a partial reactor so Maven compiles sibling Ratchet artifacts from the worktree instead of using stale local artifacts.

```bash
mvn -pl :ratchet-showcase -am test
mvn -pl :ratchet-showcase -am spotless:check
mvn -pl :ratchet-showcase -am -DskipTests package
```

Generated output under `target/` and `.flattened-pom.xml` is ignored and should not be committed.

## Run Locally

The only prerequisite is Docker. By default the launcher starts a throwaway
database container, runs the demo against it, and removes it on exit.

Build the reactor once so the sibling artifacts are in your local repository:

```bash
mvn install -DskipTests
```

Then launch the showcase with a single command — it packages the WAR and runs
the server, starting (and later removing) its own database:

```bash
mvn -pl :ratchet-showcase \
  -P wildfly-managed,postgresql \
  -DskipTests \
  -Dshowcase.http.port=4176 \
  -Dshowcase.context.path=/app \
  -Dwildfly.management.port=19993 \
  -Dwildfly.https.port=18446 \
  package exec:exec@run-showcase
```

Open `http://127.0.0.1:4176/app/`. Stop with Ctrl-C; the server and the
database container both shut down.

> Don't add `-am` to the run command: `exec:exec@run-showcase` would then also
> fire on the parent reactor module, which has no executable configured, and the
> build fails. Use `mvn install` (above) to refresh siblings instead.

The embedded container publishes on a random loopback port, so a Postgres,
MySQL, Oracle, SQL Server, or Mongo already running on the standard port won't collide.

### Using your own database

Set `-Dshowcase.db.embedded=false` (or `SHOWCASE_DB_EMBEDDED=false`) and point
the launcher at an existing database. Supplying a connection target also
disables the embedded container on its own. Connection defaults:

- PostgreSQL: `POSTGRES_HOST=localhost`, `POSTGRES_PORT=5432`, database/user/password `ratchet`
- MySQL: `MYSQL_HOST=localhost`, `MYSQL_PORT=3306`, database/user/password `ratchet`
- Oracle: `ORACLE_HOST=localhost`, `ORACLE_PORT=1521`, `ORACLE_SERVICE=FREEPDB1`, user/password `ratchet`
- SQL Server: `SQLSERVER_HOST=localhost`, `SQLSERVER_PORT=1433`, `SQLSERVER_DATABASE=ratchet`, `SQLSERVER_USER`/`SQLSERVER_PASSWORD` (the database must have `READ_COMMITTED_SNAPSHOT` enabled)
- MongoDB: `MONGO_URI=mongodb://localhost:27017`, database `ratchet`

```bash
POSTGRES_HOST=db.internal POSTGRES_PORT=5432 \
mvn -pl :ratchet-showcase \
  -P wildfly-managed,postgresql \
  -DskipTests -Dshowcase.http.port=4176 -Dshowcase.context.path=/app \
  -Dwildfly.management.port=19993 -Dwildfly.https.port=18446 \
  package exec:exec@run-showcase
```

If the script is hard-killed (SIGKILL) the cleanup trap can't run; remove any
stray container with `docker rm -f $(docker ps -q --filter name=ratchet-showcase-db-)`.

The WAR can be packaged for these managed server profiles:

- `wildfly-managed`
- `payara-managed`
- `openliberty-managed`
- `glassfish-managed`

Combine one server profile with one store profile:

- `postgresql`
- `mysql`
- `oracle`
- `sqlserver`
- `mongodb`

GlassFish 8 requires JDK 21 or newer.

The embedded Oracle (`gvenzl/oracle-free`) and SQL Server (`mcr.microsoft.com/mssql/server:2022-latest`) images are larger than the Postgres/MySQL/Mongo images and take longer to become ready on first pull. The embedded SQL Server database is created with `READ_COMMITTED_SNAPSHOT` enabled, which Ratchet's claim path requires.
