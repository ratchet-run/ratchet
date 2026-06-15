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

The launcher expects an already-running database. Defaults are:

- PostgreSQL: `localhost:5432`, database/user/password `ratchet`
- MySQL: `localhost:3306`, database/user/password `ratchet`
- MongoDB: `mongodb://localhost:27017`, database `ratchet`

Example WildFly/PostgreSQL run:

```bash
POSTGRES_PORT=5432 \
mvn -pl :ratchet-showcase \
  -P wildfly-managed,postgresql \
  -DskipTests \
  -Dshowcase.http.port=4176 \
  -Dshowcase.context.path=/app \
  -Dwildfly.management.port=19993 \
  -Dwildfly.https.port=18446 \
  exec:exec@run-showcase
```

Open `http://127.0.0.1:4176/app/`.

The WAR can be packaged for these managed server profiles:

- `wildfly-managed`
- `payara-managed`
- `openliberty-managed`
- `glassfish-managed`

Combine one server profile with one store profile:

- `postgresql`
- `mysql`
- `mongodb`

GlassFish 8 requires JDK 21 or newer.
