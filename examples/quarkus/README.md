# Ratchet on Quarkus: runnable example

A minimal Quarkus application that runs a Ratchet job. It needs no database install and no schema
setup: in dev mode Quarkus Dev Services provisions a throwaway PostgreSQL container, and Ratchet
creates its own schema on startup.

This mirrors the [Quarkus Deployment quickstart](../../website/docs/deployment/quarkus.md).

## What it shows

- The four dependencies for the SQL flavor (`pom.xml`).
- Two-line configuration: a datasource kind and `ratchet.schema.auto-migrate=true`
  (`src/main/resources/application.properties`).
- A job (`Reports#rebuild`) submitted as a method reference (`ReportResource`).
- The mandatory class allowlist (`JobsClassPolicy`), without which Ratchet refuses to start.

## Run it

You need JDK 21 and a running Docker or Podman (for Dev Services).

```bash
quarkus dev
# or: mvn quarkus:dev
```

In another shell:

```bash
curl -X POST http://localhost:8080/reports
```

The log shows Dev Services starting PostgreSQL, Ratchet applying its migrations, then `report
rebuilt` when the job runs.

## Notes

- The dependencies use `0.2.1-SNAPSHOT`. Bump to the release you are on.
- On Docker Engine 29 or newer, if Dev Services reports `client version ... is too old`, create
  `~/.docker-java.properties` containing `api.version=1.44`.
- For a production datasource, schema strategy, and the MongoDB flavor, see the
  [Quarkus deployment guide](../../website/docs/deployment/quarkus.md).
