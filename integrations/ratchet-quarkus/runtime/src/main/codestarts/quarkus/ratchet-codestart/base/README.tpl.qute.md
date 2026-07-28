## Ratchet

This starter adds Ratchet with the PostgreSQL store, the Quarkus PostgreSQL JDBC driver, and a REST endpoint that submits a background job.

Run the app with:

```bash
quarkus dev
```

In another shell, submit the sample job:

```bash
curl -X POST http://localhost:8080/reports
```

No datasource URL is set by default, so Quarkus Dev Services can provision a throwaway PostgreSQL database in dev mode. The starter sets `ratchet.schema.auto-migrate=true` so Ratchet creates its schema at startup for local development and CI.

The generated `JobsClassPolicy` allowlists the application package for submitted job method references. Ratchet requires this policy at startup so only intended application classes can be scheduled.

This starter uses `io.quarkus:quarkus-rest`. Do not combine it with the classic `io.quarkus:quarkus-resteasy` extension; Quarkus documents those REST stacks as incompatible.
