# Ratchet PostgreSQL DDL

- `postgresql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is populated by the env-var-driven `SchemaMigrationLifecycleHook` (`RATCHET_SCHEMA_AUTO_MIGRATE=true`) or by external migration tooling that records each applied `V###`.
- Auto-migration supports PostgreSQL only. Other products (including CockroachDB) are unsupported because they lack `pg_advisory_lock` semantics.
- Migration scripts live under `ddl/migrations/postgresql/` and use the `V###__description.sql` naming convention:
  - `V001__initial_schema.sql` is the squashed baseline: the hot/cold job split, business-key reservations, the `caller_principal` audit column and the query-layer indexes. The historical pre-release `V*` chain was squashed before any production deployment, so no upgrade path from those intermediate versions is needed.
  - Each later script is an incremental step on top of the baseline.
- The ordered `V*` scripts must compose to the same schema shipped in `postgresql-schema.sql`.
