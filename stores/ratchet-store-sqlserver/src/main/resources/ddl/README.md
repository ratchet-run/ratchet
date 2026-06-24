# Ratchet SQL Server DDL

- `sqlserver-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is populated by the env-var-driven `SchemaMigrationLifecycleHook` (`RATCHET_SCHEMA_AUTO_MIGRATE=true`) or by external migration tooling that records each applied `V###`.
- Auto-migration is SQL Server only. The migrator serializes concurrent starters with a session-scoped `sp_getapplock` advisory lock.
- Migration scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention. Only one script ships today:
  - `V001__initial_schema.sql` — the squashed baseline. It is the full end-state schema (hot/cold job split, business-key reservations, `caller_principal` audit column, query-layer indexes), not an incremental step. The historical pre-release `V*` chain was squashed before any production deployment, so no upgrade path from those intermediate versions is needed.
- The baseline must match the schema shipped in `sqlserver-schema.sql`.
