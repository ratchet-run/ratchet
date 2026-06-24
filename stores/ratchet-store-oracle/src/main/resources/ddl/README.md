# Ratchet Oracle DDL

- `oracle-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is populated by the env-var-driven `SchemaMigrationLifecycleHook` (`RATCHET_SCHEMA_AUTO_MIGRATE=true`) or by external migration tooling that records each applied `V###`.
- Auto-migration supports Oracle Database 23ai and later. Other products require an explicit `RATCHET_SCHEMA_MIGRATION_DIALECT`. The Oracle migrator holds its lock on a second pooled connection, so the DataSource pool maximum must be at least 2.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention.
- The ordered `V*` scripts must compose to the same schema shipped in `oracle-schema.sql`.
