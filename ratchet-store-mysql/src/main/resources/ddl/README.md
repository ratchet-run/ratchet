# Ratchet MySQL DDL

- `mysql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is populated by the env-var-driven `SchemaMigrationLifecycleHook` (`RATCHET_SCHEMA_AUTO_MIGRATE=true`) or by external migration tooling that records each applied `V###`.
- Auto-migration supports MySQL ≥ 8 and MariaDB. Other products (including CockroachDB) are unsupported.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention.
- The ordered `V*` scripts must compose to the same schema shipped in `mysql-schema.sql`.
