# Ratchet PostgreSQL DDL

- `postgresql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is reserved for ordered upgrades tracked by external migration tooling or the opt-in `SchemaMigrator` utility.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention. Current set:
  - `V001__initial_schema.sql` — baseline single-table schema.
- The ordered `V*` scripts must compose to the same schema shipped in `postgresql-schema.sql`.
