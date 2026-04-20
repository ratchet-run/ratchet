# Ratchet PostgreSQL DDL

- `postgresql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is reserved for ordered upgrades tracked by external migration tooling or the opt-in `SchemaMigrator` utility.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention. Current set:
  - `V001__initial_schema.sql` — baseline single-table schema.
  - `V002__claim_due_index_alignment.sql` — realigns the executable claim index for due-time filtering under computed priority boosting.
  - `V003__business_key_reservations.sql` — moves active business-key uniqueness to `scheduler_business_key_reservation`.
- The ordered `V*` scripts must compose to the same schema shipped in `postgresql-schema.sql`.
