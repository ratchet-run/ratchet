# Ratchet MySQL DDL

- `mysql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is reserved for ordered upgrades tracked by external migration tooling or the opt-in `SchemaMigrator` utility.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention. Current set:
  - `V001__initial_schema.sql` — initial schema.
  - `V002__hot_cold_split.sql` — introduces `scheduler_job_queue` + `scheduler_business_key_reservation` and moves live state off `scheduler_job`.
- The ordered `V*` scripts must compose to the same schema shipped in `mysql-schema.sql`.
