# Ratchet PostgreSQL DDL

- `postgresql-schema.sql` is the authoritative clean-install schema.
- `ratchet_schema_version` is reserved for ordered upgrades tracked by external migration tooling or the opt-in `SchemaMigrator` utility.
- Incremental scripts live under `ddl/migrations/` and use the `V###__description.sql` naming convention. Current set:
  - `V001__initial_schema.sql` — baseline single-table schema.
  - `V002__claim_due_index_alignment.sql` — realigns the executable claim index for due-time filtering under computed priority boosting.
  - `V003__business_key_reservations.sql` — moves active business-key uniqueness to `scheduler_business_key_reservation`.
  - `V004__add_caller_principal.sql` — adds the `caller_principal` audit column captured from `jakarta.security.enterprise.SecurityContext`.
  - `V005__hot_cold_split.sql` — splits `scheduler_job` into cold metadata + terminal state and a new `scheduler_job_queue` hot table; adds the `rec_status` shim for recurring masters.
- The ordered `V*` scripts must compose to the same schema shipped in `postgresql-schema.sql`.
