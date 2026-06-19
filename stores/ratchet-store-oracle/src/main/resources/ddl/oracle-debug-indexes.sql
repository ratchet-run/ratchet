-- Optional debug indexes for ratchet-store-mysql.
--
-- These indexes are NOT applied by the standard mysql-schema.sql. They support
-- ad-hoc queries against scheduler_job by target class or method name during
-- debugging, support escalations, or one-off audits. They add write amplification
-- on every job insert/update, so they are off by default.
--
-- Apply only if you actively query scheduler_job by target_class or method_name.

CREATE INDEX idx_target_class ON scheduler_job (target_class);
CREATE INDEX idx_method_name ON scheduler_job (method_name);
