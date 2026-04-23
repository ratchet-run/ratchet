-- Ratchet MySQL V004 — capture caller principal at job creation (Jakarta Security).
--
-- Stores the authenticated caller principal (from
-- jakarta.security.enterprise.SecurityContext.getCallerPrincipal().getName()) on each newly
-- created job. Null when no security context is resolvable or the context has no authenticated
-- principal. No enforcement is performed at any layer — downstream consumers read this field for
-- audit or to build their own authorization policy.

ALTER TABLE scheduler_job
    ADD COLUMN caller_principal VARCHAR(255) NULL AFTER created_by;

INSERT INTO ratchet_schema_version (version, description)
VALUES ('004', 'Add caller_principal audit column to scheduler_job');
