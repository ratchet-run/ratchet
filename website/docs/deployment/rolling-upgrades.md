---
title: Rolling Upgrades
description: Safely operate Ratchet 0.1.1 and a newer release during a bounded MySQL or PostgreSQL rollout.
---

# Rolling Upgrades

Ratchet supports a **bounded MySQL and PostgreSQL rolling-upgrade window from 0.1.1**. This is not a
promise that old and new nodes can use every feature interchangeably. It is a narrow bridge for
ordinary job lifecycle operations while the fleet moves to one version.

The automated compatibility contract resolves the released 0.1.1 API, store-core, MySQL, and
PostgreSQL store JARs from Maven Central. For each database, it loads those classes and the current
reactor classes in separate classloaders, points both stores at one database initialized with the
current schema, and proves both directions:

- 0.1.1 creates a job that the current store reads, claims, and completes.
- The current store creates a job that 0.1.1 reads, claims, and completes.
- The creating version reads back the other version's terminal status, payload, and result.

That contract covers the common persistence lifecycle. It does not certify newly introduced feature
semantics, arbitrary plug-ins, or a different database dialect.

## Upgrade order

1. **Back up the database and rehearse the rollout.** Verify restore time as well as backup
   creation.
2. **Apply compatible schema migrations before deploying the new binary.** During a rolling upgrade,
   a migration may add nullable columns, columns with safe defaults, indexes, or new tables. Do not
   start a mixed-version rollout when a migration renames or removes a column used by 0.1.1, changes
   an existing value representation, or adds a required value that 0.1.1 cannot write.
3. **Deploy the new version to a small part of the fleet.** Keep the 0.1.1 nodes available while
   common jobs drain through both versions.
4. **Keep the mixed window short and use only the common job lifecycle.** The compatibility contract
   covers create, read, claim, and successful finalization of ordinary MySQL and PostgreSQL jobs
   with payload shapes both versions understand.
5. **Drain and remove every 0.1.1 node.** Stop new work from reaching an old node, wait for its active
   executions to finish, and verify that no 0.1.1 process can claim another job.
6. **Only then enable new semantics.** In particular, do not submit work that depends on newer
   workflow-ordering behavior or newer recurring-misfire behavior until the fleet is entirely on the
   new version.

## Mixed-window rules

During the overlap:

- Keep producers on job options, payload formats, and result formats supported by 0.1.1.
- Do not use the mixed fleet to test a new schema. Validate the migration independently before the
  first new node starts.
- Treat encryption-envelope skew, repeated claim releases, or an old node that cannot hydrate a job
  as a rollout stop signal. Drain the lagging node instead of retrying incompatible work through it.
- Do not enable feature flags or application code that require a new durable enum value, payload
  shape, or store capability until all old nodes are gone.

## Version and database boundaries

The executable coexistence proof currently covers **0.1.1 and the current version on MySQL and
PostgreSQL**. It should not be read as evidence for Oracle, SQL Server, MongoDB, or an arbitrary pair
of future releases. Add or extend a live compatibility contract before widening that claim.

There is **no mixed-version guarantee for 0.1.0**. The TSID-to-UUIDv7 transition and the squashed
schema boundary make 0.1.0 intentionally incompatible with the 0.1.1 storage model. Upgrade from a
pre-0.1.1 deployment as a stopped migration with an explicit data-conversion plan; do not place
0.1.0 and newer nodes on the same Ratchet database.

## Rollback boundary

Rolling back to 0.1.1 is safe only while the database remains readable and writable by 0.1.1 and no
job has been submitted with new-only semantics. Once new workflow-ordering or recurring-misfire
behavior is enabled, finish the forward upgrade instead of returning old nodes to the fleet.

## Related guides

- [Database Setup](/deployment/database-setup) -- Applying and validating schema changes
- [Clustering](/deployment/clustering) -- Multi-node execution behavior
- [Monitoring](/deployment/monitoring) -- Signals to watch during a rollout
