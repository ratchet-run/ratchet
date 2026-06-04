---
sidebar_position: 12
title: Multi-Cell Deployment
description: Running several independent Ratchet cells side by side — one store per cell — for tenant isolation or throughput beyond a single shared store.
---

# Multi-Cell Deployment

A **cell** is one self-contained Ratchet deployment: its own application
instances, its own datasource, and its own schema. Running several cells
side by side gives you isolation or aggregate throughput that a single
shared store cannot, and it needs no special code. Ratchet already scopes
every uniqueness guarantee — job IDs, idempotency keys, business keys — to a
single store, so N stores are N independent schedulers that happen to run the
same engine.

This is a deployment topology, not a feature. There is no cross-cell API, no
cell registry, and no shared coordination layer. Your application decides
which cell a submission belongs to and talks to that cell's
`JobSchedulerService`.

## Cell vs. Cluster

These are different axes and they compose. A cluster scales one store
horizontally; a cell adds another store.

| | Shared-store cluster | Multi-cell |
|---|---|---|
| **Stores** | One store, many nodes | One store *per cell* |
| **Coordination** | Database claim (`SKIP LOCKED`) + optional `ClusterCoordinator` | None between cells |
| **Scales** | Worker capacity against one store | Aggregate store throughput and isolation |
| **Job visibility** | All nodes see all jobs | Each cell sees only its own jobs |
| **Failure blast radius** | One store is a shared fate | Each cell fails independently |

A single cell is almost always itself a cluster: multiple nodes sharing that
cell's store. "Multi-cell" describes the relationship *between* stores.

## When You Actually Need Cells

Reach for cells only when a shared-store cluster cannot give you what you
need. Most deployments never do.

**A shared-store cluster (optionally with worker tag affinity) is enough when:**

- One database can hold and serve your whole job population.
- You want different node groups to handle different work. Worker tag
  affinity routes jobs to eligible nodes *within one store* — tag a job with
  `withTags(...)` and constrain a node's claims with a
  [`NodeTagAffinityProvider`](/advanced/spi-implementation). No second store
  is required for workload routing.
- Tenants can coexist in one schema with application-level scoping.

**Move to cells when:**

- **You need a hard isolation boundary.** Separate schemas mean a tenant's
  data, retention policy, and schema evolution are physically independent,
  and one tenant's load cannot starve another's claims.
- **You have outgrown one store's write throughput.** A single store has a
  commit-throughput ceiling — under sustained enqueue load the bottleneck is
  transaction commit, not worker count, so adding nodes does not help past a
  point. Splitting the workload across cells multiplies the number of
  independent commit pipelines. See [Performance
  Tuning](/deployment/performance-tuning) for the single-store analysis.

Worker tag affinity and cells answer different questions. Tag affinity asks
*which node runs this job* inside one store. Cells ask *which store the job
lives in at all*. Use affinity first; reach for cells when a single store is
the wrong unit of isolation or throughput.

## Pattern 1: Cell per Tenant

Each tenant gets its own cell — its own schema, and usually its own
connection pool.

```
Tenant A  ->  Ratchet cell A  ->  schema_a
Tenant B  ->  Ratchet cell B  ->  schema_b
Tenant C  ->  Ratchet cell C  ->  schema_c
```

Use it for:

- **Compliance and data residency** — a tenant's jobs, payloads, and history
  never share a table with another tenant's.
- **A billing or quota boundary** — per-cell metrics attribute load directly
  to a tenant.
- **Independent schema evolution** — migrate or pause one tenant's cell
  without touching the others.

The cost is operational: more schemas to provision, migrate, and monitor.
Below a few dozen tenants this is usually worth it for the isolation; far
beyond that, a shared schema with application-level tenant scoping is the
more practical choice unless regulation forces separation.

## Pattern 2: Cell per Workload Domain

Each cell owns a class of work whose throughput and latency profile differs
from the others.

```
High-frequency reconciliation  ->  cell "reconcile"
Low-frequency reporting         ->  cell "reports"
User-facing notifications       ->  cell "notify"
```

Use it for:

- **Protecting latency-sensitive work from bulk work** — a reporting batch
  that saturates its own store's commit pipeline cannot slow down
  notification jobs in a different cell.
- **Tuning each store independently** — poll interval, batch size, retention,
  and pool sizing per domain.
- **Independent scaling** — grow the reconciliation cell's node count without
  touching the reporting cell.

This is the throughput-and-isolation pattern. When one shared store would
mix a heavy, commit-bound workload with a light, latency-sensitive one,
separate cells keep their commit pipelines apart.

## Cross-Cell Non-Guarantees

Cells are independent stores. Every Ratchet guarantee that is scoped to a
store stops at the cell boundary. Read these before adopting the model.

- **Job chains and cascades do not cross cells.** A `thenOnSuccess` /
  `thenOnFailure` chain, a workflow, a batch parent and its children, and a
  recurring master and its instances all live in one store. A step cannot
  depend on, trigger, or wait for a job in another cell.
- **Idempotency keys are per-cell.** The `idempotency_key` uniqueness
  constraint exists within one schema. The same key submitted to two cells
  produces two jobs.
- **Business-key deduplication is per-cell.** The active-unique business-key
  guard is a per-schema index. Two cells can each hold an active job with the
  same business key.
- **Tag-scoped queries and cancellation see one cell.** `JobQueryService`,
  `cancelJobsByTag`, and `cancelRecurringJobsByTag` operate against the cell
  whose store you queried. There is no fan-out across cells.
- **Signals are per-cell.** `deliverSignal` reaches only waiting jobs in the
  cell it is delivered to.

The application owns anything that must span cells. If you need a workflow
to cross a boundary, model it with an explicit cross-cell handoff in your own
code, not with a Ratchet chain.

## Routing Submissions

Because there is no cross-cell layer, routing is the application's
responsibility: choose the cell, then submit to that cell's
`JobSchedulerService`. The job never moves between cells afterward — it is
claimed, executed, retried, and archived entirely within the cell it was
created in.

Keep the routing key stable and outside the job payload. For cell-per-tenant
the key is the tenant ID; for cell-per-domain it is the workload class. A job
submitted to the wrong cell is not an error Ratchet can detect — it will run
in that cell against that cell's data.

## Sizing Reference

Pick the smallest model that satisfies your strongest requirement. Isolation
and throughput pull toward cells; everything else favors a single store.

| Isolation need | Throughput need | Recommended model |
|---|---|---|
| None (shared schema is fine) | Within one store's commit ceiling | **Single shared-store cluster** |
| Route work to specific nodes | Within one store's commit ceiling | **Shared-store cluster + worker tag affinity** |
| Physical per-tenant separation | Per tenant, modest | **Cell per tenant** |
| Keep workload classes apart | One workload is commit-bound | **Cell per workload domain** |
| Per-tenant separation *and* high aggregate volume | Exceeds one store | **Cells, sized per tenant or domain** |

Throughput planning starts with a single cell, because the ceiling is a
property of one store. Establish what one cell sustains for your workload and
hardware (the limiting factor is transaction commit, not worker count — see
[Performance Tuning](/deployment/performance-tuning)), then add cells when
your aggregate target or isolation requirement exceeds it. Each cell is
sized like any single deployment: see [Cluster
Configuration](/deployment/cluster-configuration) for per-cell node, pool,
and polling tuning.

## See Also

- [Deployment Overview](/deployment/overview) — application server, database, and module requirements
- [Clustering](/deployment/clustering) — scaling one store across nodes with `SKIP LOCKED`
- [Cluster Configuration](/deployment/cluster-configuration) — per-cell node, pool, and polling tuning
- [Performance Tuning](/deployment/performance-tuning) — the single-store commit-throughput ceiling
- [SPI Implementation](/advanced/spi-implementation) — `NodeTagAffinityProvider` for in-store workload routing
