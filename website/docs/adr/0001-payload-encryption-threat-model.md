# ADR 0001 — Payload encryption: threat model and protected-surface scope

- **Status:** Proposed
- **Date:** 2026-06-04
- **Supersedes:** the incubating `PayloadCipher` string-transform seam (uncommitted)
- **Roadmap:** "Payload encryption for sensitive job parameters" — this ADR is the
  prerequisite that roadmap item names ("Do not implement before: an ADR documenting
  the threat model … and which surfaces are actually protected vs out of scope").

## Why this ADR exists first

The roadmap blocks implementation on a threat model because the word "encrypted"
carries a promise. If we ship a feature called payload encryption without first
writing down what it does and does not defend against, operators will assume more
protection than the design gives them. A compliance reviewer who reads "encrypted at
rest" and then finds the job's target class, business key, and timing sitting in
plaintext has been misled, even if every byte we set out to protect is in fact
protected. This document fixes the boundary so the contract, the Javadoc, and the
marketing all describe the same thing.

It also records the architectural decisions that the boundary forces, because those
decisions are themselves security-relevant: the choice of AEAD over a reversible
transform, the per-surface AAD policy, and the failure classification are the
difference between a feature that protects data and one that only appears to.

## Context

Ratchet serializes job data to JSON and persists it. Compliance programs at regulated
enterprises require that sensitive job parameters not be readable in plaintext in the
store. There is no encryption primitive today.

Three constraints shape every option:

1. **The SQL stores cannot encrypt whole columns.** Payloads live in native
   `JSON`/`JSONB` columns, and the stores derive indexed *generated columns*
   (`target_class`, `method_name`, `trace_id_extracted`) from the JSON structure.
   Encrypting an entire column produces a value that is not valid JSON — the database
   rejects it — and blinds the generated columns that claim and query rely on.
   Encryption has to operate on leaf *values* and leave the surrounding JSON envelope
   and routing keys intact.

2. **The persistence path is not JPA.** The SQL stores bypass JPA `@Convert` and write
   through native row mappers; MongoDB uses a document mapper. A JPA
   `AttributeConverter` only ever sees the column value, never the owning row, so it
   cannot carry a job id, a surface label, or a per-job opt-in flag. Any context an
   encryption layer needs must be threaded through the row mappers, which do have the
   whole row.

3. **An incubating spike already exists and proved the placement model.** A branch
   wired a narrow `PayloadCipher { String encrypt(String); String decrypt(String); }`
   seam end to end: a JSON-walking placement layer that encrypts the `args` sub-tree,
   each parameter value, the result, the signal payload, and the workflow-condition
   predicate, while leaving routing metadata cleartext. The store TCK ran green on
   MySQL, PostgreSQL, and MongoDB and caught the native-row-mapper gap. The placement
   model works. The contract does not: `decrypt(encrypt(s)).equals(s)` is satisfied by
   ROT13 as validly as by AES-GCM. It guarantees neither confidentiality nor
   integrity, so it cannot honestly be called encryption.

## Decision

### 1. Authenticated encryption of leaf values, not a reversible transform

The public contract is a keyed AEAD transform (`byte[] → byte[]`), not a
string-to-string mapping. Authentication is non-negotiable: tampering with stored
ciphertext, or decrypting it under the wrong key or wrong associated data, must fail
loudly rather than return plausible-looking garbage. The reversible-transform contract
from the spike is withdrawn.

### 2. A closed set of protected surfaces; everything else cleartext by design

Encryption applies only to the surfaces in the table below. The set is closed and
enumerated in the API so the protected boundary is visible in code, not just prose.
The spike's enumeration is extended with the two callback-payload columns
(`on_success_payload`, `on_failure_payload`), which it encrypted implicitly through the
shared payload converter but never named — omitting them from the explicit set would
silently regress their confidentiality.

| Protected surface | Stored in | Additional authenticated data |
|---|---|---|
| `PAYLOAD_ARGS` | `payload` (`args` sub-tree only) | surface ∥ job id |
| `PARAM_VALUE` | `params` map values (keys stay cleartext) | surface ∥ job id |
| `RESULT` | `job_result` | surface ∥ job id |
| `ON_SUCCESS_PAYLOAD` | `on_success_payload` | surface ∥ job id |
| `ON_FAILURE_PAYLOAD` | `on_failure_payload` | surface ∥ job id |
| `SIGNAL_PAYLOAD` | `signal_payload` | surface only |
| `WORKFLOW_CONDITION_PREDICATE` | serialized predicate in `condition_expression` | surface only |

### 3. A per-surface AAD policy, computed by the framework

Additional authenticated data binds a ciphertext to its context, so a ciphertext lifted
out of one row and pasted into another fails to decrypt. AAD must be reconstructed
identically on read and write; it is never stored. Two surfaces cannot bind to the job
id and are documented as weaker bindings:

- **`SIGNAL_PAYLOAD`** is delivered by `deliverSignalByKey`, an atomic bulk `UPDATE`
  that writes one serialized payload to every waiting row matching the signal key.
  A single ciphertext lands on many job ids, so binding AAD to the job id would fail
  the authentication tag on every row but one. Signal payloads bind to the surface
  only.
- **`WORKFLOW_CONDITION_PREDICATE`** is decrypted at a call site
  (`WorkflowConditionEvaluator.invokePredicatePayload`) that has no job id in scope. A
  predicate belongs to exactly one parent job and is never shared, so surface-only
  binding is acceptable here.

To guarantee read/write symmetry, the framework derives the final AAD bytes per this
policy and hands them to the engine as an opaque `byte[]`. The engine does not compute
AAD and cannot reintroduce an asymmetry.

### 4. Three owners, three responsibilities

| Owner | Responsibility |
|---|---|
| Framework (store-core + RI) | Decide *which* leaves to protect; select the key for a write (`KeyProvider.currentKey()`); resolve the key for a read from the envelope's key id; own the versioned envelope and its framing; classify failures. |
| `PayloadEncryption` (engine) | A pure keyed AEAD transform over bytes. Owns the algorithm. Holds no key state and performs no key lookup. |
| `KeyProvider` | Key storage, the current/active key, lookup by id, and the lifecycle that makes rotation safe. |

This split keeps the default engine trivial to audit and lets a future KMS or Vault
adapter replace key handling without touching the engine, or replace the engine without
touching key handling.

### 5. Versioned, self-describing envelope

The framework wraps each ciphertext in a versioned envelope behind a collision-safe
marker. A stored value is treated as ciphertext only when the complete framing is
present, so plaintext that merely shares a prefix is never mistaken for an encrypted
value, and legacy plaintext rows coexist with encrypted rows during rollout. The
envelope carries:

- a version and the marker,
- the **algorithm id** (drives read-time engine dispatch),
- the **key id** (drives read-time key resolution),
- the **engine body** — the engine's opaque AEAD output, which carries its own nonce
  (nonce ∥ ciphertext ∥ tag). The engine owns nonce generation; the framing layer does
  not interpret the body,
- an **optional wrapped-key blob** (Q-C). Static and environment-variable providers
  leave it absent; KMS-style providers store the wrapped data-encryption-key here so
  envelope encryption works without a later format break.

Reserving the wrapped-key field in the first version is deliberate: the envelope is the
one part of the design that is genuinely expensive to change, because it is persisted.
Spending an optional, usually-empty field now is cheaper than versioning the format when
KMS arrives.

### 6. Failure semantics that cannot produce a retry storm

The classification distinguishes operator misconfiguration, transient infrastructure
failure, and permanently undecryptable data, because their correct remediations are
opposite.

| Failure | Classification | Disposition |
|---|---|---|
| Encryption enabled but no engine or key provider installed | Configuration error | **Fail the node at startup**; do not poll. Jobs stay `PENDING` and run once configuration is corrected. Never DLQ a job for a deploy mistake. |
| AEAD tag mismatch, corruption, wrong key | Poison data | **Non-retryable** → controlled `FAILED`/DLQ. A retry cannot fix corrupted ciphertext. |
| Key id referenced by a row is unknown to the provider (retired too early) | Poison data | **Non-retryable** → DLQ. Remediation: re-add the key, replay from DLQ. |
| Algorithm id referenced by a row has no installed engine | Poison data | **Non-retryable** → DLQ. Remediation: re-install the engine, replay. |
| Key provider transiently unreachable (KMS/HSM timeout or 5xx) | Transient | **Retryable** with backoff. A short outage must not permanently lose jobs. |

A decryption failure on the execution path must never leave a job claimed and stuck
`RUNNING`; it transitions through the controlled failure path. The internal mechanism
emits typed exceptions (not a bare `IllegalStateException`) so this routing is reliable.

### 7. Key and algorithm lifecycle: keep until drained

The envelope carries both a key id and an algorithm id, and they obey the same rule.
New writes use the current key and the active engine. Old keys and old engines must
remain resolvable until every row that references them has drained — completed and
aged out, or been re-encrypted. There is no automatic re-encryption of at-rest rows in
the first version; rotation affects new writes only, and old rows stay on their original
key until they age out. Retiring a key or removing an engine before its rows drain is
the operator error that produces the poison-data failures above, so retirement is gated
on a drain check.

The drain check is backed by an indexed `encryption_key_id` column written alongside the
payload (Q-B). The key id is not secret, so projecting it leaks nothing, and it turns the
check into a single indexed query —
`SELECT 1 FROM <jobs> WHERE encryption_key_id = ? AND status NOT IN (<terminal>) LIMIT 1`
— that CI can run before a key is retired. The column records the key used for the row's
creation-time payload. Surfaces written later in a job's life (result, signal, callbacks)
can reference a newer key if a rotation happened mid-flight, so retirement additionally
waits until the job retention window has elapsed since the key stopped being current,
which bounds how long any later-written surface under the old key can survive.

### 8. Opt-in, per job and globally

Encryption is off by default and produces byte-identical storage to a deployment with
no encryption configured. A job opts in with `JobBuilder.withEncryptedPayload()`, which
covers all of that job's surfaces. A deployment opts everything in with a global
`RatchetOptions` switch. The decision is persisted as a per-row flag so read paths know
whether to expect ciphertext, and so rotation and integrity tooling can find encrypted
rows.

The global switch is a single on/off covering every protected surface of an opted-in job
(Q-E). There is no per-surface global selection in the first version — no current use
case calls for "encrypt results but not arguments," and the closed protected-surface enum
already makes such selection an additive change later through a `PayloadEncryptionPolicy`
SPI rather than a breaking one.

A row flagged encrypted but read as unframed plaintext is surfaced as a warning — a
metric plus a throttled log — so a write-time misconfiguration or bug is visible (Q-D).
This is an operational integrity signal, not a security control: the flag is a cleartext
column, and the database-write attacker who could strip framing could also clear the
flag, so the warning is not tamper-evidence. Reads are not failed on this condition,
because flagged-plaintext rows are legitimately produced during the enable transition
before every node has the engine active, and hard-failing them would turn a rollout into
an outage.

### 9. AES-256-GCM as the reference engine; external KMS as adapters

The reference implementation is AES-256-GCM backed by a `KeyProvider`. It builds nonces
by the NIST SP 800-38D §8.2.1 deterministic construction (Q-A): a 96-bit nonce is a
64-bit per-process epoch drawn from `SecureRandom` at engine initialization, concatenated
with a 32-bit monotonic counter, and a fresh epoch is drawn if the counter would
overflow. Because every process draws a distinct epoch, nodes that share a key never
collide, and uniqueness per key is structural rather than probabilistic — there is no
per-key write ceiling for operators to size rotation against. (XChaCha20-Poly1305, whose
192-bit nonce makes random nonces safe without a counter, is a candidate alternative
engine the algorithm registry can carry later.)

Key-material adapters (environment variable, JCA `KeyStore`) and external key services
(AWS KMS, GCP KMS, HashiCorp Vault) ship as separate modules and must drop in without any
change to the SPI. External key services arrive as `KeyProvider` implementations using
envelope encryption (Q-C): the provider's "current key" is a freshly generated
data-encryption key returned in plaintext for local AES-GCM use plus a wrapped form to
store in the envelope; on read, the provider unwraps the stored blob to recover the data
key. Direct service-side encryption of payloads is not viable — AWS KMS `Encrypt` caps
plaintext at 4 KB while job payloads default to as much as 100 KB — which is why KMS is a
key provider, not an engine. That the adapters drop in without an SPI change is the test
of whether the SPI was drawn correctly.

## Threat model

### Protected against

- **Read access to the database at rest** — a DBA browsing rows, a SQL-injection bug
  that reads tables, a stolen or decommissioned disk. Protected-surface values are
  ciphertext.
- **Backup theft** — database dumps and snapshots carry ciphertext for protected
  surfaces.
- **Read-replica leak** — replicas inherit the ciphertext.
- **Casual log exposure of payload columns** — a log line or diagnostic dump that
  echoes a payload column shows ciphertext, not the value.

### Explicitly not protected against

- **Live worker memory.** A job's arguments are plaintext in the JVM heap while the job
  runs; decryption happens before execution by necessity. A memory dump of a running
  worker exposes the data.
- **The submitting application.** The caller that builds the job holds the plaintext
  before Ratchet ever sees it.
- **A compromised application server or node.** A node that runs jobs holds, or can
  reach, the keys; an attacker with code execution there can decrypt. Encryption defends
  the store, not the compute tier.
- **`KeyProvider` compromise.** The whole scheme is only as strong as the key provider
  behind it. Ratchet delegates key storage, access control, and rotation to the provider
  and makes no guarantee beyond what the provider gives.
- **Routing and correlation metadata, cleartext by design.** The fields below stay
  readable so claim, query, dedup, and tracing keep working. Anyone with store access
  can see *what* code a job runs and *how* it is correlated, just not the argument
  values.

  `target_class`, `method_name`, `method_descriptor`, `is_static`, `business_key`,
  `idempotency_key`, `caller_principal`, `tags`, `signal_key`, the W3C `trace_context`
  and its derived `trace_id_extracted` column, and **parameter keys** (only parameter
  *values* are encrypted).

- **Timing and structural shape.** When a job was scheduled, how long it ran, how often
  a recurring job fires, which branch of a workflow was taken — all observable from
  cleartext columns and timestamps.
- **Application-emitted free text.** `last_error` and anything the application writes to
  its own logs are outside the protected set. A worker that logs its arguments defeats
  the feature, and Ratchet cannot prevent that.
- **An attacker with write access to the database.** This is outside the trust boundary.
  The coexistence rule accepts an unframed value as legacy plaintext, so an attacker who
  can write rows can strip the framing and substitute plaintext; the framework will read
  it. Defending against database-write attackers is a different problem (write
  authorization, row signing) and is not in scope. A row flagged encrypted but stored as
  unframed plaintext is surfaced as a warning (see decision 8), but that is a
  misconfiguration detector, not a security control.

## Consequences

**Positive**

- "Encrypted at rest" means a specific, documented thing, and the protected set is
  visible in the API as an enum rather than buried in implementation.
- AEAD gives integrity as well as confidentiality: corruption and tampering of protected
  values are detected instead of silently propagated.
- The default-off, byte-identical-when-disabled posture means adopting the feature is a
  deliberate opt-in with no migration for deployments that do not want it.
- The three-owner split lets KMS and Vault arrive as adapters, not as breaking changes.

**Negative / accepted costs**

- Metadata leakage is real and permanent: the job's shape and correlation keys are
  legible to anyone with store access. For deployments where the *existence* or *target*
  of a job is itself sensitive, value-level encryption is insufficient, and that is
  stated rather than hidden.
- Per-surface AAD means signal and workflow-condition payloads have weaker anti-relocation
  binding than the row-bound surfaces. Documented, not silently uniform.
- Key rotation leaves old keys live until rows drain, so a retired key cannot be deleted
  immediately; operators carry a drain obligation.

**Risks**

- AES-GCM nonce reuse under one key is catastrophic. The reference engine avoids the
  random-nonce birthday bound by the deterministic construction in decision 9, so the
  residual risk shifts to per-process epoch uniqueness: two processes drawing the same
  64-bit epoch would collide on their first write under a shared key. A 64-bit epoch from
  `SecureRandom` keeps that probability negligible across realistic fleet lifetimes, but
  it depends on a correctly seeded RNG — a container image that ships a seeded or
  fork-shared RNG state would undermine it. The engine draws the epoch from
  `SecureRandom` precisely to avoid that, and this is the assumption to verify on any
  platform with questionable entropy at startup.

## Resolved decisions

Resolved 2026-06-04. These were the open forks; the resolutions are folded into the
decision sections above and recorded here for traceability.

- **Q-A — Nonce strategy (decision 9):** deterministic construction (per-process epoch ∥
  counter), not random-plus-rotation. Removes the unenforceable per-key write ceiling in
  a distributed fleet. *Worth a crypto-focused second review before the engine is built.*
- **Q-B — Rotation drain check (decision 7):** add the indexed `encryption_key_id` column
  in the initial encryption schema. Cheap, key id is not secret, avoids a second
  migration.
- **Q-C — KMS shape (decisions 5, 9):** reserve the optional wrapped-key field in the v1
  envelope now; KMS ships as a `KeyProvider` using envelope encryption. Forced by the
  4 KB KMS direct-encrypt limit against 100 KB payloads.
- **Q-D — Downgrade detection (decision 8):** warn (metric plus throttled log); do not
  hard-fail. Hard-fail would turn the enable transition into an outage; silent would hide
  misconfiguration.
- **Q-E — Global granularity (decision 8):** single on/off covering all surfaces;
  per-surface and per-field selection deferred to a future `PayloadEncryptionPolicy` SPI,
  which the protected-surface enum makes additive.

## Amended during implementation (2026-06-04)

Two AAD bindings in decision 3 were tightened once the call sites were verified against the
code; both are strictly stronger anti-relocation bindings and neither changes the engine
contract.

- **`WORKFLOW_CONDITION_PREDICATE` now binds the parent job id**, not the surface alone. The
  original rationale — that the decrypt site has no job id in scope — is incorrect: the predicate
  is evaluated through `WorkflowConditionEvaluator.evaluateCustomCondition(condition, parentJob)`
  and `evaluateResultCondition(...)`, both of which hold `parentJob`, and the write site
  `DefaultJobCreationService.createWorkflowBranch(parentId, …)` holds the same id. Binding it
  prevents a predicate ciphertext from being relocated between parent jobs.
- **`SIGNAL_PAYLOAD` now binds the signal key**, not the surface alone. A broadcast still writes
  one ciphertext to every waiting row, but every targeted row shares the signal key, so binding
  the key is broadcast-compatible while preventing a ciphertext from being replayed into a
  different signal's slot. Targeted delivery resolves the key from the awaiting job.

The framework still authenticates the full canonical envelope header (version, algorithm id, key
id, reserved wrapped-key field) as part of the AAD, so a tampered routing field fails the tag.

Per-job opt-in (`withEncryptedPayload()`) covers every job surface: payload args, parameter
values, callback payloads, the result (resolved by an indexed lookup of the row's flag), targeted
signal payloads (the awaiting job's flag), and the workflow-condition predicate (the parent job's
flag). Broadcast signal delivery cannot read a per-job flag because one ciphertext lands on many
rows, so it encrypts whenever an engine is configured rather than gating on the global switch
alone; over-encrypting a non-opted row is harmless because reads are marker-driven. Recurring
masters carry their own opt-in flag (`scheduler_recurring_job.encrypted_payload`): the stored
payload templates are encrypted at rest and every fired child inherits the flag.

## Hardening (2026-06-04, post-review)

An adversarial review of the implementation surfaced and closed the following:

- **Broadcast signals honor opt-in.** Previously a broadcast `deliverSignal` gated on the global
  switch only, so an opted-in waiting job could receive a plaintext signal payload. It now encrypts
  whenever an engine is installed; the `SIGNAL_PAYLOAD` binding (the signal key) already makes one
  ciphertext decryptable on every matching row.
- **Predicates and recurring jobs honor opt-in.** The workflow-condition predicate now follows the
  parent job's flag, and recurring master templates plus their fired children are encrypted (a new
  `encrypted_payload` column on `scheduler_recurring_job`). Both previously gated on the global
  switch only.
- **Hydration-time poison routes to the DLQ.** A payload that fails to decrypt while a claimed
  RUNNING job is being loaded is now moved to a terminal FAILED state and emits a `JobDlqEvent`,
  rather than being swallowed and left to stall until lease recovery — the `getJob()` path now
  matches the controlled failure path the signal-payload decrypt already used.
- **`encryption_key_id` is a hint, not a drain oracle.** The column is a denormalized summary of
  the most recently written surface's write key; the authoritative key for any value is the one
  named in its self-describing envelope. Drain-checking that retires a key must scan envelopes, not
  this column — the envelope, not the column, is the rotation-safe source of truth.

## Hardening (round 2, 2026-06-04, post second review)

A second adversarial review surfaced and closed the following:

- **Workflow predicate decryption no longer silently stalls the flow.** A predicate that fails to
  decrypt during branch evaluation is now classified. A transient key-provider outage is rethrown so
  the post-execution transaction rolls back and the branches are preserved (the parent has already
  completed, so there is no automatic re-evaluation — an operator re-triggers branch scheduling once
  keys are reachable). A poison/permanent failure leaves the broken branch unscheduled, so it is
  durably canceled and committed while sibling branches and the linear chain still proceed.
  Previously every predicate-evaluation failure tried to mark the already-completed parent FAILED,
  which rolled back and left the workflow silently stuck.
- **Signal-decrypt failures preserve the retry taxonomy.** Signal-payload decryption no longer wraps
  every failure in a non-retryable `IllegalArgumentException`. A transient
  `KeyProviderUnavailableException` now stays retryable; corrupt-ciphertext / forgotten-key poison
  still routes to the DLQ.
- **Chain steps inherit the parent opt-in.** A `.then(...)` chain off an opted-in job now stamps each
  step's `encrypted_payload` flag, so the row mapper encrypts the step's own args instead of
  persisting them as plaintext — the same gap the workflow-branch path had in round one.
- **Algorithm rotation is wired.** The engine registry dispatches reads by algorithm id, and the
  installer now accepts several engines when `RatchetOptions.encryption().writeAlgorithm` names which
  one writes — the rotation seam where an old engine stays installed to decrypt not-yet-drained rows
  while a new engine takes over writes. A single installed engine needs no selector; several engines
  without one fail loud rather than guessing.
- **Flagged-but-unframed reads are surfaced (Q-D, now implemented).** The hydrate paths in all three
  stores compare the `encrypted_payload` flag against the stored value's frame marker; a row flagged
  encrypted but read back as unframed plaintext increments a metric and a throttled log via
  `EncryptionIntegrity`, without failing the read.
- **`rcph:3:` is documented as a reserved marker prefix.** Frame detection is a prefix check, so a
  stored value beginning with the marker is treated as a v3 frame — fail-closed to poison, never
  silently as plaintext. The framework's encode path is the only writer of the prefix.
- **AAD-binding Javadoc corrected.** `SIGNAL_PAYLOAD` binds the surface and the signal key; the
  workflow predicate binds the surface and the parent job id — not "the surface only" as previously
  documented.

## Still open (do not block this ADR)

- Final `KeyProvider` read-resolution signature for the envelope-encryption case (it must
  carry the key id and the optional wrapped-key blob). The SPI is `@Incubating`, so this
  can be fixed when the first KMS adapter is built; only the persisted envelope field is
  locked now.

## References

- ROADMAP, "Payload encryption for sensitive job parameters."
- Incubating spike on branch `payload-cipher-spi`: `PayloadCipher`, `PayloadEncryptor`,
  `PayloadCipherHolder`, the store TCK `AbstractPayloadCipherStoreContract`, and the
  row-mapper wiring it validated.
- AWS Encryption SDK, additional-authenticated-data reference, for the AAD model.
