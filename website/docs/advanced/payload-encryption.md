---
sidebar_position: 6
title: Payload Encryption
description: Encrypt sensitive job parameters and results at rest with authenticated encryption
---

# Payload Encryption

Ratchet can encrypt sensitive job data at rest. When it is enabled, a job's payload (its
parameter values) and its result are stored as authenticated ciphertext, while the columns
the scheduler needs to claim, query, and correlate jobs stay in cleartext. The word
"encrypted" carries a promise, so this page is specific about what the feature protects and
what it leaves readable.

The mechanism is authenticated encryption (AEAD): every value is encrypted under a key and
bound to the row and column it belongs to, so decrypting corrupted, tampered, or
moved-around bytes fails rather than returning a wrong value. Two pieces plug in: a
`PayloadEncryption` engine that does the AEAD transform, and a `KeyProvider` that owns the
keys. The `ratchet-encryption` module ships a reference engine and a static-key provider so
a deployment can turn the feature on without writing code.

## Quick start

Add the module:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-encryption</artifactId>
</dependency>
```

Supply key material through the environment. Keys are 32-byte (AES-256) values, base64
encoded, given as a comma-separated list of `keyId:base64Key` entries:

```bash
# The current key is the write key; older ids stay available for decryption.
export RATCHET_ENCRYPTION_KEYS="2026-06:$(openssl rand -base64 32)"
```

Then switch encryption on for the whole deployment:

```java
RatchetOptions.builder()
    .encryption(e -> e.enabled(true))
    .build();
```

With keys configured and `ratchet-encryption` on the classpath, Ratchet builds the
reference AES-256-GCM engine and a `SecretKeyProvider` from those keys for you. Keys are
read from the environment rather than from `RatchetOptions` on purpose: secret material
belongs in your secrets manager, not in an in-memory options object.

::: tip Keys live in the environment
You can also use the system properties `ratchet.encryption.keys` and
`ratchet.encryption.current-key` instead of the `RATCHET_ENCRYPTION_*` variables. With a
single key the current-key id may be omitted.
:::

## Choosing the scope

The global switch and the per-job opt-in answer two different questions.

Turn the global switch on to encrypt every job:

```java
RatchetOptions.builder()
    .encryption(e -> e.enabled(true))
    .build();
```

Or leave the global switch off and encrypt only the jobs that carry sensitive data:

```java
scheduler.enqueue(() -> billingService.charge(cardToken))
    .withEncryptedPayload()
    .submit();
```

Either way, an engine and a key provider must be installed. A per-job opt-in whose
deployment has neither fails when the job is submitted rather than persisting data it was
asked to protect; enabling the global switch with nothing installed fails at startup
instead. See [Failure behavior](#failure-behavior) below.

## What is protected, and what is not

Encryption covers two surfaces: the **parameter values** of a job and its **result**.
Everything else stays readable so the scheduler keeps working. Be explicit with your
compliance reviewers about this boundary.

**Protected against**

- Read access to the database at rest: a DBA browsing rows, a SQL-injection bug that reads
  tables, a stolen or decommissioned disk. Protected values are ciphertext.
- Backup and snapshot theft: dumps carry ciphertext for the protected surfaces.
- Read-replica leaks: replicas inherit the ciphertext.
- A payload column echoed into a log line or diagnostic dump shows ciphertext, not the
  value.

Deployments using framework extensions get one more surface: per-namespace extension
state (`scheduler_job_extension_state`) is encrypted whenever the **global switch** is on
— it has no per-job opt-in, because the state is written by extension code after
submission, not by the submitter. Archiving copies the state rows as stored, so encrypted
state stays ciphertext on the archive row. The full protected-surface table and rationale
live in the payload-encryption ADR in the repository
(`website/docs/adr/0001-payload-encryption-threat-model.md`).

**Not protected against**

- Live worker memory. A job's arguments are plaintext on the JVM heap while the job runs;
  decryption happens before execution by necessity.
- The submitting application, which holds the plaintext before Ratchet ever sees it.
- A compromised node. A worker that runs jobs holds, or can reach, the keys. Encryption
  defends the store, not the compute tier.
- `KeyProvider` compromise. The scheme is only as strong as the provider behind it.
- Routing and correlation metadata, cleartext by design. `target_class`, `method_name`,
  `business_key`, `idempotency_key`, `caller_principal`, `tags`, `signal_key`, the W3C
  trace context, and parameter **keys** (only parameter *values* are encrypted) stay
  readable so claim, query, dedup, and tracing keep working.
- Timing and structural shape: when a job ran, how long it took, how often a recurring job
  fires, which workflow branch was taken.
- Application-emitted free text such as `last_error` or anything your job writes to its own
  logs. A worker that logs its arguments defeats the feature, and Ratchet cannot prevent
  that.
- An attacker with write access to the database, which is outside the trust boundary.

## Choosing an engine

The reference engine is AES-256-GCM, which is the right default for most deployments. The
`ratchet-encryption` module also ships an XChaCha20-Poly1305 engine, whose larger random
nonce suits very high write volumes under a single key. To use it, provide it as a bean
(see below) and name it as the write algorithm:

```java
RatchetOptions.builder()
    .encryption(e -> e.enabled(true).writeAlgorithm("XChaCha20-Poly1305"))
    .build();
```

Each stored value records the id of the engine that wrote it, so reads always decrypt with
the matching algorithm even after you change the write engine.

## Backing keys with a KMS

The reference `SecretKeyProvider` holds keys in memory from the environment. To source keys
from a KMS or `KeyStore` instead, provide your own `KeyProvider`, and provide a
`PayloadEncryption` engine alongside it. Application beans take precedence over the
reference stack.

```java
@Produces
@ApplicationScoped
public KeyProvider kmsKeyProvider() {
    return new MyKmsKeyProvider(kmsClient);
}

@Produces
@ApplicationScoped
public PayloadEncryption engine() {
    return new AesGcmPayloadEncryption(new SecureRandom(), nodeEntropy());
}
```

For a KMS that hands back wrapped data keys rather than raw key bytes, implement
`WrappedKeyProvider`, the `KeyProvider` variant built for that pattern.

::: warning Install the engine and the provider together
A `KeyProvider` bean with no `PayloadEncryption` engine bean (or the reverse) is a
misconfiguration and aborts startup. The reference stack is only used when the application
provides neither.
:::

## Key rotation

Rotation is additive. Add the new key, point the current-key id at it, and keep the old
keys available:

```bash
export RATCHET_ENCRYPTION_KEYS="2026-06:$OLD_KEY,2026-09:$NEW_KEY"
export RATCHET_ENCRYPTION_CURRENT_KEY="2026-09"
```

New writes use the current key; existing rows keep their old key id and decrypt under the
old key. Keep a retired key resolvable until every row written under it has been rewritten
or deleted. A reference to a key the provider has permanently forgotten is poison data that
no read can recover.

## Failure behavior

Encryption fails loud and never falls back to plaintext. At startup the deployment is in one
of three states:

- **Disabled.** No engine or provider is installed and the global switch is off. Jobs are
  stored as before. An opted-in job later fails at write time rather than silently writing
  plaintext.
- **Enabled.** An engine and a provider are installed. New writes use the configured write
  algorithm, or the only engine when just one is installed.
- **Misconfigured.** The global switch is on but nothing is installed, only one of the
  engine and provider is present, or several engines are installed without a
  `writeAlgorithm` to choose between them. Startup aborts with
  `EncryptionConfigurationException`.

At read time, a value that cannot be decrypted because the bytes are corrupt, the key is
wrong, or the binding does not match fails the job instead of looping into a retry storm.

## See also

- [JobBuilder.withEncryptedPayload](/api-reference/job-builder#withencryptedpayload)
- [PayloadEncryption, KeyProvider, and EncryptionContext SPIs](/api-reference/spi-interfaces#payloadencryption)
