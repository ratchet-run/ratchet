# ratchet-store-mongodb

MongoDB persistence backend for Ratchet.

## UUID representation requirement

This store **requires** `UuidRepresentation.STANDARD` on the codec registry of the supplied
`MongoClient`. The historical default `JAVA_LEGACY` swaps UUID byte order, which corrupts the
timestamp prefix on RFC 9562 §5.7 UUIDv7 round-trips and breaks time-correlation queries
(monotonicity is silently lost on read-back).

### Recommended: use the factory

```java
MongoClient client = MongoClientFactory.create("mongodb://localhost:27017");
```

### When supplying your own client

```java
MongoClientSettings settings = MongoClientSettings.builder()
    .applyConnectionString(new ConnectionString(uri))
    .uuidRepresentation(UuidRepresentation.STANDARD)  // REQUIRED
    .build();
MongoClient client = MongoClients.create(settings);
```

A `@PostConstruct` validator on `MongoJobStoreImpl` probes the codec registry at startup by
encoding a known UUID and inspecting the BSON binary subtype byte. Subtype 4 (RFC 4122 / standard)
passes; any other subtype throws `RatchetConfigurationException`. Silent UUID corruption is not
detectable post-rollout — the validator is intentionally fail-fast, not log-and-continue.

## Operator debugging

UUIDv7 IDs in BSON are stored as binary subtype 4. `mongosh` renders them as `UUID("...")`
strings; prefer it over driver-level inspection for ad-hoc queries.
