# ratchet-store-sqlserver

SQL Server store implementation for Ratchet.

## Operator debugging

UUIDv7 IDs are stored as `BINARY(16)` holding the canonical big-endian bytes —
the same byte order as the hyphenated string — not as `UNIQUEIDENTIFIER`. SSMS
and `sqlcmd` render the column as a `0x...` hex literal, so a raw
`SELECT * FROM scheduler_job` returns 16 binary bytes rather than a hyphenated
string.

Query by ID with a `0x` hex literal — drop the hyphens from the UUID:

```sql
-- 01902c4e-c4f3-7b8a-9d3e-fedcba987654
SELECT * FROM scheduler_job WHERE job_id = 0x01902C4EC4F37B8A9D3EFEDCBA987654;
```

Render stored IDs back to that hex form for comparison with `CONVERT(..., 2)`:

```sql
SELECT CONVERT(CHAR(32), job_id, 2) AS job_id_hex, business_key, terminal_status
FROM scheduler_job;
```

Do **not** cast `job_id` through `UNIQUEIDENTIFIER` (e.g.
`CONVERT(UNIQUEIDENTIFIER, job_id)`): that applies SQL Server's mixed-endian Guid
byte swap and yields a string that matches no stored row — the reason this store
uses `BINARY(16)` rather than `UNIQUEIDENTIFIER` in the first place. Tools that
handle binary IDs correctly (Hibernate, JDBC `getObject(..., UUID.class)`, a
UUID-aware viewer) can query the tables directly.

## Server configuration

Ratchet's claim contract requires the `READ COMMITTED` transaction isolation
level. SQL Server's own default is `READ COMMITTED`, but **Open Liberty's SQL
Server data-store helper overrides an unset datasource to
`TRANSACTION_REPEATABLE_READ`**. Under that default Ratchet's startup isolation
check fails, so when deploying this store on Open Liberty pin the level
explicitly:

```xml
<dataSource id="RatchetDS" jndiName="jdbc/RatchetDS" transactional="true"
            isolationLevel="TRANSACTION_READ_COMMITTED">
  ...
</dataSource>
```

Other servers either default SQL Server to `READ COMMITTED` (GlassFish, Payara)
or set it during datasource setup (WildFly), so no extra configuration is needed
there.

Index space at 100M rows × 5 secondary indexes: ~4.8 GB.

