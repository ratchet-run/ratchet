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

Index space at 100M rows × 5 secondary indexes: ~4.8 GB.

