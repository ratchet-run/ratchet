# ratchet-store-sqlserver

SQL Server store implementation for Ratchet.

## Operator debugging

UUIDv7 IDs are stored as native `uuid` (16 bytes). `psql` formats them as
hyphenated strings automatically:

```sql
SELECT * FROM scheduler_job WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654';
```

No view layer is needed (unlike the MySQL store) — `psql` handles `uuid`
natively in both query input and result output.

Index space at 100M rows × 5 secondary indexes: ~4.8 GB.

