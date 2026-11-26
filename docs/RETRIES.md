# Transient operation retry

The adapter can retry a failed Spark SQL statement or MLV on-demand refresh when
the error message matches an explicitly configured pattern. This is disabled by
default because `job_retry_on_messages` is empty.

```yaml
job_retry_on_messages:
  - 're:(?s)(?=.*TableMetadataManagerV202405\.alterTable)(?=.*BlobStorageException)(?=.*Status code 503)(?=.*ServerBusy)'
  - 're:(?s)(?=.*MLV_RUNTIME_ERROR)(?=.*Unable to execute the materialized lake view)'
enable_job_retry: true
job_retry_max_attempts: 3
job_retry_initial_wait_seconds: 30
job_retry_max_wait_seconds: 300
```

Patterns are case-sensitive plain substrings unless prefixed with `re:`. Total
attempts include the original execution. Matching failures use capped
exponential backoff; non-matching failures return immediately without retrying.
Use narrow patterns because replaying a non-idempotent SQL statement or API
operation can produce duplicate effects.

While a `streaming_table` model awaits its run, a `metadata_error` status row whose
failure matches a pattern is treated as a transient catalog read. The adapter keeps
polling with the same backoff until `statement_timeout`; any other failure still
fails the model.

---
[Home](../README.md) > [Documentation](./README.md)
