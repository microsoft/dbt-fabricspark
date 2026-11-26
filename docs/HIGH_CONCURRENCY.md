# High-concurrency Livy

By default the adapter uses Fabric's [high-concurrency Livy API](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)
(`high_concurrency: true`). Each active dbt connection borrows an exclusive HC
session — and therefore its own REPL — from a process-local pool capped at
`threads`. Statements from different REPLs execute in parallel inside the same
Spark application, so increasing `threads` buys us throughput. When dbt moves
from metadata discovery to model workers or hooks, released connections return
their REPLs to the pool for the next phase instead of acquiring additional HC
IDs.

When `reuse_session: true`, the underlying Livy session stays warm between dbt
invocations (until Fabric's `spark.livy.session.idle.timeout` elapses). HC REPL
IDs are persisted beside `session_id_file` and reclaimed by later invocations
when they are still idle, so repeated runs reuse warm REPLs rather than
accumulating unused ones. Access to the registry is locked across processes.

Set `high_concurrency: false` to fall back to the single-session-per-process mode, where one Livy session
serves every thread and statements queue FIFO inside — useful as an escape hatch
when debugging any problems with the high-concurrency API.

Fabric packs REPLs onto one underlying Livy session up to
**`spark.highConcurrency.max`** (the "dynamic session sharing" limit; see the
["Limits"](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy#key-concepts)
note in the Microsoft Learn HC Livy docs), whose **default is 5**. A single dbt
process holds at most **`threads`** REPLs: main-thread, metadata, model, and hook
connections all lease from the same bounded pool. The adapter serializes new
acquisitions so Fabric can pack them under the shared `sessionTag`; that tag is
still a service-side packing hint rather than a strict lock.

When `threads` exceeds the Fabric cap, dbt still works correctly — Fabric spins
up another underlying Livy session to host overflow REPLs, and the same
`sessionTag` makes future acquisitions attach to whichever underlying session
has room.

What that means in practice:

| Property                                              | Shared across underlying sessions? |
| ----------------------------------------------------- | ---------------------------------- |
| OneLake Delta tables (dbt model outputs)              | Yes — same lakehouse storage       |
| Catalog / metastore (`SELECT FROM <other_model>`)     | Yes — same Fabric catalog          |
| Temp views (`CREATE TEMPORARY VIEW ...`)              | No — REPL/session-local            |
| Session-level Spark configs (`SET spark.sql.X = ...`) | No                                 |
| Cached datasets / UDFs / broadcast vars               | No                                 |

Because dbt-fabricspark materializations always write permanent Delta /
MLV objects, model-to-model `ref`s resolve correctly regardless of which
underlying session produced or consumes the table. Macros that depend on
session-local state (temp views, in-session configs) are the only ones
that could surprise — none ship with this adapter today.

Cost tradeoff: each additional underlying Livy session is a separate
Spark cluster billed for the duration of the run plus the
`spark.livy.session.idle.timeout` afterwards. The single-session ceiling is
**`threads ≤ spark.highConcurrency.max`** — i.e. **`threads ≤ 5`** at the
default cap.

To run `threads > 5` inside one billed session, raise the cap in your profile —
Fabric honors it at session-create time, no Environment required:

```yaml
spark_config:
  name: <your-app-name>
  conf:
    spark.highConcurrency.max: "50"   # keep ≥ threads
```

Alternatively, attach a Fabric
[Environment](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment)
whose Spark properties set `spark.highConcurrency.max`. Keep the cap ≥ `threads`
so a single build stays on one billed session; raise `threads` only when the extra
parallelism beats the extra compute spend.

High-concurrency has no effect in local mode as this is a Fabric specific construct.

---
[Home](../README.md) > [Documentation](./README.md)
