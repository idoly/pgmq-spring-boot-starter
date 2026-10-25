# PGMQ 1.13.1 SQL API coverage

`PgmqTemplate` maps the 42 ordinary public SQL function names in bundled PGMQ 1.13.1, including
its deprecated compatibility entry points. The trigger-only `notify_queue_listeners` runs through
the trigger installed by `enableNotifyInsert`. Underscore-prefixed functions stay inside the SQL
implementation. The SQL resource remains the authority for database behavior.

## Queue and message functions

| SQL function | Java API |
| --- | --- |
| `acquire_queue_lock` | `acquireQueueLock` |
| `format_table_name` | `formatTableName` |
| `validate_queue_name` | `validateQueueName` |
| `create` | `create` |
| `create_non_partitioned` | `createNonPartitioned` |
| `create_unlogged` | `createUnlogged` |
| `create_partitioned` | `createPartitioned`, with one through four arguments |
| `convert_archive_partitioned` | `convertArchivePartitioned`, with one through four arguments |
| `drop_queue` | `dropQueue`, including the deprecated boolean overload |
| `detach_archive` | deprecated `detachArchive` |
| `purge_queue` | `purgeQueue` |
| `list_queues` | `listQueues` |
| `read` | `read` with default or explicit JSON conditional |
| `read_with_poll` | `readWithPoll`, with three through six arguments |
| `read_grouped` | `readGrouped` |
| `read_grouped_with_poll` | `readGroupedWithPoll`, with three through five arguments |
| `read_grouped_rr` | `readGroupedRoundRobin` |
| `read_grouped_rr_with_poll` | `readGroupedRoundRobinWithPoll`, with three through five arguments |
| `read_grouped_head` | `readGroupedHead` |
| `read_grouped_head_with_poll` | `readGroupedHeadWithPoll`, with three through five arguments |
| `send` | `send`: headers, integer delay and absolute timestamp overloads |
| `send_batch` | `sendBatch`: headers, integer delay and absolute timestamp overloads |
| `archive` | `archive`: single message and message ID list |
| `delete` | `delete`: single message and message ID list |
| `pop` | `pop`: default or explicit quantity |
| `set_vt` | `setVt`: single/batch IDs, integer seconds/absolute timestamp |
| `metrics` | `getMetrics` |
| `metrics_all` | `getMetricsAll` |
| `create_fifo_index` | `createFifoIndex` |
| `create_fifo_indexes_all` | `createFifoIndexesAll` |

Read, polling, pop, partition and archive conversion overloads omit unspecified SQL arguments,
so PostgreSQL supplies the remaining defaults. A zero polling duration retains the documented
single-read behavior. Explicit options are passed through unchanged. For example, the upstream
partition signature is:

```sql
create_partitioned(
    queue_name TEXT,
    partition_interval TEXT DEFAULT '10000',
    retention_interval TEXT DEFAULT '100000',
    premake INTEGER DEFAULT 4
)
```

The one-, two-, three- and four-argument Java overloads invoke that public SQL function with the
same number of arguments. A separate Java `Internal` entry point is not needed.

## Topics and notifications

| SQL function | Java API |
| --- | --- |
| `validate_routing_key` | `validateRoutingKey` |
| `validate_topic_pattern` | `validateTopicPattern` |
| `bind_topic` | `bindTopic` |
| `unbind_topic` | `unbindTopic` |
| `test_routing` | `testRouting` |
| `list_topic_bindings` | `listTopicBindings`: all bindings or one queue |
| `send_topic` | `sendTopic`: headers and integer delay |
| `send_batch_topic` | `sendBatchTopic`: headers, integer delay and absolute timestamp |
| `enable_notify_insert` | `enableNotifyInsert`: SQL default or explicit throttle interval |
| `disable_notify_insert` | `disableNotifyInsert` |
| `update_notify_insert` | `updateNotifyInsert` |
| `list_notify_insert_throttles` | `listNotifyInsertThrottles` |
| `notify_queue_listeners` | Trigger installed by `enableNotifyInsert`; no ordinary Java invocation |

Both batch-send APIs accept typed payload lists and read-only typed headers (`List<?>`,
`Map<String, ?>`, or a covariant list of header maps). Shared maps delegate to the existing expansion
and type-header handling, with either an integer delay or an absolute timestamp.

## Upstream implementation details

The underscore-prefixed functions are PostgreSQL implementation details and do not add Java
public methods. For example, `createPartitioned` calls `pgmq.create_partitioned`, which invokes
`pgmq._create_partitioned`; `convertArchivePartitioned` follows the same public-wrapper path.
Those public SQL wrappers restore the caller's search path after pg_partman calls.

`sendBatch` and `sendBatchTopic` use the SQL batch entry points, which invoke their validation and
insertion helpers. `createFifoIndex` invokes the public SQL index function, which calls its
idempotent implementation. The template retains normal payload validation, shared-header
expansion and `_type` information. It does not expose an unchecked batch-send API.

The existing `getJdbcTemplate()` provides direct SQL access for database-specific administrative
or debugging operations without adding public wrappers for each upstream helper.

## Transaction locks and deprecated behavior

The transaction lock covers later operations only when those calls use the same data source and
transaction. The template does not create a transaction automatically:

```java
transactionTemplate.executeWithoutResult(status -> {
    pgmqTemplate.acquireQueueLock("orders");
    // Queue administration on the same data source participates in this transaction.
});
```

In auto-commit mode, PostgreSQL releases the lock at statement completion. Both commit and
rollback release transaction-level advisory locks.

`detachArchive` is a deprecated SQL no-op in PGMQ 1.13.1. It does not preserve an archive when the
queue is dropped. The deprecated `dropQueue(queue, partitioned)` ignores the flag and preserves
the SQL overload's unconditional true return value, even for an already missing queue. Prefer
`dropQueue(queue)`, which reports whether an existing queue was dropped. Upstream plans to remove
both deprecated SQL entry points in PGMQ 2.0.
