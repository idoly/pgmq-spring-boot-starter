# Listener operation and tuning

Start with small, explicit concurrency and measure processing tail latency, queue age and JDBC pool
waits. Values are per listener and per application replica. More threads do not create more database
capacity.

## Connection and in-flight budgets

A long poll occupies a JDBC connection while it waits. Consumers borrow connections for business
SQL and acknowledgement; idle consumer threads do not reserve a connection.

```text
pool budget >= active long polls + concurrent consumer database work + other work + margin
active long polls <= sum(poll-concurrency) across running listeners
```

This is a conservative budget rather than a measurement of simultaneous usage. Include transaction
connections held during external calls, HTTP requests, senders, scheduled tasks and database metrics.
Budget PostgreSQL server connections across all replicas and applications too.

For one modest queue, a starting point is:

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 12
pgmq:
  listener:
    poll-concurrency: 1
    consumer-concurrency: 4
    max-in-flight: 4
    batch-size: 4
    max-poll-seconds: 5
    poll-interval-ms: 100
    visibility-timeout: 30
```

The template's long-poll methods can omit SQL-defaulted arguments. A listener passes its resolved
configuration explicitly. A zero duration uses one ordinary read; after an empty result the container
sleeps for `poll-interval-ms`. Non-empty batches dispatch immediately.

The default consumer queue is bounded and the semaphore reserves capacity before reads. Custom
executors must enforce the desired worker concurrency and belong to their container. Use the existing
`PgmqListenerExecutorFactory` extension point rather than sharing an executor directly: stopping one
container shuts down the executors it owns. A Java 21+ application can provide virtual-thread
executors, but still needs bounded in-flight work and the same JDBC budget.

## Slow work and redelivery

The visibility window starts when PostgreSQL reads the message, including local queue wait:

```text
visibility timeout > queue wait + processing time + acknowledgement time + margin
```

For work normally finishing within 30 seconds, start with four consumers, `max-in-flight: 4`, a
120-second visibility timeout and an appropriately longer shutdown allowance. These are examples,
not measured universal settings. Increasing buffering without throughput consumes visibility time.

The starter does not automatically renew visibility. Expiry can produce concurrent redelivery, even
while an old task is still running. Use business idempotency. A longer window also delays recovery
from a crashed consumer. The default error handler logs and leaves messages for visibility-based
redelivery; maximum attempts, backoff and dead-letter routing are application policies through
`PgmqErrorHandler` and `PgmqAcknowledgment`.

FIFO SQL modes control selection. Concurrent handlers can finish in a different order. Define the
required per-group execution contract before increasing concurrency.

## Transactions and completion

Use a separate transactional Spring service, invoked through its proxy, with the same data source
and transaction manager as the template:

```java
@PgmqListener(value = "orders", ackMode = AckMode.MANUAL)
public void receive(OrderEvent event, PgmqAcknowledgment acknowledgment) {
    orderService.applyAndAcknowledge(event, acknowledgment);
}

@Transactional
public void applyAndAcknowledge(OrderEvent event, PgmqAcknowledgment acknowledgment) {
    orderRepository.applyIdempotently(event);
    acknowledgment.acknowledge();
}
```

The DTO, service and repository are application types. The first successfully returned completion
SQL wins on one handle. Concurrent attempts are serialized, and exceptions leave the handle
incomplete. `isCompleted()` is not a transaction-commit signal. A later rollback restores database
state; discard the handle after that delivery. A connection failure can leave the outcome unknown,
so retries do not guarantee exactly-once side effects.

Default annotated listeners must be synchronous `void` singleton methods. Async return types,
`@Async` and prototype listener beans are rejected at registration. Lazy singletons register when
initialized. String `autoStartup` and `conditional` attributes use Spring resolution; numeric
inheritance uses only `-1`. Explicit listener IDs keep monitoring identities stable.

## Shutdown and timeouts

The registry stops containers sequentially. Each container interrupts and waits for pollers, then
drains consumers; each phase can wait up to `executor-shutdown-timeout-seconds`. Configure Spring's
shutdown phase timeout and deployment grace period for the aggregate drain requirement.

Zero shutdown wait requests interruption without waiting. A driver or business task may ignore the
interrupt. Late work retains its original run state and cannot release permits into a restarted run,
but already executing business code can still finish or acknowledge.

Connection acquisition timeout, JDBC query timeout, lock waits, network timeout and SQL poll duration
are different boundaries. The SQL polling deadline is checked between attempts and does not bound
an individual statement's lock wait. Thread interruption does not guarantee query cancellation.
Use the application's JDBC/data-source timeout settings and leave room above the intended healthy
long-poll duration. The real JDBC tests verify configured query cancellation and reuse, including the
case where immediate stop leaves SQL active until its deadline.

## Observe before changing capacity

Read queue age/depth, consumer latency, poll failures, in-flight state and data-source active/idle/
pending counts together. In-flight gauges include reserved polling capacity, so an empty queue can
show a nonzero value while a long poll is active. Listener gauges describe the current run; late tasks
from a forced stop are not counted as new-run capacity.

Database gauges are sampled after a meter registry binds. Listener counters/timers update per event.
A failed dynamic registration can be retried with the same ID; gauges rebind to the new container
while counters remain cumulative. Metric-registration failures do not break message processing.

See [verification](testing.md) for database setup and fault coverage, and [architecture](architecture.md)
for validation ownership and execution boundaries.
