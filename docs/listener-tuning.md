# Listener configuration and operation

The examples below are starting points for one application instance. Measure processing latency,
connection waits and database load before increasing concurrency. Every application replica has
its own threads and connection pool.

## Budget database connections before threads

Each listener owns a poll executor and a consumer executor. A database long poll holds a JDBC
connection while it waits. Consumer threads use connections when business code or acknowledgement
SQL runs; an idle consumer thread does not reserve a connection.

A conservative capacity budget is:

```text
pool capacity >= long-poll connections + concurrent consumer database work + other database work + margin
long-poll connections <= sum(poll-concurrency) across running listeners
```

Consumer database work includes acknowledgements. Count transaction-bound connections for the
whole transaction, including time spent on external calls inside it. Account for sends, HTTP
requests, scheduled jobs and metric refreshes in other database work. The budget is an upper
bound, not a measurement of simultaneous usage.

Use your connection pool's configuration and active, idle and pending connection metrics to
check this budget. Reserve server connections across all replicas and other applications as well.
The starter does not infer or change your application's connection pool size.

## A small queue with low processing latency

This example has one listener, four consumer threads and at most four messages in flight:

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
    poll-interval-ms: 50
    visibility-timeout: 30
```

A smaller poll interval can reduce idle delivery latency while increasing PostgreSQL work.
`max-poll-seconds` controls how long the database call waits, not how long a message may execute.
Keep JDBC query timeouts above the intended long-poll duration, with room for database latency.
Configure connection acquisition, query execution and network timeouts for their separate purposes:
a pool acquisition timeout does not cancel an executing SQL statement, and thread interruption
alone is not a guarantee that a JDBC driver cancels its query. The SQL polling deadline is checked
between read attempts, so it also does not bound time spent waiting inside an individual statement.
A zero `max-poll-seconds` performs one immediate read. After an empty immediate read, a listener
waits `poll-interval-ms` before trying again. A non-empty read is dispatched immediately.

## Slow or variable-duration tasks

Keep local buffering small so messages spend less of their visibility window waiting for a
consumer. For tasks normally completing within 30 seconds, a starting configuration is:

```yaml
pgmq:
  listener:
    poll-concurrency: 1
    consumer-concurrency: 4
    max-in-flight: 4
    batch-size: 4
    visibility-timeout: 120
    executor-shutdown-timeout-seconds: 150
```

Choose visibility timeout from measured tail latency:

```text
visibility timeout > queue wait + processing time + acknowledgement time + margin
```

The visibility window starts when PostgreSQL reads the message. Increasing `max-in-flight`
without increasing consumer throughput can consume that window in the local executor queue.
This starter does not automatically renew visibility. A handler that runs beyond its visibility
timeout can execute concurrently with a redelivery. Use idempotent business operations and size
the timeout for your workload. Longer visibility also delays retries after a crashed consumer.

Configure Spring's shutdown phase timeout and the deployment termination grace period to cover
listener draining. Each container can wait for poller shutdown and then consumer shutdown, each
up to `executor-shutdown-timeout-seconds`; the registry stops its containers sequentially. A
zero timeout requests immediate interruption. Tasks and JDBC drivers may ignore interrupts;
late tasks retain their original run's capacity and cannot dispatch work into a restarted run,
but a task already executing may still finish business work or acknowledge its message. Messages
returned by a late poll that are not dispatched become eligible for delivery again when their
visibility timeout expires.

## Many queues in one application

Properties are defaults for each listener, not a global pool limit. With eight listeners at
`poll-concurrency: 1`, allow up to eight long-poll connections. If each listener has two consumers
that can hold business connections, the conservative budget includes another sixteen connections,
plus other application work and margin. Check the aggregate before increasing per-listener values.
Override concurrency on individual `@PgmqListener` methods for busy queues.

Retain the existing `PgmqListenerExecutorFactory` extension point for custom executors. The
container shuts down executors returned by the factory, so return executors owned by that
container. Sharing one executor directly between containers allows stopping one listener to shut
down the others. A Java 21+ application may supply virtual-thread executors, but it still needs
bounded in-flight work and the same connection budget. Custom consumer executors must enforce
any desired execution concurrency; the container's semaphore bounds total in-flight messages.

## Transactions, acknowledgement and retries

For atomic business changes and queue completion in PostgreSQL, invoke a transactional Spring
service using the same data source and transaction manager as `PgmqTemplate`. One possible listener
shape is:

```java
@PgmqListener(value = "orders", ackMode = AckMode.MANUAL)
public void onOrder(OrderEvent event, PgmqAcknowledgment acknowledgment) {
    orderService.applyAndAcknowledge(event, acknowledgment);
}

// On a separate Spring-managed service, invoked through its transactional proxy:
@Transactional
public void applyAndAcknowledge(OrderEvent event, PgmqAcknowledgment acknowledgment) {
    orderRepository.applyIdempotently(event); // e.g. enforce a unique business event ID
    acknowledgment.acknowledge();
}
```

The example assumes application-defined `OrderEvent`, service and repository types. Ensure the
service call crosses Spring's transaction proxy. `isCompleted()` reports a successfully returned
completion SQL call, not a committed transaction. A later transaction rollback rolls back the
archive as well. Discard the handle after that delivery.

The first successfully returned completion action wins. Concurrent calls on one handle are
serialized. A thrown database exception leaves the handle incomplete and permits another attempt.
Connection failures can leave the database outcome unknown, so this is not an exactly-once
guarantee. Repeated delivery must remain safe for business code and external side effects.

The default error handler logs processing failures and leaves messages for visibility-based
redelivery. Maximum attempts, backoff and dead-letter routing are application policies in the
current API; implement them through `PgmqErrorHandler` when required. Avoid swallowing a processing
failure and returning normally in `AUTO` mode, since normal return triggers acknowledgement.

FIFO SQL read modes control selection; concurrent handlers can still finish in a different order.
If business completion order matters, define the required per-group execution contract before
increasing concurrency.

## Run the JDBC and Spring lifecycle checks

The JDBC lifecycle tests can run against an explicitly supplied disposable PostgreSQL database,
without Testcontainers or pg_partman. They install PGMQ when needed and create temporary queues
and a business table. Use a dedicated test database and an account allowed to install the bundled
schema. For example, in Bash:

```shell
export PGMQ_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/pgmq_test
export PGMQ_TEST_USERNAME=postgres
export PGMQ_TEST_PASSWORD=postgres
mvn '-Dtest=PgmqJdbcLifecycleIntegrationTest' test
```

Without `PGMQ_TEST_JDBC_URL`, this test class is skipped. It checks zero-duration reads, empty-poll
spacing, two listeners sharing two connections, acquisition timeout, slow-query cancellation,
shutdown bounds, and Spring transactional rollback of business work together with manual ACK.
It also exercises transaction-level queue locks, batch validation and shared headers, FIFO index
creation and deprecated compatibility functions. The existing Testcontainers suite continues to
cover pg_partman-dependent operations.

## Measure before tuning

Watch listener processing latency, in-flight work, poll failures, queue age, visible queue depth,
and connection-pool active, idle and pending counts together. A full in-flight limit can mean slow
business work or connection contention. More consumer threads help only when the constrained
resource has spare capacity. Increasing the JDBC pool also requires PostgreSQL capacity.
