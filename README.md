# PGMQ Spring Boot Starter

Spring Boot starter for [PostgreSQL PGMQ](https://github.com/pgmq/pgmq). It auto-configures a JDBC-backed
`PgmqTemplate`, annotation-driven listeners, queue declarations, topic routing, FIFO helpers,
insert notification helpers, and Micrometer metrics.

This release uses the current package layout and is not source-compatible with the legacy
`io.github.idoly.pgmq.core` API. See [MIGRATION.md](MIGRATION.md) for upgrade steps and
[CHANGELOG.md](CHANGELOG.md) for release notes.

## Dependency

```xml
<dependency>
    <groupId>io.github.idoly</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>2.1-1.11.2</version>
</dependency>
```

## Compatibility

The starter requires Java 17+ and is validated against these Spring Boot lines.

| Spring Boot line | Maven profile | Notes |
| ---------------- | ------------- | ----- |
| `3.5.x` | `boot35` (default) | Boot 3 compatibility line |
| `4.0.x` | `boot40` | Boot 4.0 validation line |
| `4.1.x` | `boot41` | Boot 4.1 validation line |

Spring Boot `2.x` and `3.0.x` through `3.4.x` are outside the supported matrix.
The default build uses Spring Boot `3.5.x` so the starter does not accidentally depend on
Spring Boot 4-only APIs. Profiles pin the first patch release in each supported line; override
`spring-boot.version` to a newer patch in the same line when validating locally.

## Auto-Configuration

When a `JdbcTemplate` is available, the starter contributes these beans unless the application
already defines equivalents:

| Bean | Purpose |
| ---- | ------- |
| `PgmqTemplate` | Main Java API for PGMQ SQL functions |
| `PgmqInitializer` | Optional extension or bundled SQL initialization |
| `PgmqQueueDeclarer` | Creates queues represented by `PgmqQueue` beans |
| `PgmqMetricsBinder` | Micrometer queue and notification sampler |
| `PgmqListenerMetrics` | Listener state, polling, and processing metrics |
| `pgmqListenerContainerFactory` | Default `@PgmqListener` container factory |
| `PgmqListenerEndpointRegistry` | Owns annotation-created listener containers |
| `PgmqErrorHandler` | Handles listener processing failures |
| `PgmqListenerExecutorFactory` | Creates container-owned poller and consumer executors |

Listener infrastructure is conditional on `pgmq.listener.enabled=true`, which is the default.

## Configuration

| Property | Type | Default | Description |
| -------- | ---- | ------- | ----------- |
| `pgmq.auto-init` | `boolean` | `false` | Initialize PGMQ during application startup. |
| `pgmq.sql-script` | `string` | `sql/pgmq.sql` | Classpath SQL script used when the PGMQ extension is unavailable and the bundled schema is not installed. |
| `pgmq.listener.enabled` | `boolean` | `true` | Enable annotation-driven listeners. |
| `pgmq.listener.visibility-timeout` | `int` | `30` | Default listener visibility timeout in seconds. |
| `pgmq.listener.batch-size` | `int` | `10` | Maximum messages requested by one poller; available in-flight permits may lower the actual quantity. |
| `pgmq.listener.conditional` | `string` | `{}` | Default JSON conditional used by standard `READ` mode. |
| `pgmq.listener.mode` | `enum` | `READ` | Default read mode: `READ`, `FIFO_GROUPED`, `FIFO_ROUND_ROBIN`, or `FIFO_HEAD`. |
| `pgmq.listener.consumer-concurrency` | `int` | `1` | Default number of concurrent consumer threads per listener. |
| `pgmq.listener.poll-concurrency` | `int` | `1` | Default number of blocking database pollers per listener. |
| `pgmq.listener.max-in-flight` | `int` | `10` | Maximum messages queued or executing per listener. |
| `pgmq.listener.executor-shutdown-timeout-seconds` | `int` | `30` | Maximum seconds to drain each executor during shutdown. |
| `pgmq.listener.auto-startup` | `boolean` | `true` | Whether listener containers start with the Spring lifecycle. |
| `pgmq.listener.ack-mode` | `enum` | `AUTO` | Default acknowledgement mode: `AUTO`, `MANUAL`, or `NONE`. |
| `pgmq.listener.max-poll-seconds` | `int` | `5` | Maximum seconds a listener long-poll waits for messages. |
| `pgmq.listener.poll-interval-ms` | `int` | `100` | Milliseconds between long-poll attempts. |

Numeric listener properties are validated during binding or listener container creation.
Visibility timeout may be `0` in template calls, but listener visibility timeout, batch size,
consumer concurrency, poll concurrency, max in-flight, and poll interval defaults must be positive.
`max-in-flight` must be greater than or equal to both concurrency values. `max-poll-seconds` and
`executor-shutdown-timeout-seconds` may be `0`.

## Initialization

Set `pgmq.auto-init=true` when the application should initialize PGMQ at startup.

If PostgreSQL reports the `pgmq` extension as available, the initializer creates the extension
when needed and warns when the installed extension version differs from `1.11.2`.

If the extension is not available, the initializer executes the configured bundled SQL script only
when the `pgmq` schema is not already installed. Bundled SQL installs do not create starter-owned
version metadata, and this starter now ships a single `sql/pgmq.sql` script rather than incremental
PGMQ upgrade scripts.

Auto-initialization is coordinated by the queue declarer, so it runs once before queue declarations
and also runs when no `PgmqQueue` beans are declared.

## Queue Declaration

Declare `PgmqQueue` beans to create queues after Spring singletons are initialized. If
`pgmq.auto-init=true`, PGMQ initialization runs before queue declaration.

Partitioned queues require the `pg_partman` extension to be installed in the target database.

```java
import io.github.idoly.pgmq.queue.PgmqQueue;
import org.springframework.context.annotation.Bean;

@Bean
public PgmqQueue ordersQueue() {
    return PgmqQueue.create("orders");
}

@Bean
public PgmqQueue auditQueue() {
    return PgmqQueue.createUnlogged("audit");
}

@Bean
public PgmqQueue eventsQueue() {
    return PgmqQueue.createPartitioned("events");
}
```

## Template Usage

Inject `io.github.idoly.pgmq.template.PgmqTemplate` for direct access to PGMQ operations.
Messages returned by reads are `io.github.idoly.pgmq.message.PgmqMessage` records.

```java
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqMetrics;
import io.github.idoly.pgmq.template.PgmqTemplate;

import java.util.List;
import java.util.Map;

long msgId = pgmqTemplate.send("orders", Map.of("id", 1001));

List<PgmqMessage> messages = pgmqTemplate.read("orders", 30, 10);
List<PgmqMessage> longPoll = pgmqTemplate.readWithPoll("orders", 30, 10);

PgmqMetrics metrics = pgmqTemplate.getMetrics("orders");

pgmqTemplate.archive("orders", msgId);
```

`send(...)`, `sendBatch(...)`, `sendTopic(...)`, and `sendBatchTopic(...)` copy caller headers
and add an internal `_type` header. The template uses that header to deserialize messages back to
their Java payload type when possible. Relative delays are passed to PGMQ as integer seconds and
use the PostgreSQL clock; absolute `visibleAt` timestamps are passed as `TIMESTAMP WITH TIME ZONE`.

Batch send methods accept no headers, one shared header map, or one header map per message.
Empty message batches, `null` payloads, `null` headers, empty header names, and mismatched
per-message header counts fail fast through template validation.

```java
List<Long> ids = pgmqTemplate.sendBatch(
        "orders",
        List.of(Map.of("id", 1001), Map.of("id", 1002)),
        Map.of("source", "api")
);
```

Common queue and message operations:

```java
pgmqTemplate.create("orders");
pgmqTemplate.createUnlogged("audit");
pgmqTemplate.createPartitioned("events");

List<PgmqMessage> popped = pgmqTemplate.pop("orders", 5);
pgmqTemplate.setVt("orders", msgId, 60);
pgmqTemplate.delete("orders", msgId);
pgmqTemplate.purgeQueue("orders");
pgmqTemplate.dropQueue("orders");

pgmqTemplate.listQueues();
pgmqTemplate.getMetricsAll();
```

## Listeners

`@PgmqListener` registers a lifecycle-managed polling container for a queue. Listener containers
use long-poll reads and, by default, archive messages after successful listener execution.

```java
import io.github.idoly.pgmq.annotation.PgmqHeader;
import io.github.idoly.pgmq.annotation.PgmqHeaders;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.annotation.PgmqPayload;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.PgmqAcknowledgment;
import io.github.idoly.pgmq.message.PgmqMessage;

import java.util.Map;

@PgmqListener("orders")
public void handleOrder(@PgmqPayload OrderEvent event, @PgmqHeader("source") String source) {
    process(event, source);
}

@PgmqListener(value = "orders", ackMode = AckMode.MANUAL)
public void handleManually(@PgmqPayload OrderEvent event, PgmqAcknowledgment acknowledgment) {
    process(event);
    acknowledgment.acknowledge();
}

@PgmqListener("orders")
public void handleHeaders(@PgmqPayload OrderEvent event, @PgmqHeaders Map<String, Object> headers) {
    process(event, headers);
}

@PgmqListener("orders")
public void handleRecord(PgmqMessage message) {
    process(message.payload(), message.headers());
}
```

Listener argument resolution order is:

1. `PgmqMessage`
2. `PgmqAcknowledgment`
3. `@PgmqHeader`
4. `@PgmqHeaders`
5. payload arguments, including unannotated parameters

Use `@PgmqHeaders` for the full headers map. An unannotated `Map` parameter is treated as a
payload argument.

Acknowledgement modes:

| Mode | Behavior |
| ---- | -------- |
| `AUTO` | Archive after successful listener execution unless the acknowledgement handle already completed the message. |
| `MANUAL` | Listener code must call `acknowledge()`, `delete()`, or `requeueAfter(seconds)`. |
| `NONE` | Leave messages untouched after listener execution. |

The default listener container factory bean is named `pgmqListenerContainerFactory`.
Override `PgmqErrorHandler` to customize listener failure handling.

### Listener Execution

Listener containers separate blocking database polling from listener method execution. Pollers
reserve bounded in-flight capacity before reading, so a slow consumer cannot create an unbounded
local backlog. Successful `AUTO` messages are archived individually as soon as processing finishes.

```java
@PgmqListener(
        value = "orders",
        pollConcurrency = 1,
        consumerConcurrency = 8,
        maxInFlight = 16,
        executorShutdownTimeoutSeconds = 30)
public void concurrentOrderListener(OrderEvent event) {
    process(event);
}
```

Keep total `pollConcurrency` below the JDBC pool capacity after reserving connections for normal
application queries and acknowledgement operations. The timeout is applied separately while
stopping the poller executor and while draining the consumer executor, so the total stop duration
can be up to twice the configured value. Applications may provide a `PgmqListenerExecutorFactory`
bean to create custom platform-thread or Java 21 virtual-thread consumer executors. Returned
executors must be dedicated to one container because the container owns their shutdown lifecycle.

## FIFO Reads

FIFO helpers group messages by the `x-pgmq-group` header. Messages without that header use the
`_default_fifo_group` group. Create the helper index before high-volume FIFO reads.

```java
pgmqTemplate.createFifoIndex("orders");

pgmqTemplate.send("orders", order, Map.of("x-pgmq-group", order.customerId()));

List<PgmqMessage> grouped = pgmqTemplate.readGrouped("orders", 30, 10);
List<PgmqMessage> roundRobin = pgmqTemplate.readGroupedRoundRobin("orders", 30, 10);
List<PgmqMessage> heads = pgmqTemplate.readGroupedHead("orders", 30, 10);

@PgmqListener(value = "orders", mode = PgmqListener.Mode.FIFO_GROUPED, batchSize = 10)
public void fifoListener(@PgmqPayload OrderEvent event) {
    process(event);
}
```

Long-poll variants are also available:

```java
pgmqTemplate.readGroupedWithPoll("orders", 30, 10);
pgmqTemplate.readGroupedRoundRobinWithPoll("orders", 30, 10);
pgmqTemplate.readGroupedHeadWithPoll("orders", 30, 10);
```

## Topic Routing

Topic routing binds routing-key patterns to queues, then sends one message to every matching queue.

```java
import io.github.idoly.pgmq.topic.TopicBatchResult;
import io.github.idoly.pgmq.topic.TopicBinding;
import io.github.idoly.pgmq.topic.TopicRoute;

pgmqTemplate.bindTopic("orders.*", "orders");

List<TopicRoute> routes = pgmqTemplate.testRouting("orders.created");

int matchedQueues = pgmqTemplate.sendTopic("orders.created", Map.of("id", 1001));

List<TopicBatchResult> results = pgmqTemplate.sendBatchTopic(
        "orders.created",
        List.of(Map.of("id", 1001), Map.of("id", 1002))
);

List<TopicBinding> bindings = pgmqTemplate.listTopicBindings("orders");
pgmqTemplate.unbindTopic("orders.*", "orders");
```

`sendTopic` returns the number of matched queues. `sendBatchTopic` returns one `TopicBatchResult`
per inserted message and queue.

## Insert Notifications

PGMQ insert notifications can be enabled and inspected through the template.

```java
import io.github.idoly.pgmq.notification.NotifyInsertThrottle;

pgmqTemplate.enableNotifyInsert("orders");
pgmqTemplate.updateNotifyInsert("orders", 500);

List<NotifyInsertThrottle> throttles = pgmqTemplate.listNotifyInsertThrottles();

pgmqTemplate.disableNotifyInsert("orders");
```

The no-argument throttle overload uses PGMQ's default `250` millisecond throttle interval.

## Micrometer Metrics

The auto-configuration registers `PgmqMetricsBinder` for database queue/notification gauges and
`PgmqListenerMetrics` for listener execution metrics. Database gauges refresh every 30 seconds by
default; listener counters, timers, and state gauges update from the listener hot path.

```text
pgmq.queue.length
pgmq.queue.visible_length
pgmq.queue.newest_msg_age_sec
pgmq.queue.oldest_msg_age_sec
pgmq.queue.total_messages
pgmq.notify_insert.throttle_interval_ms
pgmq.notify_insert.last_notified_at_epoch_seconds
pgmq.listener.running
pgmq.listener.in_flight
pgmq.listener.polls
pgmq.listener.poll.duration
pgmq.listener.messages
pgmq.listener.processing.duration
```

Queue metrics are tagged with `queue`. Notification metrics are also tagged with `queue`.
Listener metrics are tagged with `listener` and `queue`; poll and processing counters/timers also use
`result=success|error`. `newest_msg_age_sec`, `oldest_msg_age_sec`, and
`last_notified_at_epoch_seconds` preserve database `NULL` values; Micrometer exports missing numeric
values as `NaN`.

## Development and Release

Run the default Spring Boot 3.5 tests and compile against each supported Spring Boot 4 line:

```shell
mvn -q test
mvn -q -Pboot40 -DskipTests package
mvn -q -Pboot41 -DskipTests package
```

### Podman Test Environment

Tests use Testcontainers to start PostgreSQL automatically with the pinned
`ghcr.io/dbsystel/postgresql-partman@sha256:c12e31bdd5e16ea72b88f30bbf96b539368f747a8c7896880a22b3cf0741a6f0`
image. Integration tests are skipped when a Podman API socket is not available. To run the complete
suite, install Podman and expose its rootless socket:

```shell
systemctl --user enable --now podman.socket
export DOCKER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
mvn -q test
```

Testcontainers retains the `DOCKER_HOST` environment variable name because Podman exposes a
Docker-compatible API.

### Manual PostgreSQL Environment

The automated test suite uses Testcontainers. If you need a persistent local database for manual
debugging or ad-hoc verification, you can start the same PostgreSQL/pg_partman image yourself:

```shell
podman run -d --name postgresql-partman \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ghcr.io/dbsystel/postgresql-partman@sha256:c12e31bdd5e16ea72b88f30bbf96b539368f747a8c7896880a22b3cf0741a6f0

podman exec -it postgresql-partman bash
psql -U postgres
create extension if not exists pg_partman;
```

This manual container is supplemental. The Maven tests create and manage their own isolated
PostgreSQL container through Testcontainers.

### Publishing

Signing and Maven Central publishing are enabled only by the explicit `release` profile. Publishing
requires a `central` server entry in Maven settings and a GPG secret key:

```shell
mvn -Prelease deploy
```
