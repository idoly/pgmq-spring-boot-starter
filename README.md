# PGMQ Spring Boot Starter

Spring Boot starter for [PostgreSQL PGMQ](https://github.com/pgmq/pgmq). It auto-configures a JDBC-backed
`PgmqTemplate`, annotation-driven listeners, queue declarations, topic routing, FIFO helpers,
insert notification helpers, and Micrometer gauges.

This release uses the current package layout and is not source-compatible with the legacy
`io.github.idoly.pgmq.core` API. See [MIGRATION.md](MIGRATION.md) for upgrade steps and
[CHANGELOG.md](CHANGELOG.md) for release notes.

## Dependency

```xml
<dependency>
    <groupId>io.github.idoly</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>2.0-1.11.2</version>
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

Build checks:

```shell
mvn -q test
mvn -q -Pboot40 -DskipTests test
mvn -q -Pboot41 -DskipTests test
```

## Auto-Configuration

When a `JdbcTemplate` is available, the starter contributes these beans unless the application
already defines equivalents:

| Bean | Purpose |
| ---- | ------- |
| `PgmqTemplate` | Main Java API for PGMQ SQL functions |
| `PgmqInitializer` | Optional extension or bundled SQL initialization |
| `PgmqQueueDeclarer` | Creates queues represented by `PgmqQueue` beans |
| `PgmqMetricsBinder` | Micrometer `MeterBinder` and lifecycle sampler |
| `pgmqListenerContainerFactory` | Default `@PgmqListener` container factory |
| `PgmqListenerEndpointRegistry` | Owns annotation-created listener containers |
| `PgmqErrorHandler` | Handles listener processing failures |

Listener infrastructure is conditional on `pgmq.listener.enabled=true`, which is the default.

## Configuration

| Property | Type | Default | Description |
| -------- | ---- | ------- | ----------- |
| `pgmq.auto-init` | `boolean` | `false` | Initialize PGMQ during application startup. |
| `pgmq.sql-script` | `string` | `sql/pgmq.sql` | Classpath SQL script used when the PGMQ extension is unavailable and the bundled schema is not installed. |
| `pgmq.listener.enabled` | `boolean` | `true` | Enable annotation-driven listeners. |
| `pgmq.listener.visibility-timeout` | `int` | `30` | Default listener visibility timeout in seconds. |
| `pgmq.listener.batch-size` | `int` | `10` | Default number of messages read per listener poll. |
| `pgmq.listener.conditional` | `string` | `{}` | Default JSON conditional used by standard `READ` mode. |
| `pgmq.listener.mode` | `enum` | `READ` | Default read mode: `READ`, `FIFO_GROUPED`, `FIFO_ROUND_ROBIN`, or `FIFO_HEAD`. |
| `pgmq.listener.concurrency` | `int` | `1` | Default number of worker threads per listener method. |
| `pgmq.listener.auto-startup` | `boolean` | `true` | Whether listener containers start with the Spring lifecycle. |
| `pgmq.listener.ack-mode` | `enum` | `AUTO` | Default acknowledgement mode: `AUTO`, `MANUAL`, or `NONE`. |
| `pgmq.listener.max-poll-seconds` | `int` | `5` | Maximum seconds a listener long-poll waits for messages. |
| `pgmq.listener.poll-interval-ms` | `int` | `100` | Milliseconds between long-poll attempts. |

Numeric listener properties are validated during binding or listener container creation.
Visibility timeout may be `0` in template calls, but listener visibility timeout, batch size,
concurrency, and poll interval defaults must be positive. `max-poll-seconds` may be `0` for an
immediate long-poll timeout.

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
their Java payload type when possible.

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

The auto-configuration registers `PgmqMetricsBinder`, which exposes these gauges and refreshes
them every 30 seconds by default:

```text
pgmq.queue.length
pgmq.queue.visible_length
pgmq.queue.newest_msg_age_sec
pgmq.queue.oldest_msg_age_sec
pgmq.queue.total_messages
pgmq.notify_insert.throttle_interval_ms
pgmq.notify_insert.last_notified_at_epoch_seconds
```

Queue metrics are tagged with `queue`. Notification metrics are also tagged with `queue`.
`newest_msg_age_sec`, `oldest_msg_age_sec`, and `last_notified_at_epoch_seconds` preserve database
`NULL` values; Micrometer exports missing numeric values as `NaN`.

## Local Test Environment

Tests use Testcontainers to start PostgreSQL automatically with
`ghcr.io/dbsystel/postgresql-partman:latest`. Make sure Docker or Podman-compatible container
support is available, then run:

```shell
mvn -q test
```

For rootless Podman, expose the Podman socket to Testcontainers before running the tests:

```shell
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
mvn -q test
```

### Manual PostgreSQL Environment

The automated test suite uses Testcontainers. If you need a persistent local database for manual
debugging or ad-hoc verification, you can start the same PostgreSQL/pg_partman image yourself:

```shell
podman run -d --name postgresql-partman \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ghcr.io/dbsystel/postgresql-partman

podman exec -it postgresql-partman bash
psql -U postgres
create extension if not exists pg_partman;
```

This manual container is supplemental. The Maven tests create and manage their own isolated
PostgreSQL container through Testcontainers.
