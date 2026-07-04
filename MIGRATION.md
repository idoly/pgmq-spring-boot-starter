# Migration Guide

This guide covers migration from the legacy `core` API to `2.0-1.11.2`. The current release
intentionally removes backward compatibility.

## 1. Replace Imports

| Old import | New import |
| ---------- | ---------- |
| `io.github.idoly.pgmq.core.PgmqTemplate` | `io.github.idoly.pgmq.template.PgmqTemplate` |
| `io.github.idoly.pgmq.core.Message` | `io.github.idoly.pgmq.message.PgmqMessage` |
| `io.github.idoly.pgmq.core.Queue` | `io.github.idoly.pgmq.queue.PgmqQueue` |
| `io.github.idoly.pgmq.core.Metrics` | `io.github.idoly.pgmq.metrics.PgmqMetrics` |
| `io.github.idoly.pgmq.metrics.PgmqQueueMetrics` | `io.github.idoly.pgmq.metrics.PgmqMetrics` |
| `io.github.idoly.pgmq.metrics.PgmqMetrics` as a `MeterBinder` | `io.github.idoly.pgmq.metrics.PgmqMetricsBinder` |

Example:

```java
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqMetrics;
import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.template.PgmqTemplate;
```

## 2. Update Queue Declarations

Replace legacy queue records with `PgmqQueue`.

```java
import io.github.idoly.pgmq.queue.PgmqQueue;
import org.springframework.context.annotation.Bean;

@Bean
public PgmqQueue ordersQueue() {
    return PgmqQueue.create("orders");
}
```

Use:

- `PgmqQueue.create(queueName)` for standard logged queues
- `PgmqQueue.createUnlogged(queueName)` for unlogged queues
- `PgmqQueue.createPartitioned(queueName)` for partitioned queues

Partitioned queues require `pg_partman` in the target database. `PgmqQueue` bean declarations
only carry the queue name and queue type flags; use `PgmqTemplate.createPartitioned(queue, partitionInterval, retentionInterval)`
when you need custom partition settings.

## 3. Update Template Code

The template is now `io.github.idoly.pgmq.template.PgmqTemplate`, and returned messages are
`PgmqMessage` records.

```java
long msgId = pgmqTemplate.send("orders", order);
List<PgmqMessage> messages = pgmqTemplate.read("orders", 30, 10);
pgmqTemplate.archive("orders", msgId);
```

Template sends copy caller headers and add an internal `_type` header. Reads use that header to
deserialize payloads to their Java type when the message was originally sent through this template.
Do not depend on owning or mutating that header yourself.

### Empty Batches

Empty lists now fail fast through template validation:

```java
pgmqTemplate.sendBatch("orders", List.of());
pgmqTemplate.sendBatchTopic("orders.created", List.of());
```

These calls throw `IllegalArgumentException` instead of returning an empty Java list. Varargs batch
overloads also preserve `null` elements until template validation, so `null` payloads fail with the
same template validation message.

### Batch Headers

Batch APIs accept:

- no headers
- one shared header map
- one header map per message

When more than one header map is supplied, the header list size must match the message list size.

### Queue Validation

`validateQueueName(...)` still returns `Boolean`, but invalid names are reported through Spring's
database access exception handling. A successful return means the underlying
`pgmq.validate_queue_name()` call completed.

```java
if (Boolean.TRUE.equals(pgmqTemplate.validateQueueName("orders"))) {
    // valid queue name
}
```

## 4. Update Message Completion Code

Single-message and bulk completion methods are still available, but imports and return record types
changed.

```java
pgmqTemplate.archive("orders", msgId);
pgmqTemplate.archive("orders", List.of(msgId1, msgId2));

pgmqTemplate.delete("orders", msgId);
pgmqTemplate.setVt("orders", msgId, 60);
pgmqTemplate.pop("orders", 5);
```

Bulk `archive`, `delete`, and `setVt` calls now cast Java arrays explicitly to `bigint[]` in SQL.

## 5. Update Metrics Types

Template metrics now use `PgmqMetrics`.

```java
PgmqMetrics metrics = pgmqTemplate.getMetrics("orders");
List<PgmqMetrics> allMetrics = pgmqTemplate.getMetricsAll();
```

If you customized Micrometer binding by type, replace the old binder type with `PgmqMetricsBinder`.

```java
import io.github.idoly.pgmq.metrics.PgmqMetricsBinder;

@Bean
PgmqMetricsBinder pgmqMetricsBinder(PgmqTemplate template) {
    return new PgmqMetricsBinder(template);
}
```

Queue age fields are nullable:

```java
Integer newestAge = metrics.newestMsgAgeSec();
Integer oldestAge = metrics.oldestMsgAgeSec();
```

Do not assume these values are `0` for empty queues.

## 6. Update Listener Code

Use the listener package APIs:

```java
import io.github.idoly.pgmq.annotation.PgmqHeader;
import io.github.idoly.pgmq.annotation.PgmqHeaders;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.annotation.PgmqPayload;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.PgmqAcknowledgment;
import io.github.idoly.pgmq.message.PgmqMessage;
```

Payload listener:

```java
@PgmqListener("orders")
public void handle(@PgmqPayload OrderEvent event) {
    process(event);
}
```

Headers:

```java
@PgmqListener("orders")
public void handle(@PgmqPayload OrderEvent event, @PgmqHeaders Map<String, Object> headers) {
    process(event, headers);
}
```

Manual acknowledgement:

```java
@PgmqListener(value = "orders", ackMode = AckMode.MANUAL)
public void handle(@PgmqPayload OrderEvent event, PgmqAcknowledgment ack) {
    process(event);
    ack.acknowledge();
}
```

Manual acknowledgement handles also support:

```java
ack.delete();
ack.requeueAfter(60);
```

Important listener argument rule: an unannotated `Map` parameter is treated as a payload argument.
Use `@PgmqHeaders` for the complete headers map, or `@PgmqHeader("name")` for one header.

## 7. Update Listener Infrastructure Customizations

If your application referenced container or endpoint classes directly, update imports:

| Type | Package |
| ---- | ------- |
| `PgmqListenerContainer` | `io.github.idoly.pgmq.listener.container` |
| `PgmqListenerContainerFactory` | `io.github.idoly.pgmq.listener.container` |
| `SimplePgmqListenerContainerFactory` | `io.github.idoly.pgmq.listener.container` |
| `PgmqListenerEndpointRegistry` | `io.github.idoly.pgmq.listener.endpoint` |
| `PgmqListenerAnnotationBeanPostProcessor` | `io.github.idoly.pgmq.listener.endpoint` |
| `MethodPgmqMessageListenerAdapter` | `io.github.idoly.pgmq.listener.adapter` |
| `LoggingPgmqErrorHandler` | `io.github.idoly.pgmq.listener.support` |

The default listener container factory bean name remains:

```text
pgmqListenerContainerFactory
```

Override `PgmqErrorHandler` to customize listener failure handling.

## 8. Review Listener Configuration

Listener defaults live under `pgmq.listener`.

| Property | Default |
| -------- | ------- |
| `pgmq.listener.enabled` | `true` |
| `pgmq.listener.visibility-timeout` | `30` |
| `pgmq.listener.batch-size` | `10` |
| `pgmq.listener.conditional` | `{}` |
| `pgmq.listener.mode` | `READ` |
| `pgmq.listener.concurrency` | `1` |
| `pgmq.listener.auto-startup` | `true` |
| `pgmq.listener.ack-mode` | `AUTO` |
| `pgmq.listener.max-poll-seconds` | `5` |
| `pgmq.listener.poll-interval-ms` | `100` |

`@PgmqListener` annotation values use the configured defaults when left unset. `conditional`
applies to standard `READ` mode.
`max-poll-seconds` may be `0`; visibility timeout, batch size, concurrency, and poll interval
must be positive for listener configuration.

## 9. Review FIFO APIs

FIFO reads and listener modes are first-class APIs:

```java
pgmqTemplate.createFifoIndex("orders");

pgmqTemplate.send("orders", order, Map.of("x-pgmq-group", order.customerId()));

pgmqTemplate.readGrouped("orders", 30, 10);
pgmqTemplate.readGroupedRoundRobin("orders", 30, 10);
pgmqTemplate.readGroupedHead("orders", 30, 10);

pgmqTemplate.readGroupedWithPoll("orders", 30, 10);
pgmqTemplate.readGroupedRoundRobinWithPoll("orders", 30, 10);
pgmqTemplate.readGroupedHeadWithPoll("orders", 30, 10);
```

```java
@PgmqListener(value = "orders", mode = PgmqListener.Mode.FIFO_GROUPED)
public void handle(OrderEvent event) {
    process(event);
}
```

FIFO grouping uses the `x-pgmq-group` message header. Messages without the header are assigned to
`_default_fifo_group`.

## 10. Review Topic APIs

Topic APIs are now first-class template methods:

```java
pgmqTemplate.bindTopic("orders.*", "orders");

int matchedQueues = pgmqTemplate.sendTopic("orders.created", order);

List<TopicBatchResult> results = pgmqTemplate.sendBatchTopic(
        "orders.created",
        List.of(order1, order2)
);

pgmqTemplate.testRouting("orders.created");
pgmqTemplate.listTopicBindings("orders");
pgmqTemplate.unbindTopic("orders.*", "orders");
```

`sendTopic` returns the number of matched queues. `sendBatchTopic` returns one result row per
inserted queue message.

## 11. Review Insert Notification APIs

Notification helpers are exposed on `PgmqTemplate`:

```java
pgmqTemplate.enableNotifyInsert("orders");
pgmqTemplate.updateNotifyInsert("orders", 500);
pgmqTemplate.listNotifyInsertThrottles();
pgmqTemplate.disableNotifyInsert("orders");
```

The Micrometer binder exports notification gauges for enabled notification throttles.

## 12. Review SQL Initialization

`PgmqInitializer` expects PGMQ `1.11.2`. For PostgreSQL extension-managed installs, the initializer
logs a warning when the installed extension version differs from `1.11.2`. Update the extension
outside the starter when you need the exact supported SQL surface.

For bundled SQL installs, the starter does not create version metadata in the `pgmq` schema.
Existing bundled schemas are treated as already installed.

Incremental classpath scripts named like `sql/pgmq--x--y.sql` are no longer bundled. If your
application referenced those resources directly, switch to extension-managed upgrades or manage
database migrations outside this starter.

## 13. Review Spring Boot and Java Versions

Java `17+` is required. The starter is validated against these Spring Boot lines:

| Spring Boot line | Maven profile |
| ---------------- | ------------- |
| `3.5.x` | `boot35` (default) |
| `4.0.x` | `boot40` |
| `4.1.x` | `boot41` |

Spring Boot `2.x` and `3.0.x` through `3.4.x` are no longer supported targets.

When building from source, run the profile that matches your application line:

```shell
mvn -q test
mvn -q -Pboot40 -DskipTests compile
mvn -q -Pboot41 -DskipTests compile
```

No application-side change is required for the Spring Boot 4 JDBC auto-configuration package split;
the starter handles both Spring Boot 3 and 4 auto-configuration names.
