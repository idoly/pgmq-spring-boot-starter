# PGMQ Spring Boot Starter

A JDBC-backed Spring Boot integration for [PGMQ](https://github.com/pgmq/pgmq). It provides a SQL
client, queue declarations, synchronous annotated listeners, bounded in-flight work, acknowledgement
handles and Micrometer metrics.

The bundled SQL is the unmodified PGMQ **1.13.1** install script. Existing databases require an
explicit migration; updating the Java dependency does not update their functions.

## Install

```xml
<dependency>
    <groupId>io.github.idoly</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>2.1-1.13.1</version>
</dependency>
```

Java 17+ is required. Verification baselines are Spring Boot 3.5.0, 4.0.0 and 4.1.0, selected by
`boot35`, `boot40` and `boot41`. The application must provide a JDBC data source. Partition operations
also require pg_partman. The application can supply its own Jackson 2 `ObjectMapper`.

## Start a queue and listener

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/app
    username: app
    password: app
pgmq:
  auto-init: true
```

Use `auto-init` when the database role is allowed to install PGMQ. The initializer creates the
extension when available, otherwise loads the bundled SQL into a database without the schema.
It skips existing bundled schemas and warns about extension version mismatches. It never performs
an upgrade automatically.

```java
@Configuration(proxyBeanMethods = false)
class QueueConfiguration {
    @Bean
    PgmqQueue orders() {
        return PgmqQueue.create("orders");
    }
}

@Service
class OrderConsumer {
    @PgmqListener("orders")
    public void receive(OrderEvent event) {
        process(event);
    }
}
```

`PgmqQueue` is in `io.github.idoly.pgmq.queue`; `PgmqListener` is in
`io.github.idoly.pgmq.annotation`. Queue declarations run before listener startup. Queue factory
methods include `create`, `createUnlogged` and `createPartitioned`.

Default listeners are singleton Spring beans with synchronous `void` methods. Asynchronous return
values and method/class `@Async` are rejected. Lazy singleton beans register when initialized;
prototype listeners are rejected. Set an explicit listener `id` when dashboards or administrative
code require a stable identifier.

## Send and read

```java
List<OrderEvent> events = List.of(new OrderEvent(1, "created"), new OrderEvent(2, "updated"));
Map<String, String> headers = Map.of("source", "api");

long id = pgmqTemplate.send("orders", events.get(0), headers);
List<Long> ids = pgmqTemplate.sendBatch("orders", events, headers);
List<PgmqMessage> messages = pgmqTemplate.read("orders", 30, 10);
pgmqTemplate.archive("orders", id);
```

Inject `io.github.idoly.pgmq.template.PgmqTemplate`. Each batch list element becomes a message;
use `send` to send a list as one payload. Headers can be shared or supplied per message. Inputs are
not mutated. Empty batches, null payloads and invalid header counts fail before SQL execution.

Messages sent through the template include a `_type` hint. Known compatible types are restored
when reading. Unusable hints fall back to generic JSON values while retaining the headers. Listener
arguments convert to their declared types per message, so conversion failures reach the error handler
with the message ID. Direct read callers can use `convertMessagePayload` to request a concrete type.

Relative delays use integer seconds and the database clock; absolute timestamps use `OffsetDateTime`.
Read, long-poll, pop and partition overloads leave omitted arguments to SQL defaults. A zero polling
duration performs one immediate read; listeners wait `poll-interval-ms` after an empty result.

The [SQL API reference](docs/sql-api.md) covers FIFO reads, batch operations, visibility changes,
metrics, notifications, topic routing and partition management. Underscore-prefixed SQL functions
remain implementation details. The Java API uses public SQL wrappers rather than exposing alternate
`Internal` or unchecked entry points.

## Acknowledge and handle errors

| Mode | Completion behavior |
| --- | --- |
| `AUTO` | Archive after a successful synchronous invocation. |
| `MANUAL` | The listener controls completion through `PgmqAcknowledgment`. |
| `NONE` | The container does not complete the message. |

```java
@PgmqListener(value = "orders", ackMode = AckMode.MANUAL)
public void receive(OrderEvent event, PgmqAcknowledgment acknowledgment) {
    process(event);
    acknowledgment.acknowledge();
}
```

The handle supports `acknowledge`, `delete` and `requeueAfter`. Concurrent calls are serialized;
the first successfully returned completion SQL wins. A database exception permits another attempt.
The handle does not represent transaction commit or guarantee exactly-once processing.

The default `PgmqErrorHandler` logs failures and leaves messages for visibility-based redelivery.
Applications define idempotency, maximum attempts, retry backoff and dead-letter policy. Use the
same data source and transaction manager to make business changes and manual ACK transactional.
See [listener operation](docs/listener-tuning.md).

## Configuration

These are per-listener defaults, not global concurrency or connection limits.

| Property | Default | Meaning |
| --- | --- | --- |
| `pgmq.auto-init` | `false` | Install PGMQ when missing. |
| `pgmq.sql-script` | `sql/pgmq.sql` | Bundled schema install resource. |
| `pgmq.listener.enabled` | `true` | Register listener infrastructure. |
| `pgmq.listener.visibility-timeout` | `30` | Seconds until a read message may be delivered again. |
| `pgmq.listener.batch-size` | `10` | Maximum quantity requested by one poll. |
| `pgmq.listener.conditional` | `{}` | JSON filter for ordinary `READ` mode. |
| `pgmq.listener.mode` | `READ` | `READ`, `FIFO_GROUPED`, `FIFO_ROUND_ROBIN`, `FIFO_HEAD`. |
| `pgmq.listener.consumer-concurrency` | `1` | Consumer worker count. |
| `pgmq.listener.poll-concurrency` | `1` | Database poller count. |
| `pgmq.listener.max-in-flight` | `10` | Bound on queued/executing messages and reserved polling capacity. |
| `pgmq.listener.max-poll-seconds` | `5` | SQL long-poll duration; zero requests an immediate read. |
| `pgmq.listener.poll-interval-ms` | `100` | Database polling interval and empty immediate-read delay. |
| `pgmq.listener.executor-shutdown-timeout-seconds` | `30` | Maximum wait for each executor shutdown phase. |
| `pgmq.listener.auto-startup` | `true` | Start with the Spring lifecycle. |
| `pgmq.listener.ack-mode` | `AUTO` | Completion mode. |

Annotation numeric values inherit properties only when set to `-1`. Other negative values are
invalid. `max-in-flight` must cover both concurrency values. String settings use Spring placeholder
resolution; `autoStartup` uses Spring Boolean conversion and rejects invalid values.

Supply `PgmqErrorHandler`, `PgmqListenerExecutorFactory` or a named `PgmqListenerContainerFactory`
for application-specific behavior. The default factory bean is `pgmqListenerContainerFactory`.
Returned executors belong to their container and must not be shared directly between containers.

## Metrics

`PgmqMetricsBinder` samples database queue and notification gauges every 30 seconds, after a meter
registry is bound. `PgmqListenerMetrics` records poll/processing counters, timers, running state and
in-flight capacity. Tags include queue, listener ID and outcome as appropriate.

Queue gauges include length, visible length, message ages, total messages and estimated default
partition length. SQL null ages and non-partitioned default-partition estimates remain null in the
Java record and become `NaN` in gauges. A positive default-partition estimate warrants checking
pg_partman maintenance and database statistics.

## Develop and verify

```shell
mvn test                         # unit tests; database tests skip without an explicit URL
export PGMQ_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/pgmq_test
export PGMQ_TEST_USERNAME=postgres
export PGMQ_TEST_PASSWORD=postgres
mvn -Pboot35,coverage clean verify
```

Use a dedicated disposable database. All integration suites share these settings; no local
Testcontainers or Podman API is required. Partition tests skip if pg_partman is unavailable locally.
CI requires pg_partman and runs the entire suite on each Boot baseline with a pinned database image.
The coverage profile measures Java code and requires at least 90% line and 75% branch coverage.

See [verification and benchmark commands](docs/testing.md), [architecture decisions](docs/architecture.md),
[ablation evidence](docs/experiments/listener-ablation.md), [migration notes](MIGRATION.md) and
[release history](CHANGELOG.md).

Publishing is explicit: `mvn -Prelease clean deploy` activates signing and Maven Central publication.
Configure credentials and signing keys outside the repository. This command publishes artifacts;
ordinary `verify` does not.
