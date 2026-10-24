# Changelog

## 2.1-1.13.1

### Changed

- Updated bundled SQL and the expected extension version to upstream PGMQ `1.13.1`.
- Included upstream search-path hardening, restricted `pg_monitor` grants, mixed-case insert
  notification fixes, and partition creation fixes.
- Documented explicit database upgrades; existing extensions and bundled schemas are not
  automatically migrated by the starter.

### Added

- Added a `createPartitioned` overload accepting PGMQ's `premake` setting.
- Exposed the nullable `defaultPartitionLength` metric and the Micrometer
  `pgmq.queue.default_partition_length` gauge. Retained the seven-argument metrics constructor.

## 2.1-1.11.2

### Added

- Added separate consumer and database poller executors with bounded in-flight backpressure.
- Added `consumer-concurrency`, `poll-concurrency`, `max-in-flight`, and
  `executor-shutdown-timeout-seconds` listener settings.
- Added a replaceable `PgmqListenerExecutorFactory` for custom platform or virtual thread executors.
- Added Micrometer listener running, in-flight, poll, processing, success, and error metrics.

### Changed

- Consolidated all resolved listener settings in `PgmqListenerContainerOptions`; no compatibility
  aliases are retained.
- Changed `AUTO` acknowledgement to archive each message immediately after successful processing.
- Changed relative-delay template methods to use PGMQ integer-delay overloads and the database clock.
- Delegated notification, partition, and archive-partition default values to their SQL functions.

### Removed

- Removed `PgmqListenerExecutionOptions`; its fields now belong to `PgmqListenerContainerOptions`.
- Removed `pgmq.listener.concurrency` and `@PgmqListener.concurrency`; use
  `consumer-concurrency` and `consumerConcurrency`.
- Removed the Java `detachArchive` API because PGMQ 1.11.2 implements its SQL function as a
  deprecated no-op.

### Build

- Added GitHub Actions checks for Spring Boot 3.5, 4.0, and 4.1.
- Configured Testcontainers integration tests to use a Podman API socket and skip cleanly when no
  compatible socket is available.
- Pinned the PostgreSQL/pg_partman test image by digest.
- Isolated GPG signing and Maven Central publishing in the explicit `release` profile.
- Added the Apache License 2.0 text to the repository.

## 2.0-1.11.2

This release is a breaking API cleanup around the PGMQ `1.11.2` SQL surface. It does not preserve
source compatibility with the legacy `io.github.idoly.pgmq.core` package.

### Breaking Changes

- Removed the legacy `io.github.idoly.pgmq.core` package.
- Moved `PgmqTemplate` to `io.github.idoly.pgmq.template.PgmqTemplate`.
- Renamed and moved data records:
  - `core.Message` -> `message.PgmqMessage`
  - `core.Queue` -> `queue.PgmqQueue`
  - `core.Metrics` / `metrics.PgmqQueueMetrics` -> `metrics.PgmqMetrics`
- Renamed the Micrometer binder from `metrics.PgmqMetrics` to `metrics.PgmqMetricsBinder`.
- Moved listener infrastructure out of `config` into focused listener packages:
  - `listener`
  - `listener.container`
  - `listener.endpoint`
  - `listener.adapter`
  - `listener.support`
- Removed bundled incremental PGMQ SQL upgrade scripts. Bundled SQL initialization now uses
  `sql/pgmq.sql` only.
- `sendBatch(...)` and `sendBatchTopic(...)` no longer short-circuit empty lists. Empty batches
  and `null` payloads fail fast through template validation.
- Queue age metrics preserve SQL `NULL` for empty queues instead of converting missing ages to `0`.
- `validateQueueName(...)` now returns a real `Boolean` from an outer SQL query after the
  underlying `void` SQL validator succeeds.

### Added

- Spring Boot compatibility profiles:
  - `boot35` for Spring Boot `3.5.x` (default)
  - `boot40` for Spring Boot `4.0.x`
  - `boot41` for Spring Boot `4.1.x`
- Maven Enforcer rule requiring Java `17+`.
- Annotation-driven listener argument annotations:
  - `@PgmqPayload`
  - `@PgmqHeader`
  - `@PgmqHeaders`
- Listener acknowledgement API:
  - `PgmqAcknowledgment`
  - `AckMode.AUTO`
  - `AckMode.MANUAL`
  - `AckMode.NONE`
- Manual acknowledgement actions:
  - `acknowledge()`
  - `delete()`
  - `requeueAfter(seconds)`
- Long-poll template APIs:
  - `readWithPoll`
  - `readGroupedWithPoll`
  - `readGroupedRoundRobinWithPoll`
  - `readGroupedHeadWithPoll`
- FIFO listener modes:
  - `READ`
  - `FIFO_GROUPED`
  - `FIFO_ROUND_ROBIN`
  - `FIFO_HEAD`
- Queue and archive management APIs in `PgmqTemplate`:
  - `create`
  - `createNonPartitioned`
  - `createUnlogged`
  - `createPartitioned`
  - `convertArchivePartitioned`
  - `dropQueue`
  - `purgeQueue`
  - `listQueues`
- Topic routing APIs in `PgmqTemplate`:
  - `validateRoutingKey`
  - `validateTopicPattern`
  - `bindTopic`
  - `unbindTopic`
  - `testRouting`
  - `sendTopic`
  - `sendBatchTopic`
  - `listTopicBindings`
- Insert notification helpers:
  - `enableNotifyInsert`
  - `disableNotifyInsert`
  - `updateNotifyInsert`
  - `listNotifyInsertThrottles`
- Micrometer notification gauges:
  - `pgmq.notify_insert.throttle_interval_ms`
  - `pgmq.notify_insert.last_notified_at_epoch_seconds`
- Fixed PGMQ `1.11.2` extension version check for auto-initialization:
  - extension-managed installs log a warning when the installed version differs
  - bundled SQL installs do not create starter metadata in `pgmq`
- Coordinated `pgmq.auto-init` through `PgmqQueueDeclarer` so initialization is not invoked twice
  during startup.
- Testcontainers-based integration tests now start `ghcr.io/dbsystel/postgresql-partman:latest`.
- Expanded release tests across template SQL paths, topic routing, FIFO reads, notification helpers,
  listener acknowledgement modes, auto-configuration, queue declaration, and Micrometer binding.

### SQL Alignment

- `PgmqTemplate` methods are ordered and implemented against the bundled `sql/pgmq.sql` function
  signatures.
- Bulk message id operations cast Java arrays explicitly to `bigint[]`.
- Queue validation uses `pgmq.validate_queue_name()` and returns `Boolean.TRUE` only when the SQL
  validation completes successfully.
- Template send APIs add an internal `_type` header so reads and listeners can deserialize payloads
  with Java type information when the message was sent through this template.

### Compatibility

- Java `17+` is the minimum supported runtime and build JDK.
- Supported Spring Boot validation lines are `3.5.x`, `4.0.x`, and `4.1.x`.
- Spring Boot `3.5.x` is the default Boot 3 compatibility line.
- Spring Boot `2.x` and `3.0.x` through `3.4.x` are outside the supported matrix.
- Auto-configuration orders itself after both the Spring Boot 3 and Spring Boot 4
  `JdbcTemplateAutoConfiguration` names.

### Migration

See [MIGRATION.md](MIGRATION.md).
