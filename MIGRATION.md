# Migration guide

The SQL target is PGMQ 1.13.1. Java 17+ and the Spring Boot 3.5, 4.0 or 4.1 lines are the maintained
baselines. Review [the current configuration](README.md#configuration) and [SQL API](docs/sql-api.md)
when upgrading; this guide records behavior changes rather than preserving every historical example.

## Database upgrade

Updating the starter does not migrate an installed extension or an existing bundled schema.

For extension-managed databases, install the PGMQ 1.13.1 extension files on the server and migrate:

```sql
ALTER EXTENSION pgmq UPDATE TO '1.13.1';
```

For a schema installed from bundled SQL, manage the upstream upgrade path in your database migration
process, adapting extension-specific statements as needed. Do not rerun the fresh-install resource
against an existing schema. Older starter version labels did not always identify an upstream release
exactly; inspect installed function definitions when selecting the migration starting point.

PGMQ 1.13.1 hardens search paths and restricts `pg_monitor` privileges. Monitoring roles that previously
read payloads or notification state through broad grants need appropriate explicit grants. Review
upgrade warnings about revoking old privileges. Re-enable insert notifications for existing mixed-case
queues when their triggers predate the upstream fix.

## Batch lists and headers

Read-only parameters accept `List<?>`, `Map<String, ?>` and covariant lists of header maps. Typed DTO
lists and `Map<String, String>` now select the intended overloads instead of being packaged as varargs
payloads. Use `send(queue, list)` when the whole list is one message.

Recompile callers. Bytecode compiled against the old invariant generic signatures may already invoke
the varargs method; replacing the JAR does not change that compiled call. Template subclasses that
override affected methods must update their generic signatures as well. Header expansion and `_type`
enrichment still occur through the common validated send path.

## Listener execution

The default factory requires synchronous `void` methods and rejects asynchronous return types and
method/class `@Async` when registering a listener. Finish business work before returning. Custom
container factories own their execution contract.

Lazy singleton beans register when initialized after startup. Prototype listener beans are rejected.
Failed dynamic registration permits another attempt with the same ID, and state gauges follow the
replacement container. Provide explicit IDs for stable monitoring and administration.

Numeric annotation options inherit properties only for `-1`; other negative values fail runtime
option validation. `autoStartup` and JSON `conditional` annotation strings resolve through Spring.
Boolean values use Spring conversion; misspellings no longer silently disable startup.

Pollers and consumers are separate. Replace old `concurrency`/`pgmq.listener.concurrency` settings with
`consumerConcurrency`/`consumer-concurrency`, `pollConcurrency`/`poll-concurrency`, and `maxInFlight`/
`max-in-flight`. Capacity must cover both concurrency values. Custom executor instances belong to one
container, and must not be shared directly across containers.

`AUTO` archives each message after successful execution. Manual acknowledgement handles serialize
completion attempts and mark completion after SQL returns. Database exceptions allow another attempt;
transaction rollback and uncertain network outcomes still require idempotent business processing.
Use `MANUAL` with the same transactional data source when business changes and archive must roll back
together. Discard a handle after its delivery finishes.

## Type hints and read defaults

`_type` is a best-effort producer hint. If it cannot be used, reads retain generic JSON values and the
original headers. Typed listener conversion occurs per message, allowing error handling by message ID.
Direct callers requiring a concrete type should use `convertMessagePayload` explicitly.

Read, poll, pop and partition overloads delegate omitted arguments to SQL defaults. Zero-duration poll
calls perform one immediate read; listeners wait the configured interval after an empty immediate read.
This corrects the upstream zero-duration polling function's immediate empty return behavior.

PGMQ 1.13.1 adds partition `premake` and the nullable `defaultPartitionLength` metric. The legacy
seven-argument metrics constructor remains available. Recompile clients using the metrics record.
`detachArchive` is restored only as a deprecated SQL no-op. The deprecated boolean `dropQueue` overload
ignores that flag and returns true unconditionally, following upstream; prefer `dropQueue(queue)`.

## Legacy package layout

The old `io.github.idoly.pgmq.core` layout is no longer supported. Main replacements are:

| Previous role | Current type/package |
| --- | --- |
| SQL client | `template.PgmqTemplate` |
| Message | `message.PgmqMessage` |
| Queue declaration/metadata | `queue.PgmqQueue` |
| Queue metrics row | `metrics.PgmqMetrics` |
| Micrometer database gauges | `metrics.PgmqMetricsBinder` |
| Listener annotation | `annotation.PgmqListener` |
| Listener execution | `listener.container` |
| Method registration | `listener.endpoint` |

Incremental classpath SQL upgrade resources are no longer bundled. Use explicit database migrations.
Detailed historical removals remain in [CHANGELOG.md](CHANGELOG.md).

## Test and build setup

Database tests now use one explicit JDBC configuration. Remove old `DOCKER_HOST` and Ryuk settings
from project test instructions. Testcontainers dependencies and its container base class have been
removed; local Docker/Podman is an optional way to supply a server, not a required test framework.

The configuration processor remains on the compiler's processor path; its duplicate ordinary
dependency was removed. Source publication uses the non-forking source goal. `coverage` is an optional
build profile with 90% line and 75% branch checks; it needs the integration database for the intended
full run. See [testing.md](docs/testing.md). Publishing remains isolated in the explicit `release` profile.
