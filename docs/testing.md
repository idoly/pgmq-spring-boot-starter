# Verification, coverage and performance

## One database contract

All integration suites use a caller-owned disposable PostgreSQL database:

```shell
export PGMQ_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/pgmq_test
export PGMQ_TEST_USERNAME=postgres
export PGMQ_TEST_PASSWORD=postgres
mvn -Pboot35,coverage clean verify
```

Use a dedicated database. Tests install PGMQ when needed and create/drop queues and business tables.
`PostgresIntegrationSupport` supplies the JDBC settings to both Spring Boot tests and direct JDBC
checks. A local container, an installed server or a CI service can provide the same contract. The
project no longer starts a separate Testcontainers/Podman environment.

The three database test classes opt in only when `PGMQ_TEST_JDBC_URL` is set. `mvn test` without it
runs unit tests and reports the database suites as skipped. Missing pg_partman skips its partition
case locally. Set `PGMQ_TEST_REQUIRE_PARTMAN=true` to require that dependency; CI sets this flag.

## Commands

```shell
mvn test
mvn -Pboot35,coverage clean verify
mvn -Pboot40,coverage clean verify
mvn -Pboot41,coverage clean verify
```

The last three commands require the database settings above for the intended coverage run. Each
starts a clean build, runs the full suite, generates configuration metadata, attaches source/Javadoc
artifacts and checks coverage. The optional `coverage` profile adds JaCoCo only to the build; it does
not add application runtime dependencies.

Reports are generated under:

- `target/surefire-reports`: JUnit outcomes and diagnostic output.
- `target/site/jacoco/index.html`: navigable Java coverage.
- `target/site/jacoco/jacoco.xml`: machine-readable counters.
- `target/classes/META-INF/spring-configuration-metadata.json`: generated Boot property metadata.

The coverage profile enforces project-wide floors of **90% line** and **75% branch** coverage. No production packages
are excluded to raise the percentages. Java coverage does not measure PostgreSQL function bodies,
network fault tolerance or performance. A skipped case is not a successful runtime verification.

CI runs the same full `verify` on Boot 3.5, 4.0 and 4.1 with Java 17, one pinned PostgreSQL/pg_partman
service per job, and uploaded JUnit/coverage reports. Service startup failure or missing required
pg_partman fails the job. The workflow source contains the authoritative image digest.

## What the tests protect

- Public SQL binding, omitted defaults, FIFO modes, topic routing, notification triggers and metrics.
- Typed DTO/header overload selection, serialization failure before SQL, SQL NULL values and
  unusable producer type hints.
- Synchronous listener registration, lazy singleton registration, prototype rejection and failed
  dynamic startup followed by retry with the same ID.
- Executor creation failure, stop/restart isolation, late JDBC results, bounded capacity and
  overflow-safe quota calculation without allocating an enormous default work queue.
- Successful/failed/concurrent acknowledgement attempts and transaction rollback of business work
  together with manual ACK.
- Small shared pools, acquisition deadlines, slow-query cancellation, connection reuse and the
  distinction between interruption and query cancellation.
- Meter registry failure isolation and gauge replacement after retry.

The tests use framework facilities, real PostgreSQL and bounded latches at observable boundaries.
They do not inject VM exhaustion, assert fragile elapsed-time throughput targets, or create
application-sized thread pools merely to exercise an integer limit.

## Manual metrics component benchmark

`ListenerMetricsBenchmark` is a source-level development tool, not a Surefire test or a timing gate.
It uses a HotSpot JDK's per-thread allocation counters and `SimpleMeterRegistry`, warms three rounds,
then measures five rounds. Each round records 200,000 poll events and 200,000 processing events
across four listener IDs; final counter values are checked.

Compile it and build a test classpath:

```shell
mvn -Pboot35 test-compile dependency:build-classpath -Dmdep.outputFile=target/test-classpath.txt
java -Xms256m -Xmx256m -cp "target/test-classes:target/classes:$(cat target/test-classpath.txt)" \
  io.github.idoly.pgmq.metrics.ListenerMetricsBenchmark
```

On Windows, use `;` instead of `:` between classpath entries and read the classpath file with
PowerShell `Get-Content -Raw`. The benchmark requires a JDK with thread-allocation counters.

Compare identical code paths, registry settings, tag cardinality, JVM flags and warmup. Run both
orders in separate JVMs. Report allocation and elapsed time separately. This workload does not
access PostgreSQL or measure end-to-end queue throughput; histogram/exporter settings, contention
and database time can dominate a real application.

## Reproducible evidence and maintenance

The [ablation report](experiments/listener-ablation.md) records the measured baseline, focused
withdrawal experiments, coverage limits and component allocation comparison. Keep those results
distinct from claims about untested environments.

Before committing, run `git diff --check`, review public API/SQL mappings and confirm the generated
metadata and publication artifacts still exist. `.editorconfig` and `.gitattributes` keep text
encoding and line endings consistent. Existing caller bytecode may need recompilation after generic
parameter changes; see [migration notes](../MIGRATION.md).
