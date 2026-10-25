# Ablation and verification evidence

This report replaces the older append-only experiment log. It describes the current implementation
and the temporary withdrawal experiments performed during this cleanup. All variants used isolated
source copies; no production changes were reverted for the experiments.

## Environment and scope

- Windows HotSpot JDK 25.0.2, Maven 3.9.14, Java 17 release compilation.
- Disposable PostgreSQL 18.4, with the bundled PGMQ schema installed by the tests.
- Local pg_partman was unavailable. Its one positive partition test was reported as skipped.
- Functional verification used Boot 3.5.0, 4.0.0 and 4.1.0. CI is configured to require pg_partman,
  but GitHub-hosted execution is not a result measured in this local experiment.
- Upstream `/releases/latest` resolved to **v1.13.1**. The bundled install script matched that
  upstream file after line-ending normalization, SHA-256:
  `e861fcbd8c602df70a00eb9d230b1a2f591860a2230b9a6084172e3eff463642`.

The initial baseline was the working implementation containing the preceding reliability fixes,
before this round's validation/test-infrastructure/provider cleanup. Its coverage run executed
98 tests successfully, excluding the two old container-based suites. It is not the older Git HEAD
without those pending fixes.

## Withdrawal experiments

Each row ran the directly relevant tests. Different row totals are different focused subsets, not
comparable coverage scores. Expected failures below demonstrate a removed property; they are not
compilation errors or failed dependency downloads.

| Variant | Tests | Failures | Errors | Observation |
| --- | ---: | ---: | ---: | --- |
| Current behavior control | 74 | 0 | 0 | Focused unit and real-JDBC controls pass. |
| Restore duplicate factory numeric checks | 6 | 0 | 0 | Same configuration cases pass with or without intermediate checks. |
| Restore invariant payload `List<Object>` parameters | 2 | 2 | 0 | Typed queue/topic lists fall into varargs and encode one list payload. |
| Remove async declaration checks | 17 | 4 | 0 | Future-returning and `@Async` listeners are accepted instead of rejected. |
| Remove post-startup registration only | 11 | 2 | 0 | Lazy singleton listeners are silently omitted. |
| Restore shared run resources | 7 | 3 | 0 | Late completion accesses cleared permits, contaminates new capacity, or dispatches into a new run. |
| Restore pre-SQL ACK completion | 4 | 2 | 0 | Failed completion remains completed and in-progress SQL reports completion. |
| Restore strict type-hint batch mapping | 4 | 3 | 1 | Missing, incompatible and non-string hints hide a healthy message; direct stale-hint reads throw. |
| Publish dynamic registration before successful startup | 6 | 1 | 0 | A failed container remains registered, preventing same-ID retry. |
| Retain atomic run-active flag | 7 | 0 | 0 | Lifecycle cases also pass with AtomicBoolean; lifecycle writes are already serialized. |

Numeric-check controls shared the same Spring Boolean resolver, strict `-1` sentinel and tests;
only the redundant positive/non-negative checks changed. They were repeated after the Boolean
conversion cleanup. Properties' public setter checks and the resolved options constructor remain
separate supported boundaries.

The run-resource withdrawal retained immediate-read backoff, safe quota arithmetic and the current
ACK call path. Thus its failures concern resource ownership rather than those independent changes.
The late-registration withdrawal retained prototype rejection. The registration withdrawal retained
cleanup and only moved map publication ahead of successful start. A test cleanup stub was made
one-shot so its secondary exception could not mask the stale-registration assertion.

The active-flag control shows that the run-state object is useful while the extra atomic wrapper is
not required. The final code uses a volatile flag: start/stop serialize writes, and pollers need
visibility without acquiring that lock. The per-run resource aggregation is retained.

These are bounded experiments, not mathematical proofs for every possible executor or scheduler.
They support retaining the fixes and removing duplicate transit validation. Focused concurrency
checks use latches and bounded observation windows instead of assuming immediate thread scheduling.

## Coverage and test expansion

No project packages were excluded from JaCoCo. Built-in compiler-generated-code filtering still
applies. The initial and final test populations differ because the old integration suites now run
through the shared JDBC setup; this is an expansion in exercised behavior as well as code cleanup.

| Measurement | Covered lines / total | Line coverage | Covered branches / total | Branch coverage |
| --- | ---: | ---: | ---: | ---: |
| Initial baseline, Boot 3.5 | 1064 / 1281 | 83.06% | 246 / 336 | 73.21% |
| Final Boot 3.5 | 1164 / 1277 | 91.15% | 262 / 336 | 77.98% |
| Final Boot 4.0 | 1164 / 1277 | 91.15% | 263 / 336 | 78.27% |
| Final Boot 4.1 | 1164 / 1277 | 91.15% | 263 / 336 | 78.27% |

Each final full run discovered **130 cases: 129 passed, one pg_partman case skipped, zero failures
and errors**. All three `clean verify` runs passed the 90% line / 75% branch floors and produced
configuration metadata plus source/Javadoc artifacts. The metadata still contained 15 properties
after removing the duplicate processor dependency.

The no-database run also passed: 128 discovered cases, 87 passed and 41 skipped. JUnit reports a
parameterized class differently when its environment condition disables it, so that total is not
the full-database total minus only one test.

Java coverage does not instrument SQL function bodies. The partition-success path, network outages,
JVM exhaustion and arbitrary custom executor behavior are not established by these percentages.

## Metrics allocation comparison

The manual `ListenerMetricsBenchmark` used Micrometer 1.15.0 / `SimpleMeterRegistry`, four listener
IDs, one queue/result, a 256 MiB fixed heap, three warmup rounds and five measured rounds. Each round
recorded 200,000 polls and 200,000 processing events. Counter totals were checked. Both execution
orders used separate JVMs and the identical fixture/classpath.

| Component implementation | Median bytes / 400,000 events | Bytes / event | Median time, order 1 | Median time, reversed order |
| --- | ---: | ---: | ---: | ---: |
| Counter/timer builders and separate tags per record | 233,600,032 | about 584 | 36.82 ms | 36.38 ms |
| Micrometer providers and shared immutable event tags | 105,600,032 | about 264 | 23.10 ms | 23.22 ms |

Measured allocation fell by **54.8%** in both orders. Component elapsed time fell by about 36–37%
on this setup. This does not establish the same improvement for PostgreSQL queue throughput,
multiple exporters, histograms or contention. The retained change uses Micrometer's provider and
meter lookup facilities rather than introducing an application meter cache.

## Cleanup and architectural decisions

Retained: one public SQL facade, the existing factory extension points, per-run resource ownership,
synchronous default listener semantics, first-successful-SQL ACK state and explicit database setup.
The role separation between discovery, option resolution, lifecycle and invocation remains useful.

Simplified: factory-only numeric validation was removed; empty-read and shared-header delegation
validate at the common boundary; Spring supplies string/Boolean/thread facilities; source attachment
no longer forks the build lifecycle.

Removed: the old container test base class, both Testcontainers dependency declarations and their
version property, the separate Podman job, and the configuration processor's duplicate ordinary
dependency. The processor path was retained and verified. On the Boot 3.5 test classpath, JAR entries
fell from **77 to 62**. Production JDBC/Jackson/Micrometer dependencies remain required; dynamic driver
loading and compiler processing are not classified as unused by import counts.

Public annotations, endpoint metadata and factory interfaces were preserved where applications can
consume them. Documentation was rebuilt around the current behavior, and text encoding/line endings
were normalized without rewriting the upstream SQL. Generated classes, reports, variants, logs and
the temporary database are cleanup artifacts, not checked-in application files.
