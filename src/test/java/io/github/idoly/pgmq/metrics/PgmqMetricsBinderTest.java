package io.github.idoly.pgmq.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.github.idoly.pgmq.notification.NotifyInsertThrottle;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqMetricsBinderTest {

    @Test
    void bindToDoesNotQueryDatabase() {
        StubPgmqTemplate pgmqTemplate = new StubPgmqTemplate();
        PgmqMetricsBinder binder = new PgmqMetricsBinder(pgmqTemplate, Duration.ofMillis(10));

        binder.bindTo(new SimpleMeterRegistry());

        assertEquals(0, pgmqTemplate.metricsCalls.get());
        assertEquals(0, pgmqTemplate.throttleCalls.get());
    }

    @Test
    void refreshPublishesQueueAndNotificationRows() {
        StubPgmqTemplate pgmqTemplate = new StubPgmqTemplate();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PgmqMetricsBinder binder = new PgmqMetricsBinder(pgmqTemplate, Duration.ofMillis(10));

        binder.bindTo(registry);
        binder.refresh();

        Gauge queueLength = gauge(registry, "pgmq.queue.length", "jobs");
        Gauge newestAge = gauge(registry, "pgmq.queue.newest_msg_age_sec", "jobs");
        Gauge throttle = gauge(registry, "pgmq.notify_insert.throttle_interval_ms", "jobs");
        Gauge lastNotified = gauge(registry, "pgmq.notify_insert.last_notified_at_epoch_seconds", "jobs");

        assertEquals(2.0, queueLength.value());
        assertEquals(3.0, gauge(registry, "pgmq.queue.default_partition_length", "jobs").value());
        assertTrue(Double.isNaN(gauge(registry, "pgmq.queue.default_partition_length", "plain").value()));
        assertTrue(Double.isNaN(newestAge.value()));
        assertEquals(250.0, throttle.value());
        assertEquals(OffsetDateTime.parse("2026-07-04T00:00:00Z").toEpochSecond(), lastNotified.value());
    }

    private static Gauge gauge(SimpleMeterRegistry registry, String name, String queue) {
        Gauge gauge = registry.find(name).tag("queue", queue).gauge();
        assertNotNull(gauge);
        return gauge;
    }

    private static class StubPgmqTemplate extends PgmqTemplate {

        private final AtomicInteger metricsCalls = new AtomicInteger();

        private final AtomicInteger throttleCalls = new AtomicInteger();

        StubPgmqTemplate() {
            super(new JdbcTemplate(), new ObjectMapper());
        }

        @Override
        public List<PgmqMetrics> getMetricsAll() {
            metricsCalls.incrementAndGet();
            return List.of(new PgmqMetrics(
                    "jobs",
                    2L,
                    null,
                    7,
                    5L,
                    OffsetDateTime.parse("2026-07-04T00:00:00Z"),
                    1L,
                    3L
            ), new PgmqMetrics("plain", 0L, null, null, 0L,
                    OffsetDateTime.parse("2026-07-04T00:00:00Z"), 0L));
        }

        @Override
        public List<NotifyInsertThrottle> listNotifyInsertThrottles() {
            throttleCalls.incrementAndGet();
            return List.of(new NotifyInsertThrottle(
                    "jobs",
                    250,
                    OffsetDateTime.parse("2026-07-04T00:00:00Z")
            ));
        }
    }
}
