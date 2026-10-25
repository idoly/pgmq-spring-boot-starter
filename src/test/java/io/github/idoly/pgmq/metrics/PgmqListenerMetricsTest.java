package io.github.idoly.pgmq.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;

class PgmqListenerMetricsTest {

    @Test
    void retryWithSameListenerIdRebindsGaugesToTheNewContainer() {
        var registry = new SimpleMeterRegistry();
        var metrics = new PgmqListenerMetrics();
        var failed = mock(PgmqListenerContainer.class);
        when(failed.getId()).thenReturn("orders");
        when(failed.getQueue()).thenReturn("queue");
        var healthy = mock(PgmqListenerContainer.class);
        when(healthy.getId()).thenReturn("orders");
        when(healthy.getQueue()).thenReturn("queue");
        when(healthy.isRunning()).thenReturn(true);
        when(healthy.getInFlightCount()).thenReturn(2);

        metrics.bindTo(registry);
        metrics.registerContainer(failed);
        assertEquals(0.0, registry.get("pgmq.listener.running").gauge().value());
        metrics.recordProcessing("orders", "queue", 1, "error");
        metrics.registerContainer(healthy);
        metrics.registerContainer(healthy);

        assertEquals(1.0, registry.get("pgmq.listener.running").gauge().value());
        assertEquals(2.0, registry.get("pgmq.listener.in_flight").gauge().value());
        assertEquals(1.0, registry.get("pgmq.listener.messages").counter().count());
        var secondRegistry = new SimpleMeterRegistry();
        metrics.bindTo(secondRegistry);
        assertEquals(1.0, secondRegistry.get("pgmq.listener.running").gauge().value());
    }

    @Test
    void aFailingMeterRegistryDoesNotBreakMessageProcessing() {
        var registry = new SimpleMeterRegistry();
        registry.config().meterFilter(new io.micrometer.core.instrument.config.MeterFilter() {
            @Override
            public io.micrometer.core.instrument.Meter.Id map(io.micrometer.core.instrument.Meter.Id id) {
                throw new IllegalStateException("exporter registration failed");
            }
        });
        var metrics = new PgmqListenerMetrics();
        metrics.bindTo(registry);
        assertDoesNotThrow(() -> metrics.recordPoll("orders", "queue", 1, "success"));
        assertDoesNotThrow(() -> metrics.recordProcessing("orders", "queue", 1, "error"));
        registry.close();
    }

    @Test
    void publishesContainerStateAndExecutionMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PgmqListenerMetrics metrics = new PgmqListenerMetrics();
        PgmqListenerContainer container = mock(PgmqListenerContainer.class);
        when(container.getId()).thenReturn("orders-listener");
        when(container.getQueue()).thenReturn("orders");
        when(container.isRunning()).thenReturn(true);
        when(container.getInFlightCount()).thenReturn(3);

        metrics.bindTo(registry);
        metrics.bindTo(registry);
        metrics.registerContainer(container);
        metrics.registerContainer(container);
        metrics.recordPoll("orders-listener", "orders", 1_000_000, "success");
        metrics.recordProcessing("orders-listener", "orders", 2_000_000, "error");

        assertEquals(1.0, registry.get("pgmq.listener.running").gauge().value());
        assertEquals(3.0, registry.get("pgmq.listener.in_flight").gauge().value());
        assertEquals(1.0, registry.get("pgmq.listener.polls").tag("result", "success").counter().count());
        assertEquals(1.0, registry.get("pgmq.listener.messages").tag("result", "error").counter().count());
        assertEquals(1.0, registry.get("pgmq.listener.poll.duration").timer().totalTime(TimeUnit.MILLISECONDS));
        assertEquals(2.0, registry.get("pgmq.listener.processing.duration").timer()
                .totalTime(TimeUnit.MILLISECONDS));
    }
}
