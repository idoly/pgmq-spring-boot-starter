package io.github.idoly.pgmq.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;

class PgmqListenerMetricsTest {

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
