package io.github.idoly.pgmq.metrics;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;

import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;

/**
 * Micrometer metrics for listener polling, processing, and in-flight state.
 */
public class PgmqListenerMetrics implements MeterBinder {

    private static final Log logger = LogFactory.getLog(PgmqListenerMetrics.class);

    /**
     * Create an initially unbound listener metric recorder.
     */
    public PgmqListenerMetrics() {
    }

    private final CopyOnWriteArrayList<MeterRegistry> registries = new CopyOnWriteArrayList<>();

    private final CopyOnWriteArrayList<PgmqListenerContainer> containers = new CopyOnWriteArrayList<>();

    /**
     * Bind existing and future listener metrics to a registry.
     *
     * @param registry target meter registry
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        Assert.notNull(registry, "MeterRegistry must not be null");
        if (registries.addIfAbsent(registry)) {
            containers.forEach(container -> registerGauges(registry, container));
        }
    }

    /**
     * Register one listener container for state gauges.
     *
     * @param container listener container
     */
    public void registerContainer(PgmqListenerContainer container) {
        Assert.notNull(container, "PgmqListenerContainer must not be null");
        if (containers.addIfAbsent(container)) {
            registries.forEach(registry -> registerGauges(registry, container));
        }
    }

    /**
     * Record one database poll operation.
     *
     * @param listenerId listener id
     * @param queue queue name
     * @param durationNanos elapsed nanoseconds
     * @param result success or error
     */
    public void recordPoll(String listenerId, String queue, long durationNanos, String result) {
        forEachRegistry(registry -> {
            counter(registry, "pgmq.listener.polls", listenerId, queue, result).increment();
            timer(registry, "pgmq.listener.poll.duration", "PGMQ listener poll duration",
                    listenerId, queue, result).record(durationNanos, TimeUnit.NANOSECONDS);
        });
    }

    /**
     * Record one listener invocation and acknowledgement outcome.
     *
     * @param listenerId listener id
     * @param queue queue name
     * @param durationNanos elapsed nanoseconds
     * @param result success or error
     */
    public void recordProcessing(String listenerId, String queue, long durationNanos, String result) {
        forEachRegistry(registry -> {
            counter(registry, "pgmq.listener.messages", listenerId, queue, result).increment();
            timer(registry, "pgmq.listener.processing.duration", "PGMQ listener processing duration",
                    listenerId, queue, result).record(durationNanos, TimeUnit.NANOSECONDS);
        });
    }

    private void registerGauges(MeterRegistry registry, PgmqListenerContainer container) {
        Gauge.builder("pgmq.listener.running", container, value -> value.isRunning() ? 1 : 0)
                .description("Whether the PGMQ listener container is running")
                .tags("listener", container.getId(), "queue", container.getQueue())
                .register(registry);
        Gauge.builder("pgmq.listener.in_flight", container, PgmqListenerContainer::getInFlightCount)
                .description("Number of PGMQ messages queued or executing")
                .tags("listener", container.getId(), "queue", container.getQueue())
                .register(registry);
    }

    private Counter counter(MeterRegistry registry, String name, String listenerId, String queue, String result) {
        return Counter.builder(name)
                .tags("listener", listenerId, "queue", queue, "result", result)
                .register(registry);
    }

    private void forEachRegistry(Consumer<MeterRegistry> action) {
        registries.forEach(registry -> {
            try {
                action.accept(registry);
            } catch (RuntimeException error) {
                logger.warn("failed to record PGMQ listener metric", error);
            }
        });
    }

    private Timer timer(MeterRegistry registry, String name, String description,
            String listenerId, String queue, String result) {
        return Timer.builder(name)
                .description(description)
                .tags("listener", listenerId, "queue", queue, "result", result)
                .register(registry);
    }
}
