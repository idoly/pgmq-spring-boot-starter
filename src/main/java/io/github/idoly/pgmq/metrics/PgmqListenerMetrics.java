package io.github.idoly.pgmq.metrics;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;

import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;

/** Micrometer metrics for listener polling, processing, and in-flight state. */
public class PgmqListenerMetrics implements MeterBinder {

    private static final Log logger = LogFactory.getLog(PgmqListenerMetrics.class);

    private final CopyOnWriteArrayList<RegistryMeters> registries = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PgmqListenerContainer> containers = new CopyOnWriteArrayList<>();

    /** Create an initially unbound listener metric recorder. */
    public PgmqListenerMetrics() {
    }

    /**
     * Bind existing and future listener metrics to a registry.
     *
     * @param registry target meter registry
     */
    @Override
    public synchronized void bindTo(MeterRegistry registry) {
        Assert.notNull(registry, "MeterRegistry must not be null");
        if (registries.stream().noneMatch(bound -> bound.registry() == registry)) {
            registries.add(RegistryMeters.create(registry));
            containers.forEach(container -> registerGauges(registry, container));
        }
    }

    /**
     * Register state gauges, replacing an earlier container with the same listener and queue.
     *
     * @param container listener container
     */
    public synchronized void registerContainer(PgmqListenerContainer container) {
        Assert.notNull(container, "PgmqListenerContainer must not be null");
        if (containers.contains(container)) {
            return;
        }
        boolean replaced = containers.removeIf(existing -> existing.getId().equals(container.getId())
                && existing.getQueue().equals(container.getQueue()));
        containers.add(container);
        registries.forEach(bound -> {
            MeterRegistry registry = bound.registry();
            if (replaced) {
                registry.find("pgmq.listener.running").tags("listener", container.getId(), "queue", container.getQueue())
                        .gauges().forEach(registry::remove);
                registry.find("pgmq.listener.in_flight").tags("listener", container.getId(), "queue", container.getQueue())
                        .gauges().forEach(registry::remove);
            }
            registerGauges(registry, container);
        });
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
        forEachRegistry(bound -> {
            Tags tags = Tags.of("listener", listenerId, "queue", queue, "result", result);
            bound.polls().withTags(tags).increment();
            bound.pollDuration().withTags(tags).record(durationNanos, TimeUnit.NANOSECONDS);
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
        forEachRegistry(bound -> {
            Tags tags = Tags.of("listener", listenerId, "queue", queue, "result", result);
            bound.messages().withTags(tags).increment();
            bound.processingDuration().withTags(tags).record(durationNanos, TimeUnit.NANOSECONDS);
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

    private void forEachRegistry(Consumer<RegistryMeters> action) {
        registries.forEach(bound -> {
            try {
                action.accept(bound);
            } catch (RuntimeException error) {
                logger.warn("failed to record PGMQ listener metric", error);
            }
        });
    }

    /** Providers reuse Micrometer's registration path; tag sets are shared by each counter/timer pair. */
    private record RegistryMeters(MeterRegistry registry, MeterProvider<Counter> polls,
            MeterProvider<Timer> pollDuration, MeterProvider<Counter> messages,
            MeterProvider<Timer> processingDuration) {
        static RegistryMeters create(MeterRegistry registry) {
            return new RegistryMeters(registry,
                    Counter.builder("pgmq.listener.polls").withRegistry(registry),
                    Timer.builder("pgmq.listener.poll.duration").description("PGMQ listener poll duration").withRegistry(registry),
                    Counter.builder("pgmq.listener.messages").withRegistry(registry),
                    Timer.builder("pgmq.listener.processing.duration").description("PGMQ listener processing duration")
                            .withRegistry(registry));
        }
    }
}
