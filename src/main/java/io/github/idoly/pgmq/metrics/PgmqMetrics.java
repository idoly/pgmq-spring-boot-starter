package io.github.idoly.pgmq.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.github.idoly.pgmq.core.Metrics;
import io.github.idoly.pgmq.core.PgmqTemplate;

/**
 * Binds PGMQ queue metrics to a Micrometer {@link MeterRegistry}.
 * <p>
 * This class registers gauges for each queue to monitor metrics such as:
 * queue length, visible messages, newest and oldest message age, and total messages.
 */
public class PgmqMetrics implements MeterBinder {

    /** The PgmqTemplate used to fetch queue metrics */
    private final PgmqTemplate pgmqTemplate;

    /**
     * Constructs a PgmqMetrics binder with the given {@link PgmqTemplate}.
     *
     * @param pgmqTemplate the template to access PGMQ metrics
     */
    public PgmqMetrics(PgmqTemplate pgmqTemplate) {
        this.pgmqTemplate = pgmqTemplate;
    }

    /**
     * Binds queue metrics to the provided {@link MeterRegistry}.
     * <p>
     * For each queue, the following metrics are registered:
     * <ul>
     *     <li>{@code pgmq.queue.length} – number of messages currently in the queue</li>
     *     <li>{@code pgmq.queue.visible_length} – number of messages visible in the queue (vt ≤ now)</li>
     *     <li>{@code pgmq.queue.newest_msg_age_sec} – age of the newest message in seconds</li>
     *     <li>{@code pgmq.queue.oldest_msg_age_sec} – age of the oldest message in seconds</li>
     *     <li>{@code pgmq.queue.total_messages} – total number of messages ever in the queue</li>
     * </ul>
     *
     * @param registry the meter registry to bind metrics to
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        pgmqTemplate.getMetricsAll().forEach(result -> {
            String queue = result.queueName();

            Gauge.builder("pgmq.queue.length", result, Metrics::queueLength)
                    .description("Number of messages currently in the queue")
                    .tag("queue", queue)
                    .register(registry);

            Gauge.builder("pgmq.queue.visible_length", result, Metrics::queueVisibleLength)
                    .description("Number of messages currently visible in the queue (vt <= now)")
                    .tag("queue", queue)
                    .register(registry);

            Gauge.builder("pgmq.queue.newest_msg_age_sec", result, Metrics::newestMsgAgeSec)
                    .description("Age of the newest message in the queue in seconds")
                    .tag("queue", queue)
                    .register(registry);

            Gauge.builder("pgmq.queue.oldest_msg_age_sec", result, Metrics::oldestMsgAgeSec)
                    .description("Age of the oldest message in the queue in seconds")
                    .tag("queue", queue)
                    .register(registry);

            Gauge.builder("pgmq.queue.total_messages", result, Metrics::totalMessages)
                    .description("Total number of messages that have ever been in the queue")
                    .tag("queue", queue)
                    .register(registry);
        });
    }
}

