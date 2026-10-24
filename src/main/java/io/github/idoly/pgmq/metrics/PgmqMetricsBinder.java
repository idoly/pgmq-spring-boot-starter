package io.github.idoly.pgmq.metrics;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import io.github.idoly.pgmq.notification.NotifyInsertThrottle;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Micrometer binder for PGMQ queue and notification gauges.
 *
 * <p>Registers {@link MultiGauge} meters without querying the database during
 * binding. A lifecycle-managed sampler refreshes queue-tagged rows
 * periodically; nullable database values are exported as {@code NaN}, which is
 * Micrometer's gauge representation for missing numeric values.</p>
 */
public class PgmqMetricsBinder implements MeterBinder, SmartLifecycle {

    /** Default interval between metric refreshes. */
    public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofSeconds(30);

    /** Logger used for metric refresh failures. */
    private static final Log logger = LogFactory.getLog(PgmqMetricsBinder.class);

    /** Template used to sample queue and notification metrics. */
    private final PgmqTemplate pgmqTemplate;

    /** Interval between metric refreshes. */
    private final Duration refreshInterval;

    /** Meter sets registered for one or more registries. */
    private final List<MeterSet> meterSets = new CopyOnWriteArrayList<>();

    /** Binder running state used to guard lifecycle transitions. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Scheduler owned by this binder while running. */
    private ScheduledExecutorService scheduler;

    /**
     * Create a binder backed by the given template.
     *
     * @param pgmqTemplate the template to access PGMQ metrics
     */
    public PgmqMetricsBinder(PgmqTemplate pgmqTemplate) {
        this(pgmqTemplate, DEFAULT_REFRESH_INTERVAL);
    }

    /**
     * Create a binder with a custom refresh interval.
     *
     * @param pgmqTemplate the template to access PGMQ metrics
     * @param refreshInterval interval between refreshes
     */
    public PgmqMetricsBinder(PgmqTemplate pgmqTemplate, Duration refreshInterval) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(refreshInterval, "Refresh interval must not be null");
        Assert.isTrue(!refreshInterval.isNegative() && !refreshInterval.isZero(),
                "Refresh interval must be greater than 0");
        this.pgmqTemplate = pgmqTemplate;
        this.refreshInterval = refreshInterval;
    }

    /**
     * Register queue and notification multi-gauges without sampling the database.
     *
     * @param registry the meter registry to bind metrics to
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        Assert.notNull(registry, "MeterRegistry must not be null");
        meterSets.add(MeterSet.register(registry));
        if (running.get()) {
            startSchedulerIfNecessary();
        }
    }

    /**
     * Start periodic metric refresh.
     */
    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        startSchedulerIfNecessary();
    }

    /**
     * Start the refresh scheduler once a registry has been bound.
     */
    private synchronized void startSchedulerIfNecessary() {
        if (!running.get() || scheduler != null || meterSets.isEmpty()) {
            return;
        }
        AtomicInteger threadIndex = new AtomicInteger();
        scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task);
            thread.setName("pgmq-metrics-" + threadIndex.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::refreshSafely, 0,
                refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop periodic metric refresh.
     */
    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        ScheduledExecutorService currentScheduler = scheduler;
        if (currentScheduler != null) {
            currentScheduler.shutdownNow();
            scheduler = null;
        }
    }

    /**
     * Stop the binder and notify the lifecycle callback.
     */
    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    /**
     * Return whether periodic refresh is running.
     *
     * @return running state
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Always participate in Spring auto-startup.
     *
     * @return {@code true}
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Run late so optional auto-initialization can complete first.
     *
     * @return lifecycle phase
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /**
     * Refresh metrics and keep database failures from failing application startup.
     */
    private void refreshSafely() {
        if (meterSets.isEmpty()) {
            return;
        }

        try {
            refresh();
        } catch (RuntimeException e) {
            logger.warn("failed to refresh PGMQ metrics", e);
        }
    }

    /**
     * Sample PGMQ and publish the latest queue-tagged rows.
     */
    void refresh() {
        List<PgmqMetrics> queueMetrics = pgmqTemplate.getMetricsAll();
        List<NotifyInsertThrottle> throttles = pgmqTemplate.listNotifyInsertThrottles();
        for (MeterSet meterSet : meterSets) {
            meterSet.refresh(queueMetrics, throttles);
        }
    }

    /**
     * Convert the notification timestamp to an epoch-second gauge value.
     *
     * @param throttle notification throttle row
     * @return epoch seconds or {@code null} when no notification has been sent
     */
    private static Long lastNotifiedAtEpochSecond(NotifyInsertThrottle throttle) {
        return throttle.lastNotifiedAt() == null ? null : throttle.lastNotifiedAt().toEpochSecond();
    }

    /**
     * Queue-tagged multi-gauges registered for one meter registry.
     */
    private record MeterSet(
            MultiGauge queueLength,
            MultiGauge queueVisibleLength,
            MultiGauge newestMsgAgeSec,
            MultiGauge oldestMsgAgeSec,
            MultiGauge totalMessages,
            MultiGauge defaultPartitionLength,
            MultiGauge notifyThrottleIntervalMs,
            MultiGauge notifyLastNotifiedAtEpochSeconds
    ) {

        /**
         * Register all multi-gauges in the target registry.
         */
        private static MeterSet register(MeterRegistry registry) {
            return new MeterSet(
                    multiGauge(registry, "pgmq.queue.length", "Number of messages currently in the queue"),
                    multiGauge(registry, "pgmq.queue.visible_length",
                            "Number of messages currently visible in the queue (vt <= now)"),
                    multiGauge(registry, "pgmq.queue.newest_msg_age_sec",
                            "Age of the newest message in the queue in seconds"),
                    multiGauge(registry, "pgmq.queue.oldest_msg_age_sec",
                            "Age of the oldest message in the queue in seconds"),
                    multiGauge(registry, "pgmq.queue.total_messages",
                            "Total number of messages that have ever been in the queue"),
                    multiGauge(registry, "pgmq.queue.default_partition_length",
                            "Estimated messages in queue and archive default partitions"),
                    multiGauge(registry, "pgmq.notify_insert.throttle_interval_ms",
                            "Minimum milliseconds between insert notifications"),
                    multiGauge(registry, "pgmq.notify_insert.last_notified_at_epoch_seconds",
                            "Last insert notification timestamp in epoch seconds")
            );
        }

        /**
         * Publish the latest queue and notification rows.
         */
        private void refresh(List<PgmqMetrics> queueMetrics, List<NotifyInsertThrottle> throttles) {
            queueLength.register(metricRows(queueMetrics, PgmqMetrics::queueLength), true);
            queueVisibleLength.register(metricRows(queueMetrics, PgmqMetrics::queueVisibleLength), true);
            newestMsgAgeSec.register(metricRows(queueMetrics, PgmqMetrics::newestMsgAgeSec), true);
            oldestMsgAgeSec.register(metricRows(queueMetrics, PgmqMetrics::oldestMsgAgeSec), true);
            totalMessages.register(metricRows(queueMetrics, PgmqMetrics::totalMessages), true);
            defaultPartitionLength.register(metricRows(queueMetrics, PgmqMetrics::defaultPartitionLength), true);
            notifyThrottleIntervalMs.register(throttleRows(throttles, NotifyInsertThrottle::throttleIntervalMs), true);
            notifyLastNotifiedAtEpochSeconds.register(throttleRows(throttles,
                    PgmqMetricsBinder::lastNotifiedAtEpochSecond), true);
        }

        /**
         * Create one queue-tagged multi-gauge.
         */
        private static MultiGauge multiGauge(MeterRegistry registry, String name, String description) {
            return MultiGauge.builder(name)
                    .description(description)
                    .register(registry);
        }

        /**
         * Convert queue metric rows to multi-gauge rows.
         */
        private static <T extends Number> List<MultiGauge.Row<?>> metricRows(List<PgmqMetrics> metrics,
                Function<PgmqMetrics, T> valueFunction) {
            return metrics.stream()
                    .<MultiGauge.Row<?>>map(metric -> MultiGauge.Row.of(Tags.of("queue", metric.queueName()),
                            () -> valueFunction.apply(metric)))
                    .toList();
        }

        /**
         * Convert notification throttle rows to multi-gauge rows.
         */
        private static <T extends Number> List<MultiGauge.Row<?>> throttleRows(List<NotifyInsertThrottle> throttles,
                Function<NotifyInsertThrottle, T> valueFunction) {
            return throttles.stream()
                    .<MultiGauge.Row<?>>map(throttle -> MultiGauge.Row.of(Tags.of("queue", throttle.queueName()),
                            () -> valueFunction.apply(throttle)))
                    .toList();
        }
    }
}
