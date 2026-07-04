package io.github.idoly.pgmq.listener.container;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.PgmqMessageListener;

/**
 * Lifecycle-managed polling container for one PGMQ queue.
 *
 * <p>Starts one or more worker threads, reads messages through
 * {@link PgmqTemplate}, dispatches them to a {@link PgmqMessageListener}, and
 * applies the configured acknowledgement mode.</p>
 */
public class PgmqListenerContainer implements SmartLifecycle {

    /** Logger used for lifecycle and polling failures. */
    private static final Log logger = LogFactory.getLog(PgmqListenerContainer.class);

    /** Maximum seconds to wait for worker threads to stop. */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    /** Container running state used to guard lifecycle transitions. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Template used for polling and acknowledgement operations. */
    private final PgmqTemplate pgmqTemplate;

    /** Callback invoked for each message. */
    private final PgmqMessageListener messageListener;

    /** Error handler invoked when message processing fails. */
    private final PgmqErrorHandler errorHandler;

    /** Resolved runtime options for this container. */
    private final PgmqListenerContainerOptions options;

    /** Worker pool owned by this container while it is running. */
    private ExecutorService executor;

    /**
     * Create a listener container from resolved runtime dependencies.
     *
     * @param pgmqTemplate template used for polling and acknowledgements
     * @param messageListener callback invoked for each message
     * @param errorHandler handler invoked when processing fails
     * @param options resolved listener options
     */
    public PgmqListenerContainer(PgmqTemplate pgmqTemplate, PgmqMessageListener messageListener,
            PgmqErrorHandler errorHandler, PgmqListenerContainerOptions options) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(messageListener, "PgmqMessageListener must not be null");
        Assert.notNull(errorHandler, "PgmqErrorHandler must not be null");
        Assert.notNull(options, "PgmqListenerContainerOptions must not be null");
        this.pgmqTemplate = pgmqTemplate;
        this.messageListener = messageListener;
        this.errorHandler = errorHandler;
        this.options = options;
    }

    /**
     * Return the listener id owned by this container.
     *
     * @return listener id
     */
    public String getId() {
        return options.id();
    }

    /**
     * Return whether this container should start automatically with Spring.
     *
     * @return auto-startup flag
     */
    public boolean isAutoStartup() {
        return options.autoStartup();
    }

    /**
     * Start worker threads and begin polling.
     */
    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("starting PgmqListenerContainer " + options.id() + " for queue " + options.queue());
        AtomicInteger threadIndex = new AtomicInteger();
        executor = Executors.newFixedThreadPool(options.concurrency(), task -> {
            Thread thread = new Thread(task);
            thread.setName("pgmq-listener-" + options.id() + "-" + threadIndex.incrementAndGet());
            return thread;
        });

        for (int i = 0; i < options.concurrency(); i++) {
            executor.submit(this::pollLoop);
        }
    }

    /**
     * Worker loop that polls and dispatches messages until the container stops.
     */
    private void pollLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                process(readMessages());
            } catch (Exception e) {
                if (running.get()) {
                    logger.error("failed to poll PGMQ listener " + options.id(), e);
                    pauseAfterFailure();
                }
            }
        }
    }

    /**
     * Read messages using the configured listener mode.
     */
    private List<PgmqMessage> readMessages() {
        return switch (options.mode()) {
            case FIFO_GROUPED -> pgmqTemplate.readGroupedWithPoll(
                    options.queue(), options.visibilityTimeout(), options.batchSize(),
                    options.maxPollSeconds(), options.pollIntervalMs());
            case FIFO_ROUND_ROBIN -> pgmqTemplate.readGroupedRoundRobinWithPoll(
                    options.queue(), options.visibilityTimeout(), options.batchSize(),
                    options.maxPollSeconds(), options.pollIntervalMs());
            case FIFO_HEAD -> pgmqTemplate.readGroupedHeadWithPoll(
                    options.queue(), options.visibilityTimeout(), options.batchSize(),
                    options.maxPollSeconds(), options.pollIntervalMs());
            case DEFAULT, READ -> pgmqTemplate.readWithPoll(
                    options.queue(), options.visibilityTimeout(), options.batchSize(),
                    options.maxPollSeconds(), options.pollIntervalMs(), options.conditional());
        };
    }

    /**
     * Dispatch a batch and collect messages that should be auto-archived.
     */
    private void process(List<PgmqMessage> records) {
        if (records.isEmpty()) {
            return;
        }

        List<Long> autoAckIds = new ArrayList<>(records.size());
        for (PgmqMessage record : records) {
            DefaultPgmqAcknowledgment acknowledgment =
                    new DefaultPgmqAcknowledgment(pgmqTemplate, options.queue(), record.msgId());
            try {
                messageListener.onMessage(record, acknowledgment);
                if (options.ackMode() == AckMode.AUTO && !acknowledgment.isCompleted()) {
                    autoAckIds.add(record.msgId());
                }
            } catch (Throwable error) {
                errorHandler.handle(error, record, acknowledgment);
            }
        }

        if (!autoAckIds.isEmpty()) {
            pgmqTemplate.archive(options.queue(), autoAckIds);
        }
    }

    /**
     * Back off briefly after polling failures to avoid a tight error loop.
     */
    private void pauseAfterFailure() {
        try {
            Thread.sleep(Math.max(100, options.pollIntervalMs()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop polling and interrupt worker threads.
     */
    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("stopping PgmqListenerContainer " + options.id() + " for queue " + options.queue());
        ExecutorService currentExecutor = executor;
        if (currentExecutor != null) {
            currentExecutor.shutdownNow();
            awaitTermination(currentExecutor);
            executor = null;
        }
    }

    /**
     * Wait briefly for interrupted worker threads to exit.
     */
    private void awaitTermination(ExecutorService currentExecutor) {
        try {
            if (!currentExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warn("timed out stopping PgmqListenerContainer " + options.id());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop the container and notify the lifecycle callback.
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
     * Return whether worker threads should keep polling.
     *
     * @return running state
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Run late in the Spring lifecycle so dependencies are initialized first.
     *
     * @return lifecycle phase
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
