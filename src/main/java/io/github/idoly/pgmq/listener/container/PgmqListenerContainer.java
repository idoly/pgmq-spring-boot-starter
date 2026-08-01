package io.github.idoly.pgmq.listener.container;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.PgmqMessageListener;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Lifecycle-managed polling container for one PGMQ queue.
 *
 * <p>Blocking database pollers reserve bounded in-flight capacity before
 * reading messages. Consumer threads invoke listeners separately, acknowledge
 * successful messages, and release capacity when processing finishes.</p>
 */
public class PgmqListenerContainer implements SmartLifecycle {

    private static final Log logger = LogFactory.getLog(PgmqListenerContainer.class);

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final PgmqTemplate pgmqTemplate;

    private final PgmqMessageListener messageListener;

    private final PgmqErrorHandler errorHandler;

    private final PgmqListenerContainerOptions listenerOptions;

    private final PgmqListenerExecutorFactory executorFactory;

    private final PgmqListenerMetrics listenerMetrics;

    private ExecutorService pollExecutor;

    private ExecutorService consumerExecutor;

    private Semaphore inFlightPermits;

    /**
     * Create a container with explicit execution and metric infrastructure.
     *
     * @param pgmqTemplate template used for polling and acknowledgements
     * @param messageListener callback invoked for each message
     * @param errorHandler handler invoked when processing fails
     * @param listenerOptions resolved listener options
     * @param executorFactory factory for container-owned executors
     * @param listenerMetrics listener metric recorder
     */
    public PgmqListenerContainer(PgmqTemplate pgmqTemplate, PgmqMessageListener messageListener,
            PgmqErrorHandler errorHandler, PgmqListenerContainerOptions listenerOptions,
            PgmqListenerExecutorFactory executorFactory, PgmqListenerMetrics listenerMetrics) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(messageListener, "PgmqMessageListener must not be null");
        Assert.notNull(errorHandler, "PgmqErrorHandler must not be null");
        Assert.notNull(listenerOptions, "PgmqListenerContainerOptions must not be null");
        Assert.notNull(executorFactory, "PgmqListenerExecutorFactory must not be null");
        Assert.notNull(listenerMetrics, "PgmqListenerMetrics must not be null");
        this.pgmqTemplate = pgmqTemplate;
        this.messageListener = messageListener;
        this.errorHandler = errorHandler;
        this.listenerOptions = listenerOptions;
        this.executorFactory = executorFactory;
        this.listenerMetrics = listenerMetrics;
    }

    /**
     * Return the listener id owned by this container.
     *
     * @return listener id
     */
    public String getId() {
        return listenerOptions.id();
    }

    /**
     * Return the queue polled by this container.
     *
     * @return queue name
     */
    public String getQueue() {
        return listenerOptions.queue();
    }

    /**
     * Return whether this container should start automatically with Spring.
     *
     * @return auto-startup flag
     */
    public boolean isAutoStartup() {
        return listenerOptions.autoStartup();
    }

    /**
     * Return the resolved options used by this container.
     *
     * @return listener container options
     */
    public PgmqListenerContainerOptions getOptions() {
        return listenerOptions;
    }

    /**
     * Return the current number of messages executing or waiting for execution.
     *
     * @return in-flight message count
     */
    public int getInFlightCount() {
        Semaphore current = inFlightPermits;
        return current == null ? 0 : listenerOptions.maxInFlight() - current.availablePermits();
    }

    /**
     * Start poller and consumer executors.
     */
    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("starting PgmqListenerContainer " + listenerOptions.id() + " for queue " + listenerOptions.queue());
        inFlightPermits = new Semaphore(listenerOptions.maxInFlight(), true);
        try {
            consumerExecutor = executorFactory.createConsumerExecutor(listenerOptions);
            pollExecutor = executorFactory.createPollExecutor(listenerOptions);
            Assert.state(consumerExecutor != null, "Consumer executor must not be null");
            Assert.state(pollExecutor != null, "Poll executor must not be null");
            for (int i = 0; i < listenerOptions.pollConcurrency(); i++) {
                pollExecutor.submit(this::pollLoop);
            }
        } catch (RuntimeException error) {
            running.set(false);
            shutdownNow(pollExecutor);
            shutdownNow(consumerExecutor);
            pollExecutor = null;
            consumerExecutor = null;
            throw error;
        }
    }

    private void pollLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            int reservedPermits = 0;
            long pollStartedNanos = 0;
            boolean pollMetricRecorded = false;
            try {
                reservedPermits = reserveCapacity();
                pollStartedNanos = System.nanoTime();
                List<PgmqMessage> messages = readMessages(reservedPermits);
                listenerMetrics.recordPoll(listenerOptions.id(), listenerOptions.queue(),
                        System.nanoTime() - pollStartedNanos, "success");
                pollMetricRecorded = true;
                if (messages.size() > reservedPermits) {
                    throw new IllegalStateException("PGMQ returned more messages than reserved in-flight capacity");
                }

                int unusedPermits = reservedPermits - messages.size();
                releaseCapacity(unusedPermits);
                reservedPermits -= unusedPermits;

                for (PgmqMessage message : messages) {
                    submit(message);
                    reservedPermits--;
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            } catch (Exception error) {
                if (pollStartedNanos != 0 && !pollMetricRecorded) {
                    listenerMetrics.recordPoll(listenerOptions.id(), listenerOptions.queue(),
                            System.nanoTime() - pollStartedNanos, "error");
                }
                if (running.get()) {
                    logger.error("failed to poll PGMQ listener " + listenerOptions.id(), error);
                    pauseAfterFailure();
                }
            } finally {
                releaseCapacity(reservedPermits);
            }
        }
    }

    private int reserveCapacity() throws InterruptedException {
        Semaphore current = inFlightPermits;
        current.acquire();
        int reservedPermits = 1;
        int permitsPerPoller = (listenerOptions.maxInFlight() + listenerOptions.pollConcurrency() - 1)
                / listenerOptions.pollConcurrency();
        int limit = Math.min(listenerOptions.batchSize(), permitsPerPoller);
        while (reservedPermits < limit && current.tryAcquire()) {
            reservedPermits++;
        }
        return reservedPermits;
    }

    private List<PgmqMessage> readMessages(int quantity) {
        return switch (listenerOptions.mode()) {
            case FIFO_GROUPED -> pgmqTemplate.readGroupedWithPoll(
                    listenerOptions.queue(), listenerOptions.visibilityTimeout(), quantity,
                    listenerOptions.maxPollSeconds(), listenerOptions.pollIntervalMs());
            case FIFO_ROUND_ROBIN -> pgmqTemplate.readGroupedRoundRobinWithPoll(
                    listenerOptions.queue(), listenerOptions.visibilityTimeout(), quantity,
                    listenerOptions.maxPollSeconds(), listenerOptions.pollIntervalMs());
            case FIFO_HEAD -> pgmqTemplate.readGroupedHeadWithPoll(
                    listenerOptions.queue(), listenerOptions.visibilityTimeout(), quantity,
                    listenerOptions.maxPollSeconds(), listenerOptions.pollIntervalMs());
            case DEFAULT, READ -> pgmqTemplate.readWithPoll(
                    listenerOptions.queue(), listenerOptions.visibilityTimeout(), quantity,
                    listenerOptions.maxPollSeconds(), listenerOptions.pollIntervalMs(), listenerOptions.conditional());
        };
    }

    private void submit(PgmqMessage message) {
        ExecutorService current = consumerExecutor;
        if (current == null) {
            throw new RejectedExecutionException("PGMQ consumer executor is not available");
        }
        current.execute(() -> process(message));
    }

    private void process(PgmqMessage message) {
        long processingStartedNanos = System.nanoTime();
        String metricResult = "error";
        DefaultPgmqAcknowledgment acknowledgment =
                new DefaultPgmqAcknowledgment(pgmqTemplate, listenerOptions.queue(), message.msgId());
        try {
            messageListener.onMessage(message, acknowledgment);
            if (listenerOptions.ackMode() == AckMode.AUTO && !acknowledgment.isCompleted()) {
                pgmqTemplate.archive(listenerOptions.queue(), message.msgId());
            }
            metricResult = "success";
        } catch (Throwable error) {
            rethrowIfFatal(error);
            errorHandler.handle(error, message, acknowledgment);
        } finally {
            try {
                listenerMetrics.recordProcessing(listenerOptions.id(), listenerOptions.queue(),
                        System.nanoTime() - processingStartedNanos, metricResult);
            } finally {
                releaseCapacity(1);
            }
        }
    }

    private void releaseCapacity(int permits) {
        if (permits > 0) {
            inFlightPermits.release(permits);
        }
    }

    private void rethrowIfFatal(Throwable error) {
        if (error instanceof VirtualMachineError virtualMachineError) {
            throw virtualMachineError;
        }
        if (error instanceof ThreadDeath threadDeath) {
            throw threadDeath;
        }
        if (error instanceof LinkageError linkageError) {
            throw linkageError;
        }
    }

    private void pauseAfterFailure() {
        try {
            Thread.sleep(Math.max(100, listenerOptions.pollIntervalMs()));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop polling, then allow accepted consumer tasks to drain.
     */
    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("stopping PgmqListenerContainer " + listenerOptions.id() + " for queue " + listenerOptions.queue());
        ExecutorService currentPollExecutor = pollExecutor;
        ExecutorService currentConsumerExecutor = consumerExecutor;

        shutdownNow(currentPollExecutor);
        awaitTermination(currentPollExecutor, "pollers");

        if (currentConsumerExecutor != null) {
            currentConsumerExecutor.shutdown();
            if (!awaitTermination(currentConsumerExecutor, "consumers")) {
                currentConsumerExecutor.shutdownNow();
            }
        }

        pollExecutor = null;
        consumerExecutor = null;
        inFlightPermits = null;
    }

    private void shutdownNow(ExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private boolean awaitTermination(ExecutorService executor, String executorName) {
        if (executor == null) {
            return true;
        }
        try {
            boolean terminated = executor.awaitTermination(
                    listenerOptions.executorShutdownTimeoutSeconds(), TimeUnit.SECONDS);
            if (!terminated) {
                logger.warn("timed out stopping PGMQ listener " + listenerOptions.id() + " " + executorName);
            }
            return terminated;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            return false;
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
