package io.github.idoly.pgmq.listener.container;

import org.springframework.util.Assert;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;

/**
 * Immutable runtime options resolved for a listener container.
 *
 * @param id listener id
 * @param queue queue name
 * @param visibilityTimeout visibility timeout in seconds
 * @param batchSize maximum messages requested by one poller
 * @param conditional JSON conditional filter
 * @param mode read mode
 * @param maxPollSeconds long-poll maximum seconds
 * @param pollIntervalMs long-poll interval milliseconds
 * @param consumerConcurrency consumer worker count
 * @param pollConcurrency blocking database poller count
 * @param maxInFlight maximum messages queued or executing
 * @param executorShutdownTimeoutSeconds maximum seconds for each executor shutdown phase
 * @param autoStartup whether the container starts automatically
 * @param ackMode acknowledgement mode
 */
public record PgmqListenerContainerOptions(
        String id,
        String queue,
        int visibilityTimeout,
        int batchSize,
        String conditional,
        PgmqListener.Mode mode,
        int maxPollSeconds,
        int pollIntervalMs,
        int consumerConcurrency,
        int pollConcurrency,
        int maxInFlight,
        int executorShutdownTimeoutSeconds,
        boolean autoStartup,
        AckMode ackMode
) {

    /**
     * Validate resolved options before a listener container is created.
     */
    public PgmqListenerContainerOptions {
        Assert.hasText(id, "Listener id must not be empty");
        Assert.hasText(queue, "Queue name must not be empty");
        Assert.isTrue(visibilityTimeout > 0, "Visibility timeout must be greater than 0");
        Assert.isTrue(batchSize > 0, "Batch size must be greater than 0");
        Assert.hasText(conditional, "Conditional must not be empty");
        Assert.notNull(mode, "Listener mode must not be null");
        Assert.isTrue(maxPollSeconds >= 0, "Max poll seconds must be greater than or equal to 0");
        Assert.isTrue(pollIntervalMs > 0, "Poll interval must be greater than 0");
        Assert.isTrue(consumerConcurrency > 0, "Consumer concurrency must be greater than 0");
        Assert.isTrue(pollConcurrency > 0, "Poll concurrency must be greater than 0");
        Assert.isTrue(maxInFlight >= consumerConcurrency,
                "Max in-flight must be greater than or equal to consumer concurrency");
        Assert.isTrue(maxInFlight >= pollConcurrency,
                "Max in-flight must be greater than or equal to poll concurrency");
        Assert.isTrue(executorShutdownTimeoutSeconds >= 0,
                "Executor shutdown timeout must be greater than or equal to 0");
        Assert.notNull(ackMode, "Ack mode must not be null");
    }
}
