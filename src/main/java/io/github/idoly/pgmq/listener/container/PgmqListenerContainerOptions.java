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
 * @param batchSize read batch size
 * @param conditional JSON conditional filter
 * @param mode read mode
 * @param maxPollSeconds long-poll maximum seconds
 * @param pollIntervalMs long-poll interval milliseconds
 * @param concurrency worker count
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
        int concurrency,
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
        Assert.notNull(conditional, "Conditional must not be null");
        Assert.notNull(mode, "Listener mode must not be null");
        Assert.isTrue(maxPollSeconds >= 0, "Max poll seconds must be greater than or equal to 0");
        Assert.isTrue(pollIntervalMs > 0, "Poll interval must be greater than 0");
        Assert.isTrue(concurrency > 0, "Concurrency must be greater than 0");
        Assert.notNull(ackMode, "Ack mode must not be null");
    }
}
