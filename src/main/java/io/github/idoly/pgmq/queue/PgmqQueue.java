package io.github.idoly.pgmq.queue;

import java.time.OffsetDateTime;

import org.springframework.util.Assert;

/**
 * Queue declaration and metadata for a PGMQ queue.
 *
 * <p>When registered as a Spring bean, this record is consumed by
 * {@link io.github.idoly.pgmq.config.PgmqQueueDeclarer}. It is also used
 * for rows returned by {@code pgmq.list_queues()}.</p>
 *
 * @param queueName the queue name
 * @param partitioned whether the queue is partitioned
 * @param unlogged whether the queue is unlogged
 * @param createdAt the timestamp when the queue was created
 */
public record PgmqQueue(
        String queueName,
        Boolean partitioned,
        Boolean unlogged,
        OffsetDateTime createdAt
) {

    /**
     * Validate required queue metadata.
     */
    public PgmqQueue {
        Assert.hasText(queueName, "Queue name must not be empty");
        Assert.notNull(partitioned, "Partitioned flag must not be null");
        Assert.notNull(unlogged, "Unlogged flag must not be null");
        Assert.isTrue(!partitioned || !unlogged, "Queue cannot be both partitioned and unlogged");
    }

    /**
     * Return a standard logged queue declaration.
     *
     * @param queueName the queue name
     * @return a queue definition
     */
    public static PgmqQueue create(String queueName) {
        return new PgmqQueue(queueName, false, false, null);
    }

    /**
     * Return an unlogged queue declaration.
     *
     * @param queueName the queue name
     * @return a queue definition
     */
    public static PgmqQueue createUnlogged(String queueName) {
        return new PgmqQueue(queueName, false, true, null);
    }

    /**
     * Return a partitioned queue declaration.
     *
     * @param queueName the queue name
     * @return a queue definition
     */
    public static PgmqQueue createPartitioned(String queueName) {
        return new PgmqQueue(queueName, true, false, null);
    }
}
