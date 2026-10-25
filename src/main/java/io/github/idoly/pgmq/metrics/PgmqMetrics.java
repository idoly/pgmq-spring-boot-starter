package io.github.idoly.pgmq.metrics;

import java.time.OffsetDateTime;

/**
 * Metrics row returned by {@code pgmq.metrics()} and {@code pgmq.metrics_all()}.
 *
 * <p>Message age fields are nullable when a queue has no messages.</p>
 *
 * @param queueName the name of the queue
 * @param queueLength the total number of messages currently in the queue
 * @param newestMsgAgeSec the age in seconds of the newest message in the queue
 * @param oldestMsgAgeSec the age in seconds of the oldest message in the queue
 * @param totalMessages the total number of messages ever enqueued in the queue
 * @param scrapeTime the timestamp when these metrics were collected
 * @param queueVisibleLength the number of messages currently visible in the queue
 * @param defaultPartitionLength estimated messages in queue and archive default partitions,
 *                               or {@code null} for a non-partitioned queue
 */
public record PgmqMetrics(
        String queueName,
        Long queueLength,
        Integer newestMsgAgeSec,
        Integer oldestMsgAgeSec,
        Long totalMessages,
        OffsetDateTime scrapeTime,
        Long queueVisibleLength,
        Long defaultPartitionLength
) {
    /**
     * Construct metrics without a default-partition estimate.
     *
     * @param queueName the queue name
     * @param queueLength current queue length
     * @param newestMsgAgeSec newest message age in seconds
     * @param oldestMsgAgeSec oldest message age in seconds
     * @param totalMessages total messages ever enqueued
     * @param scrapeTime collection timestamp
     * @param queueVisibleLength visible queue length
     */
    public PgmqMetrics(String queueName, Long queueLength, Integer newestMsgAgeSec,
                       Integer oldestMsgAgeSec, Long totalMessages, OffsetDateTime scrapeTime,
                       Long queueVisibleLength) {
        this(queueName, queueLength, newestMsgAgeSec, oldestMsgAgeSec, totalMessages,
                scrapeTime, queueVisibleLength, null);
    }
}
