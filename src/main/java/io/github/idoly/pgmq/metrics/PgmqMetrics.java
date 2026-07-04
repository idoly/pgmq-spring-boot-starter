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
 */
public record PgmqMetrics(
        String queueName,
        Long queueLength,
        Integer newestMsgAgeSec,
        Integer oldestMsgAgeSec,
        Long totalMessages,
        OffsetDateTime scrapeTime,
        Long queueVisibleLength
) {}
