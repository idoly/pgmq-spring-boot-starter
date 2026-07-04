package io.github.idoly.pgmq.topic;

/**
 * Insert result for one message routed by {@code pgmq.send_batch_topic()}.
 *
 * @param queueName the queue that received the message
 * @param msgId the inserted message ID
 */
public record TopicBatchResult(
        String queueName,
        Long msgId
) {}
