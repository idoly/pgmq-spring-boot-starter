package io.github.idoly.pgmq.message;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Message record returned by PGMQ read, pop, and visibility-timeout operations.
 *
 * @param msgId the unique identifier of the message
 * @param readCt the number of times the message has been read
 * @param enqueuedAt the timestamp when the message was enqueued
 * @param lastReadAt the timestamp when the message was last read
 * @param vt the visibility timeout timestamp for the message
 * @param payload the message payload
 * @param headers additional metadata associated with the message
 */
public record PgmqMessage(
        Long msgId,
        Integer readCt,
        OffsetDateTime enqueuedAt,
        OffsetDateTime lastReadAt,
        OffsetDateTime vt,
        Object payload,
        Map<String, Object> headers
) {}
