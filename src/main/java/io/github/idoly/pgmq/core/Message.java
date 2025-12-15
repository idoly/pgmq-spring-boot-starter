package io.github.idoly.pgmq.core;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Represents a message retrieved from a PGMQ queue.
 *
 * @param msgId      the unique identifier of the message
 * @param readCt     the number of times the message has been read
 * @param enqueuedAt the timestamp when the message was enqueued
 * @param vt         the visibility timeout timestamp for the message
 * @param message    the payload of the message
 * @param headers    additional metadata associated with the message
 */
public record Message(
        Long msgId,
        Integer readCt,
        OffsetDateTime enqueuedAt,
        OffsetDateTime vt,
        Object message,
        Map<String, Object> headers
) {}
