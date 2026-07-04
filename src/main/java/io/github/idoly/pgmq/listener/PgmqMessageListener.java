package io.github.idoly.pgmq.listener;

import io.github.idoly.pgmq.message.PgmqMessage;

/**
 * Callback invoked by listener containers for each PGMQ message.
 */
@FunctionalInterface
public interface PgmqMessageListener {

    /**
     * Process a message read from a queue.
     *
     * @param message the message record
     * @param acknowledgment acknowledgement handle
     * @throws Exception if processing fails
     */
    void onMessage(PgmqMessage message, PgmqAcknowledgment acknowledgment) throws Exception;
}
