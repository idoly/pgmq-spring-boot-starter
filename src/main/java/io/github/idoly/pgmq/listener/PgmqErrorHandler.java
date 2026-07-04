package io.github.idoly.pgmq.listener;

import io.github.idoly.pgmq.message.PgmqMessage;

/**
 * Strategy interface for listener processing failures.
 *
 * <p>Implementations can log, route, requeue, delete, or acknowledge the failed
 * message through the supplied acknowledgement handle.</p>
 */
@FunctionalInterface
public interface PgmqErrorHandler {

    /**
     * Handle a failed listener invocation.
     *
     * @param error the processing error
     * @param message the message record
     * @param acknowledgment acknowledgement handle
     */
    void handle(Throwable error, PgmqMessage message, PgmqAcknowledgment acknowledgment);
}
