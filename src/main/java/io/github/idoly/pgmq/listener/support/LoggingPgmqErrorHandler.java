package io.github.idoly.pgmq.listener.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.listener.PgmqAcknowledgment;
import io.github.idoly.pgmq.listener.PgmqErrorHandler;

/**
 * Default listener error handler.
 *
 * <p>Logs the failure and leaves the message uncompleted so PGMQ can make it
 * visible again after its visibility timeout.</p>
 */
public class LoggingPgmqErrorHandler implements PgmqErrorHandler {

    /** Logger used for listener processing failures. */
    private static final Log logger = LogFactory.getLog(LoggingPgmqErrorHandler.class);

    /**
     * Log the listener failure without completing the message.
     */
    @Override
    public void handle(Throwable error, PgmqMessage message, PgmqAcknowledgment acknowledgment) {
        logger.error("failed to process PGMQ message [" + message.msgId() + "]", error);
    }
}
