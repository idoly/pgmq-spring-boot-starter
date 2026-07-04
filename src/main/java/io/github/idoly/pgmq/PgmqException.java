package io.github.idoly.pgmq;

/**
 * Runtime exception used by the PGMQ starter.
 *
 * <p>Thrown for initialization, SQL access, serialization, and listener
 * processing failures that should be surfaced through the starter API.</p>
 */
public class PgmqException extends RuntimeException {

    /**
     * Create an exception with a message.
     *
     * @param message the detail message
     */
    public PgmqException(String message) {
        super(message);
    }

    /**
     * Create an exception with a message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public PgmqException(String message, Throwable cause) {
        super(message, cause);
    }
}
