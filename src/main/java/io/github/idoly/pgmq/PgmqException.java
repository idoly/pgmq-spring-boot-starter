package io.github.idoly.pgmq;

/**
 * Custom runtime exception for PGMQ-related errors.
 * <p>
 * This exception is thrown when an operation within the PGMQ framework fails,
 * such as initialization errors, message processing failures, or database issues.
 */
public class PgmqException extends RuntimeException {

    /**
     * Constructs a new PgmqException with the specified detail message.
     *
     * @param message the detail message
     */
    public PgmqException(String message) {
        super(message);
    }

    /**
     * Constructs a new PgmqException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause of the exception
     */
    public PgmqException(String message, Throwable cause) {
        super(message, cause);
    }
}
