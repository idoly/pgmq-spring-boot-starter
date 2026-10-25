package io.github.idoly.pgmq.listener;

/**
 * Manual completion handle exposed to listener methods.
 *
 * <p>Methods are idempotent for a single message: the first successfully returned completion
 * action wins and later calls are ignored by the default implementation. Concurrent completion
 * calls are serialized. A database exception leaves the handle incomplete so callers can retry.
 * A failed connection may leave the database outcome unknown; retrying does not provide
 * exactly-once processing.</p>
 */
public interface PgmqAcknowledgment {

    /**
     * Archive the message.
     */
    void acknowledge();

    /**
     * Delete the message without archiving it.
     */
    void delete();

    /**
     * Make the message visible again after the given visibility timeout.
     *
     * @param visibilityTimeoutSeconds visibility timeout in seconds
     */
    void requeueAfter(int visibilityTimeoutSeconds);

    /**
     * Return whether this handle has completed the message.
     *
     * @return {@code true} if the message has been acknowledged, deleted, or requeued
     */
    boolean isCompleted();
}
