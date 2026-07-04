package io.github.idoly.pgmq.listener;

/**
 * Manual completion handle exposed to listener methods.
 *
 * <p>Methods are idempotent for a single message: the first completion action
 * wins and later calls are ignored by the default implementation.</p>
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
