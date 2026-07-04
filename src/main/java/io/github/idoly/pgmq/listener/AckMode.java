package io.github.idoly.pgmq.listener;

/**
 * Acknowledgement strategy used by listener containers after message handling.
 */
public enum AckMode {

    /** Resolve the mode from listener properties or the container factory. */
    DEFAULT,

    /** Archive a message after the listener method returns successfully. */
    AUTO,

    /** Require the listener to explicitly acknowledge, delete, or requeue. */
    MANUAL,

    /** Leave messages untouched after listener execution. */
    NONE
}
