package io.github.idoly.pgmq.listener.container;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.util.Assert;

import io.github.idoly.pgmq.listener.PgmqAcknowledgment;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Default idempotent acknowledgement handle for one queue message.
 */
final class DefaultPgmqAcknowledgment implements PgmqAcknowledgment {

    /** Template used to complete the message in PGMQ. */
    private final PgmqTemplate pgmqTemplate;

    /** Queue containing the message. */
    private final String queue;

    /** Message id being acknowledged. */
    private final long msgId;

    /** Guards the handle so only one completion action is applied. */
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Create an acknowledgement handle for one queue message.
     *
     * @param pgmqTemplate template used to complete the message
     * @param queue queue containing the message
     * @param msgId message id
     */
    DefaultPgmqAcknowledgment(PgmqTemplate pgmqTemplate, String queue, long msgId) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.hasText(queue, "Queue name must not be empty");
        Assert.isTrue(msgId > 0, "Message id must be greater than 0");
        this.pgmqTemplate = pgmqTemplate;
        this.queue = queue;
        this.msgId = msgId;
    }

    /**
     * Archive the message if no completion action has already run.
     */
    @Override
    public void acknowledge() {
        if (completed.compareAndSet(false, true)) {
            pgmqTemplate.archive(queue, msgId);
        }
    }

    /**
     * Delete the message if no completion action has already run.
     */
    @Override
    public void delete() {
        if (completed.compareAndSet(false, true)) {
            pgmqTemplate.delete(queue, msgId);
        }
    }

    /**
     * Requeue the message after the given visibility timeout if no completion action has already run.
     */
    @Override
    public void requeueAfter(int visibilityTimeoutSeconds) {
        if (completed.compareAndSet(false, true)) {
            pgmqTemplate.setVt(queue, msgId, visibilityTimeoutSeconds);
        }
    }

    /**
     * Return whether this handle already completed the message.
     */
    @Override
    public boolean isCompleted() {
        return completed.get();
    }
}
