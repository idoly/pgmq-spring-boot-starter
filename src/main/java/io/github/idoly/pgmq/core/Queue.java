package io.github.idoly.pgmq.core;

import java.time.OffsetDateTime;

/**
 * Represents a queue in PGMQ with optional partitioning and logging configurations.
 */
public class Queue {

    /** The name of the queue */
    private String queueName;

    /** Indicates if the queue is partitioned */
    private Boolean isPartitioned;

    /** Indicates if the queue is unlogged (not persisted) */
    private Boolean isUnlogged;

    /** The timestamp when the queue was created */
    private OffsetDateTime createdAt;

    /**
     * Constructs a Queue instance with specified properties.
     *
     * @param queueName     the name of the queue
     * @param isPartitioned true if the queue is partitioned
     * @param isUnlogged    true if the queue is unlogged
     * @param createdAt     the creation timestamp
     */
    protected Queue(String queueName, Boolean isPartitioned, Boolean isUnlogged, OffsetDateTime createdAt) {
        this.queueName = queueName;
        this.isPartitioned = isPartitioned;
        this.isUnlogged = isUnlogged;
        this.createdAt = createdAt;
    }

    /**
     * Creates a standard queue (not partitioned, logged).
     *
     * @param queueName the name of the queue
     * @return a new Queue instance
     */
    public static Queue create(String queueName) {
        return new Queue(queueName, false, false, null);
    }

    /**
     * Creates an unlogged queue (not persisted).
     *
     * @param queueName the name of the queue
     * @return a new Queue instance
     */
    public static Queue createUnlogged(String queueName) {
        return new Queue(queueName, false, true, null);
    }

    /**
     * Creates a partitioned queue.
     *
     * @param queueName the name of the queue
     * @return a new Queue instance
     */
    public static Queue createPartitioned(String queueName) {
        return new Queue(queueName, true, false, null);
    }

    /**
     * Returns the name of the queue.
     *
     * @return the queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Sets the name of the queue.
     *
     * @param queueName the queue name to set
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * Returns whether the queue is partitioned.
     *
     * @return true if the queue is partitioned, false otherwise
     */
    public Boolean isPartitioned() {
        return isPartitioned;
    }

    /**
     * Sets whether the queue is partitioned.
     *
     * @param isPartitioned true to mark the queue as partitioned
     */
    public void setPartitioned(Boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }

    /**
     * Returns whether the queue is unlogged (not persisted).
     *
     * @return true if the queue is unlogged, false otherwise
     */
    public Boolean isUnlogged() {
        return isUnlogged;
    }

    /**
     * Sets whether the queue is unlogged (not persisted).
     *
     * @param isUnlogged true to mark the queue as unlogged
     */
    public void setUnlogged(Boolean isUnlogged) {
        this.isUnlogged = isUnlogged;
    }

    /**
     * Returns the timestamp when the queue was created.
     *
     * @return the creation timestamp
     */
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the timestamp when the queue was created.
     *
     * @param createdAt the creation timestamp to set
     */
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "Queue [queueName=" + queueName
            + ", isPartitioned=" + isPartitioned 
            + ", isUnlogged=" + isUnlogged
            + ", createdAt=" + createdAt + "]";
    }
}
