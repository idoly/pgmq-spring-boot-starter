package io.github.idoly.pgmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.github.idoly.pgmq.listener.AckMode;

/**
 * Declares a method-driven listener for a PGMQ queue.
 *
 * <p>The annotation processor registers a listener container for each annotated
 * method and resolves method arguments from the message payload, headers, message
 * record, or acknowledgement handle.</p>
 *
 * <p>The default factory requires synchronous {@code void} methods. Asynchronous return types
 * and {@code @Async} on listener methods or classes are rejected during registration.</p>
 *
 * <pre>{@code
 * @PgmqListener(value = "myQueue", visibilityTimeout = 60, batchSize = 5, consumerConcurrency = 2)
 * public void handle(MyMessage message) {
 *     // handle message
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PgmqListener {

    /**
     * Unique listener id used by the listener registry.
     *
     * @return listener id
     */
    String id() default "";

    /**
     * Name of the queue to listen to.
     *
     * @return queue name
     */
    String value();

    /**
     * Listener container factory bean name.
     *
     * @return container factory bean name
     */
    String containerFactory() default "";

    /**
     * Visibility timeout in seconds.
     * Uses {@code pgmq.listener.visibility-timeout} when set to {@code -1}.
     *
     * @return visibility timeout
     */
    int visibilityTimeout() default -1;

    /**
     * Number of messages to read per batch.
     * Uses {@code pgmq.listener.batch-size} when set to {@code -1}.
     *
     * @return batch size
     */
    int batchSize() default -1;

    /**
     * JSON filter for message content (experimental).
     * Only applies to {@link Mode#READ}.
     * Uses {@code pgmq.listener.conditional} when empty.
     *
     * @return JSON conditional
     */
    String conditional() default "";

    /**
     * Read mode used by the listener container.
     * Uses {@code pgmq.listener.mode} when set to {@link Mode#DEFAULT}.
     *
     * @return listener read mode
     */
    Mode mode() default Mode.DEFAULT;

    /**
     * Maximum seconds to wait for messages when polling.
     * Uses {@code pgmq.listener.max-poll-seconds} when set to {@code -1}.
     *
     * @return maximum poll seconds
     */
    int maxPollSeconds() default -1;

    /**
     * Milliseconds between poll attempts.
     * Uses {@code pgmq.listener.poll-interval-ms} when set to {@code -1}.
     *
     * @return poll interval in milliseconds
     */
    int pollIntervalMs() default -1;

    /**
     * Number of concurrent consumer threads.
     * Uses {@code pgmq.listener.consumer-concurrency} when set to {@code -1}.
     *
     * @return consumer concurrency level
     */
    int consumerConcurrency() default -1;

    /**
     * Number of concurrent database pollers.
     * Uses {@code pgmq.listener.poll-concurrency} when set to {@code -1}.
     *
     * @return poller concurrency level
     */
    int pollConcurrency() default -1;

    /**
     * Maximum number of messages that may be processing or queued for processing.
     * Uses {@code pgmq.listener.max-in-flight} when set to {@code -1}.
     *
     * @return maximum in-flight messages
     */
    int maxInFlight() default -1;

    /**
     * Maximum seconds to wait for each poller or consumer executor shutdown phase.
     * Uses {@code pgmq.listener.executor-shutdown-timeout-seconds} when set to {@code -1}.
     *
     * @return shutdown timeout in seconds
     */
    int executorShutdownTimeoutSeconds() default -1;

    /**
     * Whether this listener should start automatically.
     * Uses {@code pgmq.listener.auto-startup} when empty.
     *
     * @return auto-startup flag string
     */
    String autoStartup() default "";

    /**
     * Message acknowledgement mode.
     * Uses {@code pgmq.listener.ack-mode} when set to {@link AckMode#DEFAULT}.
     *
     * @return acknowledgement mode
     */
    AckMode ackMode() default AckMode.DEFAULT;

    /**
     * Listener read mode.
     */
    enum Mode {

        /** Use the listener mode configured in properties. */
        DEFAULT,

        /** Standard read_with_poll behavior. */
        READ,

        /** FIFO grouped read behavior. */
        FIFO_GROUPED,

        /** FIFO grouped round-robin read behavior. */
        FIFO_ROUND_ROBIN,

        /** FIFO grouped head read behavior. */
        FIFO_HEAD
    }
}
