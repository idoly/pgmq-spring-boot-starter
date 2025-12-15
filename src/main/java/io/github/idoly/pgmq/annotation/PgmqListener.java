package io.github.idoly.pgmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a listener for a PGMQ queue.
 *
 * <p>Annotated methods are invoked automatically when messages are received
 * from the configured queue.</p>
 *
 * <pre>{@code
 * @PgmqListener(queue = "myQueue", vt = 60, qty = 5, concurrency = 2)
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
     * Name of the queue to listen to.
     *
     * @return queue name
     */
    String queue();

    /**
     * Visibility timeout in seconds.
     *
     * @return visibility timeout
     */
    int vt() default 30;

    /**
     * Number of messages to read per batch.
     *
     * @return batch size
     */
    int qty() default 10;

    /**
     * JSON filter for message content (experimental).
     *
     * @return JSON filter string
     */
    String conditional() default "{}";

    /**
     * Number of concurrent threads for processing.
     *
     * @return concurrency level
     */
    int concurrency() default 1;
}
