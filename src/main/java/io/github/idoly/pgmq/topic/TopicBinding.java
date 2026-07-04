package io.github.idoly.pgmq.topic;

import java.time.OffsetDateTime;

/**
 * Topic binding row returned by {@code pgmq.list_topic_bindings()}.
 *
 * @param pattern the topic pattern
 * @param queueName the queue bound to the pattern
 * @param boundAt the timestamp when the binding was created
 * @param compiledRegex the compiled regular expression used by PGMQ
 */
public record TopicBinding(
        String pattern,
        String queueName,
        OffsetDateTime boundAt,
        String compiledRegex
) {}
