package io.github.idoly.pgmq.topic;

/**
 * Topic route matched by {@code pgmq.test_routing()}.
 *
 * @param pattern the matching topic pattern
 * @param queueName the queue that would receive the message
 * @param compiledRegex the compiled regular expression used by PGMQ
 */
public record TopicRoute(
        String pattern,
        String queueName,
        String compiledRegex
) {}
