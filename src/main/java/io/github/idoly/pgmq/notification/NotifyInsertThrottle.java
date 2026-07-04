package io.github.idoly.pgmq.notification;

import java.time.OffsetDateTime;

/**
 * Notification throttle row returned by {@code pgmq.list_notify_insert_throttles()}.
 *
 * @param queueName the queue name
 * @param throttleIntervalMs the minimum milliseconds between notifications
 * @param lastNotifiedAt the last notification timestamp
 */
public record NotifyInsertThrottle(
        String queueName,
        Integer throttleIntervalMs,
        OffsetDateTime lastNotifiedAt
) {}
