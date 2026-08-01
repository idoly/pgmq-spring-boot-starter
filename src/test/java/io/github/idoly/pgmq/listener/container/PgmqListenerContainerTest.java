package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqListenerContainerTest {

    @Test
    void distributesInFlightCapacityAcrossPollers() throws Exception {
        PgmqTemplate template = mock(PgmqTemplate.class);
        CountDownLatch pollsStarted = new CountDownLatch(2);
        CountDownLatch releasePolls = new CountDownLatch(1);
        when(template.readWithPoll(eq("queue"), eq(30), anyInt(), eq(0), eq(10), eq("{}")))
                .thenAnswer(invocation -> {
                    pollsStarted.countDown();
                    releasePolls.await(5, TimeUnit.SECONDS);
                    return List.of();
                });

        PgmqListenerContainer container = new PgmqListenerContainer(
                template,
                (record, acknowledgment) -> {},
                (error, record, acknowledgment) -> {},
                containerOptions(1, 2, 4, 2),
                new DefaultPgmqListenerExecutorFactory(),
                new PgmqListenerMetrics());

        try {
            container.start();
            assertTrue(pollsStarted.await(2, TimeUnit.SECONDS));
            verify(template, atLeastOnce()).readWithPoll("queue", 30, 2, 0, 10, "{}");
        } finally {
            releasePolls.countDown();
            container.stop();
        }
    }

    @Test
    void boundsPollingAndAcknowledgesEachCompletedMessage() throws Exception {
        PgmqTemplate template = mock(PgmqTemplate.class);
        PgmqMessage message = new PgmqMessage(
                1L, 1, OffsetDateTime.now(), null, null, "payload", Map.of());
        AtomicInteger polls = new AtomicInteger();
        CountDownLatch processingStarted = new CountDownLatch(1);
        CountDownLatch releaseProcessing = new CountDownLatch(1);

        when(template.readWithPoll(eq("queue"), eq(30), anyInt(), eq(0), eq(10), eq("{}")))
                .thenAnswer(invocation -> {
                    if (polls.getAndIncrement() == 0) {
                        return List.of(message);
                    }
                    Thread.sleep(10);
                    return List.of();
                });

        PgmqListenerContainer container = new PgmqListenerContainer(
                template,
                (record, acknowledgment) -> {
                    processingStarted.countDown();
                    if (!releaseProcessing.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out waiting to release listener");
                    }
                },
                (error, record, acknowledgment) -> {},
                containerOptions(1, 1, 1, 2),
                new DefaultPgmqListenerExecutorFactory(),
                new PgmqListenerMetrics());

        try {
            container.start();
            assertTrue(processingStarted.await(2, TimeUnit.SECONDS));
            assertEquals(1, container.getInFlightCount());
            Thread.sleep(100);
            assertEquals(1, polls.get(), "poller must wait while max-in-flight capacity is exhausted");

            releaseProcessing.countDown();

            verify(template, timeout(2000)).archive("queue", 1L);
            verify(template, atLeastOnce()).readWithPoll("queue", 30, 1, 0, 10, "{}");
        } finally {
            releaseProcessing.countDown();
            container.stop();
        }
    }

    private PgmqListenerContainerOptions containerOptions(
            int consumerConcurrency, int pollConcurrency, int maxInFlight, int shutdownTimeoutSeconds) {
        return new PgmqListenerContainerOptions(
                "listener", "queue", 30, 10, "{}", PgmqListener.Mode.READ,
                0, 10, consumerConcurrency, pollConcurrency, maxInFlight,
                shutdownTimeoutSeconds, true, AckMode.AUTO);
    }
}
