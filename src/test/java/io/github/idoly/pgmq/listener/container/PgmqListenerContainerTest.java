package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateTaskCompletionIsIsolatedFromStopAndRestart(boolean restart) throws Exception {
        PgmqTemplate template = mock(PgmqTemplate.class);
        CountDownLatch oldStarted = new CountDownLatch(1);
        CountDownLatch newStarted = new CountDownLatch(1);
        CountDownLatch releaseOld = new CountDownLatch(1);
        CountDownLatch releaseNew = new CountDownLatch(1);
        CountDownLatch unexpectedPoll = new CountDownLatch(1);
        AtomicInteger polls = new AtomicInteger();
        var executors = new ArrayList<RecordingExecutor>();
        when(template.readWithPoll(eq("queue"), eq(30), anyInt(), eq(0), eq(10), eq("{}")))
                .thenAnswer(invocation -> {
                    int poll = polls.incrementAndGet();
                    if (poll > 2) {
                        unexpectedPoll.countDown();
                        Thread.sleep(10);
                        return List.of();
                    }
                    return List.of(new PgmqMessage((long) poll, 1, OffsetDateTime.now(),
                            null, null, "payload", Map.of()));
                });
        var factory = new DefaultPgmqListenerExecutorFactory() {
            @Override
            public ExecutorService createConsumerExecutor(PgmqListenerContainerOptions options) {
                var executor = new RecordingExecutor();
                executors.add(executor);
                return executor;
            }
        };
        var container = new PgmqListenerContainer(template, (record, acknowledgment) -> {
            if (record.msgId() == 1L) {
                oldStarted.countDown();
                awaitIgnoringInterrupts(releaseOld);
            } else {
                newStarted.countDown();
                awaitIgnoringInterrupts(releaseNew);
            }
        }, (error, record, acknowledgment) -> {}, containerOptions(1, 1, 1, 0), factory,
                new PgmqListenerMetrics());
        try {
            container.start();
            assertTrue(oldStarted.await(2, TimeUnit.SECONDS));
            container.stop();
            if (restart) {
                container.start();
                assertTrue(newStarted.await(2, TimeUnit.SECONDS));
            }
            releaseOld.countDown();
            assertTrue(executors.get(0).finished.await(2, TimeUnit.SECONDS));
            assertNull(executors.get(0).failure);
            assertFalse(unexpectedPoll.await(100, TimeUnit.MILLISECONDS),
                    "old tasks must not release permits into the restarted container");
            assertEquals(restart ? 1 : 0, container.getInFlightCount());
        } finally {
            releaseOld.countDown();
            releaseNew.countDown();
            container.stop();
            for (var executor : executors) {
                assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void stoppedLongPollCannotDispatchIntoRestartedContainer() throws Exception {
        PgmqTemplate template = mock(PgmqTemplate.class);
        CountDownLatch oldPollStarted = new CountDownLatch(1);
        CountDownLatch newPollStarted = new CountDownLatch(1);
        CountDownLatch releaseOldPoll = new CountDownLatch(1);
        CountDownLatch releaseNewPoll = new CountDownLatch(1);
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger processed = new AtomicInteger();
        var executors = new ArrayList<RecordingExecutor>();
        when(template.readWithPoll(eq("queue"), eq(30), anyInt(), eq(0), eq(10), eq("{}")))
                .thenAnswer(invocation -> {
                    if (polls.incrementAndGet() == 1) {
                        oldPollStarted.countDown();
                        awaitIgnoringInterrupts(releaseOldPoll);
                        return List.of(new PgmqMessage(1L, 1, OffsetDateTime.now(),
                                null, null, "payload", Map.of()));
                    }
                    newPollStarted.countDown();
                    releaseNewPoll.await();
                    return List.of();
                });
        var factory = new DefaultPgmqListenerExecutorFactory() {
            @Override
            public ExecutorService createPollExecutor(PgmqListenerContainerOptions options) {
                var executor = new RecordingExecutor();
                executors.add(executor);
                return executor;
            }
        };
        var container = new PgmqListenerContainer(template,
                (record, acknowledgment) -> processed.incrementAndGet(),
                (error, record, acknowledgment) -> {}, containerOptions(1, 1, 1, 0), factory,
                new PgmqListenerMetrics());
        try {
            container.start();
            assertTrue(oldPollStarted.await(2, TimeUnit.SECONDS));
            container.stop();
            container.start();
            assertTrue(newPollStarted.await(2, TimeUnit.SECONDS));
            releaseOldPoll.countDown();
            assertTrue(executors.get(0).finished.await(2, TimeUnit.SECONDS));
            assertEquals(0, processed.get());
        } finally {
            releaseOldPoll.countDown();
            container.stop();
            releaseNewPoll.countDown();
            for (var executor : executors) {
                assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            }
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class RecordingExecutor extends ThreadPoolExecutor {
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile Throwable failure;

        RecordingExecutor() {
            super(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        protected void afterExecute(Runnable task, Throwable error) {
            failure = error;
            finished.countDown();
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
