package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;

class DefaultPgmqListenerExecutorFactoryTest {

    @Test
    void frameworkThreadFactoryKeepsNamesAndNonDaemonWorkers() throws Exception {
        var options = new PgmqListenerContainerOptions("orders", "queue", 30, 1, "{}", PgmqListener.Mode.READ,
                1, 100, 1, 1, 1, 1, true, AckMode.AUTO);
        var factory = new DefaultPgmqListenerExecutorFactory();
        var poller = factory.createPollExecutor(options);
        var consumer = factory.createConsumerExecutor(options);
        try {
            assertEquals("pgmq-poller-orders-1", poller.submit(() -> Thread.currentThread().getName()).get(2, TimeUnit.SECONDS));
            assertEquals("pgmq-consumer-orders-1", consumer.submit(() -> Thread.currentThread().getName()).get(2, TimeUnit.SECONDS));
            assertFalse(poller.submit(() -> Thread.currentThread().isDaemon()).get(2, TimeUnit.SECONDS));
            assertFalse(consumer.submit(() -> Thread.currentThread().isDaemon()).get(2, TimeUnit.SECONDS));
        } finally {
            poller.shutdownNow();
            consumer.shutdownNow();
            assertTrue(poller.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(consumer.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
