package io.github.idoly.pgmq.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.github.idoly.pgmq.annotation.PgmqHeader;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.annotation.PgmqPayload;
import io.github.idoly.pgmq.config.PgmqInitializer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.template.PgmqTemplate;
import io.github.idoly.pgmq.testsupport.PostgresIntegrationSupport;

@SpringBootTest(
        classes = PgmqListenerIntegrationTest.TestApplication.class,
        properties = {
                "pgmq.listener.auto-startup=false",
                "pgmq.listener.max-poll-seconds=1",
                "pgmq.listener.poll-interval-ms=50"
        })
@EnabledIfEnvironmentVariable(named = "PGMQ_TEST_JDBC_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgmqListenerIntegrationTest extends PostgresIntegrationSupport {

    private static final String LISTENER_ID = "listener-e2e";

    private static final String LISTENER_QUEUE = "listener_e2e_queue";

    private static final String MANUAL_LISTENER_ID = "listener-manual-e2e";

    private static final String MANUAL_LISTENER_QUEUE = "listener_manual_e2e_queue";

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        ListenerProbe listenerProbe() {
            return new ListenerProbe();
        }
    }

    @Autowired
    private PgmqInitializer pgmqInitializer;

    @Autowired
    private PgmqTemplate pgmqTemplate;

    @Autowired
    private PgmqListenerEndpointRegistry registry;

    @Autowired
    private ListenerProbe listenerProbe;

    @BeforeAll
    void initializePgmq() {
        pgmqInitializer.init();
    }

    @BeforeEach
    void createQueue() {
        listenerProbe.reset();
        pgmqTemplate.dropQueue(LISTENER_QUEUE);
        pgmqTemplate.dropQueue(MANUAL_LISTENER_QUEUE);
        pgmqTemplate.createUnlogged(LISTENER_QUEUE);
        pgmqTemplate.createUnlogged(MANUAL_LISTENER_QUEUE);
    }

    @AfterEach
    void stopListenerAndDropQueue() {
        stopContainer(LISTENER_ID);
        stopContainer(MANUAL_LISTENER_ID);
        pgmqTemplate.dropQueue(LISTENER_QUEUE);
        pgmqTemplate.dropQueue(MANUAL_LISTENER_QUEUE);
    }

    @Test
    void annotationDrivenListenerConsumesPayloadHeadersAndAutoArchives() throws InterruptedException {
        PgmqListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        assertNotNull(container);
        assertFalse(container.isAutoStartup());

        ListenerEvent event = new ListenerEvent(7, "created");
        pgmqTemplate.send(LISTENER_QUEUE, event, Map.of("source", "test"));

        container.start();

        assertTrue(listenerProbe.awaitMessage());
        assertEquals(event, listenerProbe.payload());
        assertEquals("test", listenerProbe.source());
        awaitQueueLength(LISTENER_QUEUE, 0);
    }

    @Test
    void manualAcknowledgementArchivesMessage() throws InterruptedException {
        PgmqListenerContainer container = registry.getListenerContainer(MANUAL_LISTENER_ID);
        assertNotNull(container);
        assertFalse(container.isAutoStartup());

        ListenerEvent event = new ListenerEvent(8, "manual");
        pgmqTemplate.send(MANUAL_LISTENER_QUEUE, event);

        container.start();

        assertTrue(listenerProbe.awaitManualMessage());
        assertEquals(event, listenerProbe.manualPayload());
        awaitQueueLength(MANUAL_LISTENER_QUEUE, 0);
    }

    private void awaitQueueLength(String queue, long expectedLength) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (pgmqTemplate.getMetrics(queue).queueLength() == expectedLength) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for queue " + queue + " length " + expectedLength);
    }

    private void stopContainer(String listenerId) {
        PgmqListenerContainer container = registry.getListenerContainer(listenerId);
        if (container != null) {
            container.stop();
        }
    }

    public record ListenerEvent(int id, String event) {}

    static class ListenerProbe {

        private final AtomicReference<ListenerEvent> payload = new AtomicReference<>();

        private final AtomicReference<String> source = new AtomicReference<>();

        private final AtomicReference<ListenerEvent> manualPayload = new AtomicReference<>();

        private volatile CountDownLatch latch = new CountDownLatch(1);

        private volatile CountDownLatch manualLatch = new CountDownLatch(1);

        @PgmqListener(id = LISTENER_ID, value = LISTENER_QUEUE)
        void handle(@PgmqPayload ListenerEvent payload, @PgmqHeader("source") String source) {
            this.payload.set(payload);
            this.source.set(source);
            latch.countDown();
        }

        @PgmqListener(id = MANUAL_LISTENER_ID, value = MANUAL_LISTENER_QUEUE, ackMode = AckMode.MANUAL)
        void handleManual(@PgmqPayload ListenerEvent payload, PgmqAcknowledgment acknowledgment) {
            this.manualPayload.set(payload);
            acknowledgment.acknowledge();
            manualLatch.countDown();
        }

        void reset() {
            payload.set(null);
            source.set(null);
            manualPayload.set(null);
            latch = new CountDownLatch(1);
            manualLatch = new CountDownLatch(1);
        }

        boolean awaitMessage() throws InterruptedException {
            return latch.await(5, TimeUnit.SECONDS);
        }

        boolean awaitManualMessage() throws InterruptedException {
            return manualLatch.await(5, TimeUnit.SECONDS);
        }

        ListenerEvent payload() {
            return payload.get();
        }

        String source() {
            return source.get();
        }

        ListenerEvent manualPayload() {
            return manualPayload.get();
        }
    }
}
