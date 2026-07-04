package io.github.idoly.pgmq.listener.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerFactory;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerOptions;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqListenerEndpointRegistryTest {

    @Test
    void startsOnlyAutoStartupContainersWhenRegistryStarts() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        TrackingContainer autoContainer = new TrackingContainer(true);
        TrackingContainer manualContainer = new TrackingContainer(false);

        registry.registerListenerContainer(new TestEndpoint("auto"), new TrackingFactory(autoContainer));
        registry.registerListenerContainer(new TestEndpoint("manual"), new TrackingFactory(manualContainer));

        registry.start();
        registry.stop();

        assertEquals(1, autoContainer.starts);
        assertEquals(0, manualContainer.starts);
        assertEquals(1, autoContainer.stops);
        assertEquals(1, manualContainer.stops);
    }

    @Test
    void startsAutoStartupContainersRegisteredWhileRunning() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        TrackingContainer container = new TrackingContainer(true);

        registry.start();
        registry.registerListenerContainer(new TestEndpoint("dynamic"), new TrackingFactory(container));

        assertEquals(1, container.starts);
    }

    @Test
    void rejectsDuplicateListenerIds() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        TestEndpoint endpoint = new TestEndpoint("duplicate");
        TrackingFactory factory = new TrackingFactory(new TrackingContainer(false));

        registry.registerListenerContainer(endpoint, factory);

        assertThrows(PgmqException.class, () -> registry.registerListenerContainer(endpoint, factory));
        assertEquals(1, factory.creates);
    }

    private record TestEndpoint(String id) implements PgmqListenerEndpoint {

        @Override
        public String getId() {
            return id;
        }
    }

    private static class TrackingFactory implements PgmqListenerContainerFactory<TestEndpoint> {

        private final TrackingContainer container;

        private int creates;

        TrackingFactory(TrackingContainer container) {
            this.container = container;
        }

        @Override
        public PgmqListenerContainer createListenerContainer(TestEndpoint endpoint) {
            creates++;
            return container;
        }
    }

    private static class TrackingContainer extends PgmqListenerContainer {

        private int starts;

        private int stops;

        TrackingContainer(boolean autoStartup) {
            super(
                    new PgmqTemplate(new JdbcTemplate(), new ObjectMapper()),
                    (message, acknowledgment) -> {},
                    (error, message, acknowledgment) -> {},
                    new PgmqListenerContainerOptions(
                            "container",
                            "queue",
                            30,
                            10,
                            "{}",
                            PgmqListener.Mode.READ,
                            0,
                            1,
                            1,
                            autoStartup,
                            AckMode.AUTO
                    )
            );
        }

        @Override
        public synchronized void start() {
            starts++;
        }

        @Override
        public synchronized void stop() {
            stops++;
        }
    }
}
