package io.github.idoly.pgmq.listener.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerFactory;

class PgmqListenerEndpointRegistryTest {

    @Test
    void startsOnlyAutoStartupContainersWhenRegistryStarts() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        PgmqListenerContainer autoContainer = container(true);
        PgmqListenerContainer manualContainer = container(false);

        registry.registerListenerContainer(new TestEndpoint("auto"), new TrackingFactory(autoContainer));
        registry.registerListenerContainer(new TestEndpoint("manual"), new TrackingFactory(manualContainer));

        registry.start();
        registry.stop();

        verify(autoContainer).start();
        verify(manualContainer, never()).start();
        verify(manualContainer).stop();
        verify(autoContainer).stop();
    }

    @Test
    void startsAutoStartupContainersRegisteredWhileRunning() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        PgmqListenerContainer container = container(true);

        registry.start();
        registry.registerListenerContainer(new TestEndpoint("dynamic"), new TrackingFactory(container));

        verify(container).start();
    }

    @Test
    void rejectsDuplicateListenerIds() {
        PgmqListenerEndpointRegistry registry = new PgmqListenerEndpointRegistry();
        TestEndpoint endpoint = new TestEndpoint("duplicate");
        TrackingFactory factory = new TrackingFactory(container(false));

        registry.registerListenerContainer(endpoint, factory);

        assertThrows(PgmqException.class, () -> registry.registerListenerContainer(endpoint, factory));
        assertEquals(1, factory.creates);
    }

    private static PgmqListenerContainer container(boolean autoStartup) {
        PgmqListenerContainer container = mock(PgmqListenerContainer.class);
        when(container.isAutoStartup()).thenReturn(autoStartup);
        return container;
    }

    private record TestEndpoint(String id) implements PgmqListenerEndpoint {

        @Override
        public String getId() {
            return id;
        }
    }

    private static class TrackingFactory implements PgmqListenerContainerFactory<TestEndpoint> {

        private final PgmqListenerContainer container;

        private int creates;

        TrackingFactory(PgmqListenerContainer container) {
            this.container = container;
        }

        @Override
        public PgmqListenerContainer createListenerContainer(TestEndpoint endpoint) {
            creates++;
            return container;
        }
    }
}
