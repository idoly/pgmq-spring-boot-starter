package io.github.idoly.pgmq.listener.endpoint;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerFactory;

/**
 * Registry and lifecycle coordinator for PGMQ listener containers.
 *
 * <p>Tracks containers by endpoint id and starts auto-startup containers with
 * the Spring lifecycle.</p>
 */
public class PgmqListenerEndpointRegistry implements SmartLifecycle {

    /** Registered listener containers keyed by endpoint id. */
    private final Map<String, PgmqListenerContainer> listenerContainers = new LinkedHashMap<>();

    /** Registry running state used to guard lifecycle transitions. */
    private volatile boolean running;

    /**
     * Register a listener container for the endpoint.
     *
     * @param endpoint listener endpoint
     * @param factory container factory
     */
    public synchronized <E extends PgmqListenerEndpoint> void registerListenerContainer(E endpoint,
            PgmqListenerContainerFactory<E> factory) {
        Assert.notNull(endpoint, "PgmqListenerEndpoint must not be null");
        Assert.notNull(factory, "PgmqListenerContainerFactory must not be null");
        String id = endpoint.getId();
        Assert.hasText(id, "PGMQ listener id must not be empty");
        if (listenerContainers.containsKey(id)) {
            throw new PgmqException("Duplicate PGMQ listener id: " + id);
        }

        PgmqListenerContainer container = factory.createListenerContainer(endpoint);
        if (running && container.isAutoStartup()) {
            try {
                container.start();
            } catch (RuntimeException error) {
                try {
                    container.stop();
                } catch (RuntimeException stopError) {
                    if (stopError != error) {
                        error.addSuppressed(stopError);
                    }
                }
                throw error;
            }
        }
        listenerContainers.put(id, container);
    }

    /**
     * Return one listener container by id.
     *
     * @param id listener id
     * @return listener container or {@code null}
     */
    public synchronized PgmqListenerContainer getListenerContainer(String id) {
        return listenerContainers.get(id);
    }

    /**
     * Return all registered listener containers.
     *
     * @return registered containers
     */
    public synchronized Collection<PgmqListenerContainer> getListenerContainers() {
        return List.copyOf(listenerContainers.values());
    }

    /**
     * Start all auto-startup listener containers.
     */
    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        try {
            listenerContainers.values().stream()
                    .filter(PgmqListenerContainer::isAutoStartup)
                    .forEach(PgmqListenerContainer::start);
        } catch (RuntimeException error) {
            try {
                stop();
            } catch (RuntimeException stopError) {
                if (stopError != error) {
                    error.addSuppressed(stopError);
                }
            }
            throw error;
        }
    }

    /**
     * Stop all registered listener containers.
     */
    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        RuntimeException failure = null;
        for (PgmqListenerContainer container : listenerContainers.values()) {
            try {
                container.stop();
            } catch (RuntimeException error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Stop containers and notify the lifecycle callback.
     */
    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    /**
     * Return whether the registry is currently running.
     *
     * @return running state
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Always participate in Spring auto-startup.
     *
     * @return {@code true}
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Run late in the Spring lifecycle so dependent beans are ready first.
     *
     * @return lifecycle phase
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
