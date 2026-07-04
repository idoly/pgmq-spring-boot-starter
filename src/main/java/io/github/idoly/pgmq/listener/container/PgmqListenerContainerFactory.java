package io.github.idoly.pgmq.listener.container;

import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpoint;

/**
 * Factory for listener containers created from listener endpoints.
 *
 * @param <E> endpoint type
 */
public interface PgmqListenerContainerFactory<E extends PgmqListenerEndpoint> {

    /**
     * Create a listener container for the endpoint.
     *
     * @param endpoint listener endpoint
     * @return listener container
     */
    PgmqListenerContainer createListenerContainer(E endpoint);
}
