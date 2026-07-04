package io.github.idoly.pgmq.listener.endpoint;

/**
 * Describes a listener endpoint that can be turned into a container.
 */
public interface PgmqListenerEndpoint {

    /**
     * Return the unique endpoint id.
     *
     * @return endpoint id
     */
    String getId();
}
