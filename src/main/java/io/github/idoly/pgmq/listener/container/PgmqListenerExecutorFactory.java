package io.github.idoly.pgmq.listener.container;

import java.util.concurrent.ExecutorService;

/**
 * Creates the executor services owned by each listener container.
 *
 * <p>Applications may replace this bean to use custom platform or virtual thread
 * executors. Every returned executor must be new and dedicated to the requesting
 * container; the container shuts it down with its lifecycle.</p>
 */
public interface PgmqListenerExecutorFactory {

    /**
     * Create an executor for blocking database poll operations.
     *
     * @param options listener container options
     * @return container-owned poll executor
     */
    ExecutorService createPollExecutor(PgmqListenerContainerOptions options);

    /**
     * Create an executor for listener method invocations.
     *
     * @param options listener container options
     * @return container-owned consumer executor
     */
    ExecutorService createConsumerExecutor(PgmqListenerContainerOptions options);
}
