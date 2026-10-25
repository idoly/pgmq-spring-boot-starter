package io.github.idoly.pgmq.listener.container;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * Default platform-thread executor factory for listener containers.
 */
public class DefaultPgmqListenerExecutorFactory implements PgmqListenerExecutorFactory {

    /**
     * Create the default platform-thread executor factory.
     */
    public DefaultPgmqListenerExecutorFactory() {
    }

    /**
     * Create a fixed-size poller pool.
     */
    @Override
    public ExecutorService createPollExecutor(PgmqListenerContainerOptions options) {
        return Executors.newFixedThreadPool(
                options.pollConcurrency(),
                new CustomizableThreadFactory("pgmq-poller-" + options.id() + "-"));
    }

    /**
     * Create a fixed-size consumer pool with a bounded work queue.
     */
    @Override
    public ExecutorService createConsumerExecutor(PgmqListenerContainerOptions options) {
        return new ThreadPoolExecutor(
                options.consumerConcurrency(),
                options.consumerConcurrency(),
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(options.maxInFlight()),
                new CustomizableThreadFactory("pgmq-consumer-" + options.id() + "-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

}
