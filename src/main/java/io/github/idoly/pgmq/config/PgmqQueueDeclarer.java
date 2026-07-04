package io.github.idoly.pgmq.config;

import java.util.List;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Declares queues represented by {@link PgmqQueue} beans.
 *
 * <p>Runs after singleton initialization and before listener containers start.
 * When auto-initialization is enabled, it initializes PGMQ before creating the
 * declared queues.</p>
 */
public class PgmqQueueDeclarer implements SmartInitializingSingleton {

    /** Template used to create declared queues. */
    private final PgmqTemplate pgmqTemplate;

    /** Initializer used when auto-initialization is enabled. */
    private final PgmqInitializer pgmqInitializer;

    /** Starter properties controlling auto-initialization. */
    private final PgmqProperties properties;

    /** Queue declarations provided by the application context. */
    private final ObjectProvider<PgmqQueue> queues;

    /**
     * Create a queue declarer backed by the template and queue declarations.
     *
     * @param pgmqTemplate template used to create queues
     * @param pgmqInitializer initializer used before queue declaration
     * @param properties starter properties
     * @param queues queue declarations to materialize
     */
    public PgmqQueueDeclarer(PgmqTemplate pgmqTemplate, PgmqInitializer pgmqInitializer,
            PgmqProperties properties, ObjectProvider<PgmqQueue> queues) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(pgmqInitializer, "PgmqInitializer must not be null");
        Assert.notNull(properties, "PgmqProperties must not be null");
        Assert.notNull(queues, "PgmqQueue provider must not be null");
        this.pgmqTemplate = pgmqTemplate;
        this.pgmqInitializer = pgmqInitializer;
        this.properties = properties;
        this.queues = queues;
    }

    /**
     * Create all declared queues after singleton initialization.
     */
    @Override
    public void afterSingletonsInstantiated() {
        if (properties.isAutoInit()) {
            pgmqInitializer.init();
        }

        List<PgmqQueue> queueDeclarations = queues.orderedStream().toList();
        for (PgmqQueue queue : queueDeclarations) {
            if (Boolean.TRUE.equals(queue.unlogged())) {
                pgmqTemplate.createUnlogged(queue.queueName());
            } else if (Boolean.TRUE.equals(queue.partitioned())) {
                pgmqTemplate.createPartitioned(queue.queueName());
            } else {
                pgmqTemplate.create(queue.queueName());
            }
        }
    }

}
