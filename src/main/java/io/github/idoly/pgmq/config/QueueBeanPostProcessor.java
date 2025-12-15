package io.github.idoly.pgmq.config;

import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import io.github.idoly.pgmq.core.PgmqTemplate;
import io.github.idoly.pgmq.core.Queue;

/**
 * A Spring BeanPostProcessor that initializes PGMQ queues after all singleton beans are instantiated.
 *
 * <p>This processor automatically creates queues based on {@link io.github.idoly.pgmq.core.Queue} beans in the application context.
 * It uses {@link io.github.idoly.pgmq.core.PgmqTemplate} to create normal, partitioned, or unlogged queues depending on the queue configuration.</p>
 */
public class QueueBeanPostProcessor implements SmartInitializingSingleton, ApplicationContextAware {

    /**
     * The Spring application context, used to retrieve beans.
     */
    private ApplicationContext applicationContext;

    /**
     * Default constructor for QueueBeanPostProcessor.
     *
     * <p>Required for Spring to instantiate this bean post-processor.</p>
     */
    public QueueBeanPostProcessor() {}

    /**
     * Set the {@link ApplicationContext} that this object runs in.
     *
     * @param applicationContext the application context
     * @throws BeansException if the context cannot be set
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Invoked after all singleton beans have been instantiated.
     *
     * <p>This method retrieves the {@link PgmqTemplate} bean and all {@link Queue} beans,
     * then initializes each queue according to its type:</p>
     * <ul>
     *     <li>Unlogged queues are created with {@link PgmqTemplate#createUnlogged(String)}</li>
     *     <li>Partitioned queues are created with {@link PgmqTemplate#createPartitioned(String)}</li>
     *     <li>Regular queues are created with {@link PgmqTemplate#create(String)}</li>
     * </ul>
     *
     * @throws IllegalStateException if the {@link PgmqTemplate} bean cannot be found
     */
    @Override
    public void afterSingletonsInstantiated() {
        PgmqTemplate pgmqTemplate = applicationContext.getBean(PgmqTemplate.class);
        if (pgmqTemplate == null) {
            throw new IllegalStateException("PgmqTemplate bean not found");
        }

        List<Queue> queues = applicationContext.getBeansOfType(Queue.class).values().stream().toList();
        for (Queue queue : queues) {
            if (queue.isUnlogged()) {
                pgmqTemplate.createUnlogged(queue.getQueueName());
            } else if (queue.isPartitioned()) {
                pgmqTemplate.createPartitioned(queue.getQueueName());
            } else {
                pgmqTemplate.create(queue.getQueueName());
            }
        }
    }

}
