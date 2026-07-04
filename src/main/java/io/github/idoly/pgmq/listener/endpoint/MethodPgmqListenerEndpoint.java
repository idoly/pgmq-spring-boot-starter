package io.github.idoly.pgmq.listener.endpoint;

import java.lang.reflect.Method;

import org.springframework.util.Assert;

import io.github.idoly.pgmq.annotation.PgmqListener;

/**
 * Listener endpoint backed by a bean method annotated with {@link PgmqListener}.
 *
 * <p>Stores the resolved listener id, queue name, bean instance, method, and
 * annotation metadata used by the container factory.</p>
 */
public class MethodPgmqListenerEndpoint implements PgmqListenerEndpoint {

    /** Unique listener id. */
    private final String id;

    /** Bean instance that owns the listener method. */
    private final Object bean;

    /** Invocable listener method. */
    private final Method method;

    /** Spring bean name of the listener bean. */
    private final String beanName;

    /** Queue consumed by this endpoint. */
    private final String queue;

    /** Source annotation for this endpoint. */
    private final PgmqListener annotation;

    /**
     * Create an endpoint from resolved annotation metadata.
     *
     * @param id unique listener id
     * @param bean listener bean
     * @param method invocable listener method
     * @param beanName Spring bean name
     * @param queue queue consumed by the listener
     * @param annotation source annotation
     */
    public MethodPgmqListenerEndpoint(String id, Object bean, Method method, String beanName, String queue,
            PgmqListener annotation) {
        Assert.hasText(id, "Listener id must not be empty");
        Assert.notNull(bean, "Listener bean must not be null");
        Assert.notNull(method, "Listener method must not be null");
        Assert.hasText(beanName, "Listener bean name must not be empty");
        Assert.hasText(queue, "Queue name must not be empty");
        Assert.notNull(annotation, "PgmqListener annotation must not be null");
        this.id = id;
        this.bean = bean;
        this.method = method;
        this.beanName = beanName;
        this.queue = queue;
        this.annotation = annotation;
    }

    /**
     * Return the unique listener id.
     *
     * @return listener id
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Return the listener bean.
     *
     * @return listener bean
     */
    public Object getBean() {
        return bean;
    }

    /**
     * Return the invocable listener method.
     *
     * @return listener method
     */
    public Method getMethod() {
        return method;
    }

    /**
     * Return the Spring bean name for the listener.
     *
     * @return bean name
     */
    public String getBeanName() {
        return beanName;
    }

    /**
     * Return the queue consumed by this endpoint.
     *
     * @return queue name
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Return the source listener annotation.
     *
     * @return listener annotation
     */
    public PgmqListener getAnnotation() {
        return annotation;
    }
}
