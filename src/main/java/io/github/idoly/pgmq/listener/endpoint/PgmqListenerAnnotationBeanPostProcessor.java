package io.github.idoly.pgmq.listener.endpoint;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerFactory;

/**
 * Detects {@link PgmqListener} methods and registers listener endpoints.
 *
 * <p>Collects annotated methods during singleton initialization, then registers later-created
 * listener beans immediately through the same registry.</p>
 */
public class PgmqListenerAnnotationBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton,
        ApplicationContextAware {

    /** Default bean name for the annotation-driven listener container factory. */
    public static final String DEFAULT_CONTAINER_FACTORY_BEAN_NAME = "pgmqListenerContainerFactory";

    /** Listener method metadata collected during bean post-processing. */
    private final List<Metadata> metadatas = new ArrayList<>();

    /** Registration phase and default-id sequence, guarded by the metadata list monitor. */
    private boolean initialized;
    private int nextEndpointIndex;

    /** Application context used to resolve factories, registry, and placeholders. */
    private ApplicationContext applicationContext;

    /**
     * Store the application context for later endpoint registration.
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Assert.notNull(applicationContext, "ApplicationContext must not be null");
        this.applicationContext = applicationContext;
    }

    /**
     * Collect methods annotated with {@link PgmqListener} after each bean is initialized.
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        Map<Method, PgmqListener> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (Method method) -> AnnotatedElementUtils.findMergedAnnotation(method, PgmqListener.class));

        if (!annotatedMethods.isEmpty()) {
            Assert.state(applicationContext.isSingleton(beanName), "PGMQ listener beans must be singletons");
        }
        annotatedMethods.forEach((method, annotation) -> {
            Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
            ReflectionUtils.makeAccessible(invocableMethod);
            Metadata metadata;
            boolean registerNow;
            synchronized (metadatas) {
                metadata = new Metadata(beanName, bean, invocableMethod, annotation, nextEndpointIndex++);
                registerNow = initialized;
                if (!registerNow) {
                    metadatas.add(metadata);
                }
            }
            if (registerNow) {
                registerEndpoint(metadata);
            }
        });

        return bean;
    }

    /**
     * Register listener containers after all singleton beans are available.
     */
    @Override
    public void afterSingletonsInstantiated() {
        List<Metadata> pending;
        synchronized (metadatas) {
            initialized = true;
            pending = List.copyOf(metadatas);
            metadatas.clear();
        }
        pending.forEach(this::registerEndpoint);
    }

    private void registerEndpoint(Metadata metadata) {
        PgmqListenerEndpointRegistry registry = applicationContext.getBean(PgmqListenerEndpointRegistry.class);
        registry.registerListenerContainer(createEndpoint(metadata), resolveFactory(metadata.annotation()));
    }

    /**
     * Create an endpoint from collected listener metadata.
     */
    private MethodPgmqListenerEndpoint createEndpoint(Metadata metadata) {
        PgmqListener annotation = metadata.annotation();
        String queue = resolveRequired(annotation.value(), "@PgmqListener value must not be empty");
        String id = resolveId(annotation.id(), metadata);

        return new MethodPgmqListenerEndpoint(id, metadata.bean(), metadata.method(), metadata.beanName(), queue, annotation);
    }

    /**
     * Resolve the container factory configured on the listener annotation.
     */
    @SuppressWarnings("unchecked")
    private PgmqListenerContainerFactory<MethodPgmqListenerEndpoint> resolveFactory(PgmqListener annotation) {
        String beanName = applicationContext.getEnvironment().resolvePlaceholders(annotation.containerFactory());
        if (!StringUtils.hasText(beanName)) {
            beanName = DEFAULT_CONTAINER_FACTORY_BEAN_NAME;
        }
        return applicationContext.getBean(beanName, PgmqListenerContainerFactory.class);
    }

    /**
     * Resolve the endpoint id, falling back to a stable bean-method id.
     */
    private String resolveId(String configuredId, Metadata metadata) {
        String id = applicationContext.getEnvironment().resolvePlaceholders(configuredId);
        if (StringUtils.hasText(id)) {
            return id;
        }
        return metadata.beanName() + "." + metadata.method().getName() + "." + metadata.index();
    }

    /**
     * Resolve a required annotation value after placeholder expansion.
     */
    private String resolveRequired(String value, String message) {
        String resolved = applicationContext.getEnvironment().resolvePlaceholders(value);
        if (!StringUtils.hasText(resolved)) {
            throw new PgmqException(message);
        }
        return resolved;
    }

    /**
     * Collected listener method metadata used to create endpoints after bean initialization.
     *
     * @param beanName Spring bean name
     * @param bean listener bean
     * @param method invocable listener method
     * @param annotation source annotation
     * @param index sequence used by the default listener id
     */
    private record Metadata(String beanName, Object bean, Method method, PgmqListener annotation, int index) {}
}
