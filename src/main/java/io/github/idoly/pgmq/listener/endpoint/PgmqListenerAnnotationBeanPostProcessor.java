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
 * <p>Collects annotated methods after bean initialization and creates listener
 * containers once all singletons are available.</p>
 */
public class PgmqListenerAnnotationBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton,
        ApplicationContextAware {

    /** Default bean name for the annotation-driven listener container factory. */
    public static final String DEFAULT_CONTAINER_FACTORY_BEAN_NAME = "pgmqListenerContainerFactory";

    /** Listener method metadata collected during bean post-processing. */
    private final List<Metadata> metadatas = new ArrayList<>();

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

        annotatedMethods.forEach((method, annotation) -> {
            Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
            ReflectionUtils.makeAccessible(invocableMethod);
            metadatas.add(new Metadata(beanName, bean, invocableMethod, annotation));
        });

        return bean;
    }

    /**
     * Register listener containers after all singleton beans are available.
     */
    @Override
    public void afterSingletonsInstantiated() {
        if (metadatas.isEmpty()) {
            return;
        }

        PgmqListenerEndpointRegistry registry = applicationContext.getBean(PgmqListenerEndpointRegistry.class);
        for (int i = 0; i < metadatas.size(); i++) {
            Metadata metadata = metadatas.get(i);
            MethodPgmqListenerEndpoint endpoint = createEndpoint(metadata, i);
            registry.registerListenerContainer(endpoint, resolveFactory(metadata.annotation()));
        }
    }

    /**
     * Create an endpoint from collected listener metadata.
     */
    private MethodPgmqListenerEndpoint createEndpoint(Metadata metadata, int index) {
        PgmqListener annotation = metadata.annotation();
        String queue = resolveRequired(annotation.value(), "@PgmqListener value must not be empty");
        String id = resolveId(annotation.id(), metadata, index);

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
    private String resolveId(String configuredId, Metadata metadata, int index) {
        String id = applicationContext.getEnvironment().resolvePlaceholders(configuredId);
        if (StringUtils.hasText(id)) {
            return id;
        }
        return metadata.beanName() + "." + metadata.method().getName() + "." + index;
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
     */
    private record Metadata(String beanName, Object bean, Method method, PgmqListener annotation) {}
}
