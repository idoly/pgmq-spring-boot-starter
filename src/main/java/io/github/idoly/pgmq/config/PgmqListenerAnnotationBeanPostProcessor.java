package io.github.idoly.pgmq.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.core.PgmqTemplate;

/**
 * Bean post-processor that detects {@link io.github.idoly.pgmq.annotation.PgmqListener}
 * annotations on Spring beans and registers corresponding PGMQ listener containers.
 *
 * <p>Listeners are created and started after all singleton beans have been initialized.</p>
 */
public class PgmqListenerAnnotationBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton, ApplicationContextAware {

    /** List of listener metadata discovered during bean post-processing. */
    private final List<Metadata> metadatas = new ArrayList<>();

    /** Spring application context reference. */
    private ApplicationContext applicationContext;

    /**
     * Default constructor for PgmqListenerAnnotationBeanPostProcessor.
     *
     * <p>Required by Spring to instantiate this bean post-processor
     * which scans and registers methods annotated with {@link PgmqListener}.</p>
     */
    public PgmqListenerAnnotationBeanPostProcessor() {}

    /**
     * Set the Spring {@link ApplicationContext} instance.
     *
     * @param applicationContext the application context
     * @throws BeansException if context injection fails
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Post-process a bean after initialization to scan for {@link PgmqListener} annotations.
     *
     * @param bean the bean instance
     * @param beanName the name of the bean
     * @return the processed bean instance
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Method method : bean.getClass().getMethods()) {
            PgmqListener annotation = method.getAnnotation(PgmqListener.class);
            if (annotation != null) {
                metadatas.add(new Metadata(bean, method, annotation));
            }
        }
        return bean;
    }

    /**
     * Invoked after all singleton beans have been instantiated.
     * Creates and starts a {@link PgmqListenerContainer} for each discovered listener method.
     *
     * @throws IllegalStateException if {@link PgmqTemplate} bean is not found
     */
    @Override
    public void afterSingletonsInstantiated() {
        PgmqTemplate pgmqTemplate = applicationContext.getBean(PgmqTemplate.class);
        if (pgmqTemplate == null) {
            throw new IllegalStateException("PgmqTemplate bean not found");
        }

        for (Metadata metadata : metadatas) {
            new PgmqListenerContainer(pgmqTemplate, metadata.bean(), metadata.method(), metadata.annotation()).start();
        }
    }

    /**
     * Metadata holder for a PGMQ listener method.
     *
     * @param bean the bean containing the listener method
     * @param method the listener method
     * @param annotation the {@link PgmqListener} annotation on the method
     */
    private record Metadata(Object bean, Method method, PgmqListener annotation) {}
}
