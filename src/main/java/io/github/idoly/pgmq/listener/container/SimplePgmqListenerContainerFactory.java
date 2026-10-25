package io.github.idoly.pgmq.listener.container;

import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.util.Assert;
import org.springframework.util.StringValueResolver;
import org.springframework.util.StringUtils;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.config.PgmqProperties;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.PgmqMessageListener;
import io.github.idoly.pgmq.listener.adapter.MethodPgmqMessageListenerAdapter;
import io.github.idoly.pgmq.listener.endpoint.MethodPgmqListenerEndpoint;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Default factory for annotation-driven listener containers.
 *
 * <p>Merges {@link PgmqListener} attributes with {@link PgmqProperties}
 * defaults and creates method listener adapters for endpoint methods.</p>
 */
public class SimplePgmqListenerContainerFactory
        implements PgmqListenerContainerFactory<MethodPgmqListenerEndpoint>, EmbeddedValueResolverAware {

    /** Template shared by containers created by this factory. */
    private final PgmqTemplate pgmqTemplate;

    /** Starter properties used as listener defaults. */
    private final PgmqProperties properties;

    /** Error handler shared by containers created by this factory. */
    private final PgmqErrorHandler errorHandler;

    /** Factory for the executors owned by each listener container. */
    private final PgmqListenerExecutorFactory executorFactory;

    /** Metrics shared by containers created by this factory. */
    private final PgmqListenerMetrics listenerMetrics;

    private StringValueResolver valueResolver = value -> value;

    /**
     * Create a factory with custom executor and metric infrastructure.
     *
     * @param pgmqTemplate template used by containers and adapters
     * @param properties starter properties used as defaults
     * @param errorHandler error handler shared by created containers
     * @param executorFactory factory for container-owned executors
     * @param listenerMetrics listener metric recorder
     */
    public SimplePgmqListenerContainerFactory(PgmqTemplate pgmqTemplate, PgmqProperties properties,
            PgmqErrorHandler errorHandler, PgmqListenerExecutorFactory executorFactory,
            PgmqListenerMetrics listenerMetrics) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(properties, "PgmqProperties must not be null");
        Assert.notNull(errorHandler, "PgmqErrorHandler must not be null");
        Assert.notNull(executorFactory, "PgmqListenerExecutorFactory must not be null");
        Assert.notNull(listenerMetrics, "PgmqListenerMetrics must not be null");
        this.pgmqTemplate = pgmqTemplate;
        this.properties = properties;
        this.errorHandler = errorHandler;
        this.executorFactory = executorFactory;
        this.listenerMetrics = listenerMetrics;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        Assert.notNull(resolver, "String value resolver must not be null");
        this.valueResolver = resolver;
    }

    /**
     * Create a listener container for a method endpoint.
     */
    @Override
    public PgmqListenerContainer createListenerContainer(MethodPgmqListenerEndpoint endpoint) {
        PgmqMessageListener messageListener = new MethodPgmqMessageListenerAdapter(
                pgmqTemplate, endpoint.getBean(), endpoint.getMethod());
        PgmqListenerContainerOptions listenerOptions = resolveOptions(endpoint);
        PgmqListenerContainer container = new PgmqListenerContainer(
                pgmqTemplate,
                messageListener,
                errorHandler,
                listenerOptions,
                executorFactory,
                listenerMetrics);
        listenerMetrics.registerContainer(container);
        return container;
    }

    /**
     * Resolve annotation attributes against configured listener defaults.
     */
    private PgmqListenerContainerOptions resolveOptions(MethodPgmqListenerEndpoint endpoint) {
        PgmqListener annotation = endpoint.getAnnotation();
        PgmqProperties.Listener listenerProperties = properties.getListener();

        try {
            return new PgmqListenerContainerOptions(
                    endpoint.getId(), endpoint.getQueue(),
                    resolveInt(annotation.visibilityTimeout(), listenerProperties.getVisibilityTimeout()),
                    resolveInt(annotation.batchSize(), listenerProperties.getBatchSize()),
                    resolveConditional(annotation.conditional(), listenerProperties.getConditional()),
                    resolveMode(annotation.mode(), listenerProperties.getMode()),
                    resolveInt(annotation.maxPollSeconds(), listenerProperties.getMaxPollSeconds()),
                    resolveInt(annotation.pollIntervalMs(), listenerProperties.getPollIntervalMs()),
                    resolveInt(annotation.consumerConcurrency(), listenerProperties.getConsumerConcurrency()),
                    resolveInt(annotation.pollConcurrency(), listenerProperties.getPollConcurrency()),
                    resolveInt(annotation.maxInFlight(), listenerProperties.getMaxInFlight()),
                    resolveInt(annotation.executorShutdownTimeoutSeconds(), listenerProperties.getExecutorShutdownTimeoutSeconds()),
                    resolveAutoStartup(annotation.autoStartup(), listenerProperties.isAutoStartup()),
                    resolveAckMode(annotation.ackMode(), listenerProperties.getAckMode()));
        } catch (IllegalArgumentException error) {
            throw new PgmqException("Invalid PGMQ listener " + endpoint.getId() + ": " + error.getMessage(), error);
        }
    }

    /** Only -1 means inherit; the resolved options own numeric validation. */
    private int resolveInt(int annotationValue, int propertyValue) {
        return annotationValue == -1 ? propertyValue : annotationValue;
    }

    /**
     * Resolve the JSON conditional used by read mode.
     */
    private String resolveConditional(String annotationValue, String propertyValue) {
        String conditional = StringUtils.hasText(annotationValue) ? annotationValue : propertyValue;
        if (!StringUtils.hasText(conditional)) {
            return PgmqTemplate.DEFAULT_CONDITIONAL;
        }
        String resolved = valueResolver.resolveStringValue(conditional);
        return StringUtils.hasText(resolved) ? resolved : PgmqTemplate.DEFAULT_CONDITIONAL;
    }

    /**
     * Resolve the listener read mode.
     */
    private PgmqListener.Mode resolveMode(PgmqListener.Mode annotationValue, PgmqListener.Mode propertyValue) {
        PgmqListener.Mode mode = annotationValue == PgmqListener.Mode.DEFAULT ? propertyValue : annotationValue;
        return mode == null || mode == PgmqListener.Mode.DEFAULT ? PgmqListener.Mode.READ : mode;
    }

    /**
     * Resolve the auto-startup flag.
     */
    private boolean resolveAutoStartup(String annotationValue, boolean propertyValue) {
        if (!StringUtils.hasText(annotationValue)) {
            return propertyValue;
        }
        CustomBooleanEditor editor = new CustomBooleanEditor(false);
        editor.setAsText(valueResolver.resolveStringValue(annotationValue));
        return (Boolean) editor.getValue();
    }

    /**
     * Resolve the acknowledgement mode.
     */
    private AckMode resolveAckMode(AckMode annotationValue, AckMode propertyValue) {
        AckMode ackMode = annotationValue == AckMode.DEFAULT ? propertyValue : annotationValue;
        return ackMode == null || ackMode == AckMode.DEFAULT ? AckMode.AUTO : ackMode;
    }
}
