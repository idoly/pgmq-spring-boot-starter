package io.github.idoly.pgmq.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.container.SimplePgmqListenerContainerFactory;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerAnnotationBeanPostProcessor;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.listener.support.LoggingPgmqErrorHandler;
import io.github.idoly.pgmq.metrics.PgmqMetricsBinder;
import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Auto-configuration for the PGMQ Spring Boot starter.
 *
 * <p>Registers the template, initializer, queue declarer, listener
 * infrastructure, and Micrometer binder once {@link JdbcTemplate} is available.
 * Configuration values are supplied by {@link PgmqProperties}.</p>
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration"
})
@ConditionalOnClass({JdbcTemplate.class, PgmqTemplate.class})
@ConditionalOnBean(JdbcTemplate.class)
@EnableConfigurationProperties(PgmqProperties.class)
public class PgmqAutoConfiguration {

    /**
     * Creates the initializer used by queue declaration and direct application calls.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgmqInitializer pgmqInitializer(JdbcTemplate jdbcTemplate, PgmqProperties properties) {
        return new PgmqInitializer(jdbcTemplate, properties);
    }

    /**
     * Creates the main {@link PgmqTemplate} client.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgmqTemplate pgmqTemplate(JdbcTemplate jdbcTemplate, ObjectProvider<ObjectMapper> objectMapper) {
        return new PgmqTemplate(jdbcTemplate, objectMapper.getIfAvailable(ObjectMapper::new));
    }

    /**
     * Creates the Micrometer binder for PGMQ metrics.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgmqMetricsBinder pgmqMetricsBinder(PgmqTemplate pgmqTemplate) {
        return new PgmqMetricsBinder(pgmqTemplate);
    }

    /**
     * Creates the declarer that materializes {@code PgmqQueue} beans.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgmqQueueDeclarer pgmqQueueDeclarer(PgmqTemplate pgmqTemplate, PgmqInitializer pgmqInitializer,
            PgmqProperties properties, ObjectProvider<PgmqQueue> queues) {
        return new PgmqQueueDeclarer(pgmqTemplate, pgmqInitializer, properties, queues);
    }

    /**
     * Creates the registry that owns listener containers.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "pgmq.listener", name = "enabled", havingValue = "true", matchIfMissing = true)
    public PgmqListenerEndpointRegistry pgmqListenerEndpointRegistry() {
        return new PgmqListenerEndpointRegistry();
    }

    /**
     * Creates the default listener error handler.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "pgmq.listener", name = "enabled", havingValue = "true", matchIfMissing = true)
    public PgmqErrorHandler pgmqErrorHandler() {
        return new LoggingPgmqErrorHandler();
    }

    /**
     * Creates the default listener container factory.
     */
    @Bean(name = "pgmqListenerContainerFactory")
    @ConditionalOnMissingBean(name = "pgmqListenerContainerFactory")
    @ConditionalOnProperty(prefix = "pgmq.listener", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SimplePgmqListenerContainerFactory pgmqListenerContainerFactory(PgmqTemplate pgmqTemplate,
            PgmqProperties properties, PgmqErrorHandler errorHandler) {
        return new SimplePgmqListenerContainerFactory(pgmqTemplate, properties, errorHandler);
    }

    /**
     * Creates the processor that detects {@code @PgmqListener} methods.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "pgmq.listener", name = "enabled", havingValue = "true", matchIfMissing = true)
    public static PgmqListenerAnnotationBeanPostProcessor pgmqListenerAnnotationBeanPostProcessor() {
        return new PgmqListenerAnnotationBeanPostProcessor();
    }
}
