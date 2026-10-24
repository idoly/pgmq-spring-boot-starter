package io.github.idoly.pgmq.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.container.DefaultPgmqListenerExecutorFactory;
import io.github.idoly.pgmq.listener.container.PgmqListenerExecutorFactory;
import io.github.idoly.pgmq.listener.container.SimplePgmqListenerContainerFactory;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerAnnotationBeanPostProcessor;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.listener.support.LoggingPgmqErrorHandler;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.metrics.PgmqMetricsBinder;
import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * JDBC-backed PGMQ auto-configuration with optional annotation-driven listeners.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration"
})
@ConditionalOnClass({JdbcTemplate.class, PgmqTemplate.class})
@ConditionalOnBean(JdbcTemplate.class)
@EnableConfigurationProperties(PgmqProperties.class)
public class PgmqAutoConfiguration {

    /** Creates the initializer used before queue declaration. */
    @Bean
    @ConditionalOnMissingBean
    public PgmqInitializer pgmqInitializer(JdbcTemplate jdbcTemplate, PgmqProperties properties) {
        return new PgmqInitializer(jdbcTemplate, properties);
    }

    /** Creates the SQL client, using the application's mapper when available. */
    @Bean
    @ConditionalOnMissingBean
    public PgmqTemplate pgmqTemplate(JdbcTemplate jdbcTemplate, ObjectProvider<ObjectMapper> objectMapper) {
        return new PgmqTemplate(jdbcTemplate, objectMapper.getIfAvailable(ObjectMapper::new));
    }

    /** Creates database queue and notification metrics. */
    @Bean
    @ConditionalOnMissingBean
    public PgmqMetricsBinder pgmqMetricsBinder(PgmqTemplate pgmqTemplate) {
        return new PgmqMetricsBinder(pgmqTemplate);
    }

    /** Creates declared queues after singleton initialization. */
    @Bean
    @ConditionalOnMissingBean
    public PgmqQueueDeclarer pgmqQueueDeclarer(PgmqTemplate pgmqTemplate, PgmqInitializer pgmqInitializer,
            PgmqProperties properties, ObjectProvider<PgmqQueue> queues) {
        return new PgmqQueueDeclarer(pgmqTemplate, pgmqInitializer, properties, queues);
    }

    /** The listener switch applies to the entire listener infrastructure. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "pgmq.listener", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class ListenerConfiguration {

        @Bean
        @ConditionalOnMissingBean
        PgmqListenerEndpointRegistry pgmqListenerEndpointRegistry() {
            return new PgmqListenerEndpointRegistry();
        }

        @Bean
        @ConditionalOnMissingBean
        PgmqErrorHandler pgmqErrorHandler() {
            return new LoggingPgmqErrorHandler();
        }

        @Bean
        @ConditionalOnMissingBean
        PgmqListenerMetrics pgmqListenerMetrics() {
            return new PgmqListenerMetrics();
        }

        @Bean
        @ConditionalOnMissingBean
        PgmqListenerExecutorFactory pgmqListenerExecutorFactory() {
            return new DefaultPgmqListenerExecutorFactory();
        }

        @Bean(name = "pgmqListenerContainerFactory")
        @ConditionalOnMissingBean(name = "pgmqListenerContainerFactory")
        SimplePgmqListenerContainerFactory pgmqListenerContainerFactory(PgmqTemplate pgmqTemplate,
                PgmqProperties properties, PgmqErrorHandler errorHandler,
                PgmqListenerExecutorFactory executorFactory, PgmqListenerMetrics listenerMetrics) {
            return new SimplePgmqListenerContainerFactory(
                    pgmqTemplate, properties, errorHandler, executorFactory, listenerMetrics);
        }

        @Bean
        @ConditionalOnMissingBean
        static PgmqListenerAnnotationBeanPostProcessor pgmqListenerAnnotationBeanPostProcessor() {
            return new PgmqListenerAnnotationBeanPostProcessor();
        }
    }
}
