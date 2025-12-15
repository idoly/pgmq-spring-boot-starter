package io.github.idoly.pgmq.config;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.core.PgmqTemplate;
import io.github.idoly.pgmq.metrics.PgmqMetrics;

/**
 * Spring Boot auto-configuration for PGMQ.
 *
 * <p>Registers core PGMQ beans after {@link JdbcTemplateAutoConfiguration}
 * is available, including {@link PgmqTemplate}, {@link PgmqInitializer},
 * listener processors, and optional metrics support.</p>
 *
 * <p>Configuration is driven by {@link PgmqProperties}.</p>
 * */
@AutoConfiguration(after = JdbcTemplateAutoConfiguration.class)
@EnableConfigurationProperties(PgmqProperties.class)
public class PgmqAutoConfiguration {

    /**
     * Default constructor for PgmqAutoConfiguration.
     *
     * <p>Required by Spring to instantiate this auto-configuration class
     * for initializing PGMQ components and registering necessary beans.</p>
     */
    public PgmqAutoConfiguration() {}


    /**
     * Provides a default {@link ObjectMapper} bean if none is defined in the context.
     *
     * @return a new ObjectMapper instance
     */
    @Bean
    @ConditionalOnMissingBean
    private ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    /**
     * Creates an ApplicationRunner bean that invokes PgmqInitializer.init()
     * after the Spring Boot application context has been fully started.
     *
     * <p>This bean is only created if the property {@code pgmq.auto-init=true} is set.</p>
     *
     * @param pgmqInitializer the injected PgmqInitializer bean
     * @return an ApplicationRunner bean
     */
    @Bean
    @ConditionalOnProperty(prefix = "pgmq", name = "auto-init", havingValue = "true")
    public ApplicationRunner runner(PgmqInitializer pgmqInitializer) {
        return args -> pgmqInitializer.init();
    }

    /**
     * Creates a PgmqInitializer bean.
     *
     * @param jdbcTemplate the JdbcTemplate used to access the database
     * @param properties   the PGMQ configuration properties
     * @return a new PgmqInitializer instance
     */
    @Bean
    public PgmqInitializer pgmqInitializer(JdbcTemplate jdbcTemplate, PgmqProperties properties) {
        return new PgmqInitializer(jdbcTemplate, properties);
    }

    /**
     * Creates the {@link PgmqTemplate} bean used for core message operations.
     *
     * @param jdbcTemplate the JdbcTemplate for database access
     * @param objectMapper the ObjectMapper for JSON serialization/deserialization
     * @return a new PgmqTemplate
     */
    @Bean
    public PgmqTemplate pgmqTemplate(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        return new PgmqTemplate(jdbcTemplate, objectMapper);
    }

    /**
     * Creates the {@link PgmqMetrics} bean for exposing queue metrics to Micrometer.
     *
     * @param pgmqTemplate the PgmqTemplate to fetch metrics from
     * @return a new PgmqMetrics instance
     */
    @Bean
    public PgmqMetrics pgmqMetrics(PgmqTemplate pgmqTemplate) {
        return new PgmqMetrics(pgmqTemplate);
    }


    /**
     * Registers a {@link QueueBeanPostProcessor} that scans and initializes all {@link io.github.idoly.pgmq.core.Queue} beans.
     *
     * @return a new QueueBeanPostProcessor instance
     */
    @Bean
    public static QueueBeanPostProcessor queueBeanPostProcessor() {
        return new QueueBeanPostProcessor();
    }

    /**
     * Registers a {@link PgmqListenerAnnotationBeanPostProcessor} that scans for methods
     * annotated with {@link io.github.idoly.pgmq.annotation.PgmqListener} and sets up listener containers.
     *
     * @return a new PgmqListenerAnnotationBeanPostProcessor instance
     */
    @Bean
    public static PgmqListenerAnnotationBeanPostProcessor pgmqListenerAnnotationBeanPostProcessor() {
        return new PgmqListenerAnnotationBeanPostProcessor();
    }
}

