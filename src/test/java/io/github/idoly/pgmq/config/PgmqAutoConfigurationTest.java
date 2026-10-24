package io.github.idoly.pgmq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.PgmqErrorHandler;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerFactory;
import io.github.idoly.pgmq.listener.endpoint.MethodPgmqListenerEndpoint;
import io.github.idoly.pgmq.listener.container.PgmqListenerExecutorFactory;
import io.github.idoly.pgmq.listener.container.SimplePgmqListenerContainerFactory;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerAnnotationBeanPostProcessor;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.metrics.PgmqMetricsBinder;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PgmqAutoConfiguration.class));

    @Test
    void doesNotConfigureWithoutJdbcTemplate() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(PgmqTemplate.class));
    }

    @Test
    void configuresCoreAndListenerInfrastructureWhenJdbcTemplateIsPresent() {
        contextRunner
                .withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .run(context -> {
                    assertThat(context).hasSingleBean(PgmqProperties.class);
                    assertThat(context).hasSingleBean(PgmqInitializer.class);
                    assertThat(context).hasSingleBean(PgmqTemplate.class);
                    assertThat(context).hasSingleBean(PgmqMetricsBinder.class);
                    assertThat(context).hasSingleBean(PgmqListenerMetrics.class);
                    assertThat(context).hasSingleBean(PgmqQueueDeclarer.class);
                    assertThat(context).hasSingleBean(PgmqListenerEndpointRegistry.class);
                    assertThat(context).hasSingleBean(PgmqErrorHandler.class);
                    assertThat(context).hasSingleBean(PgmqListenerExecutorFactory.class);
                    assertThat(context).hasSingleBean(PgmqListenerAnnotationBeanPostProcessor.class);
                    assertThat(context).hasSingleBean(SimplePgmqListenerContainerFactory.class);
                    assertThat(context).hasBean("pgmqListenerContainerFactory");
                });
    }

    @Test
    void listenerInfrastructureCanBeDisabled() {
        contextRunner
                .withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withPropertyValues("pgmq.listener.enabled=false")
                .withBean(PlaceholderListener.class, PlaceholderListener::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(PgmqTemplate.class);
                    assertThat(context).doesNotHaveBean(PgmqListenerEndpointRegistry.class);
                    assertThat(context).doesNotHaveBean(PgmqErrorHandler.class);
                    assertThat(context).doesNotHaveBean(PgmqListenerExecutorFactory.class);
                    assertThat(context).doesNotHaveBean(PgmqListenerMetrics.class);
                    assertThat(context).doesNotHaveBean(PgmqListenerAnnotationBeanPostProcessor.class);
                    assertThat(context).doesNotHaveBean(SimplePgmqListenerContainerFactory.class);
                    assertThat(context).doesNotHaveBean("pgmqListenerContainerFactory");
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void registersResolvedEndpointsWithTheApplicationFactory() {
        PgmqListenerContainerFactory<MethodPgmqListenerEndpoint> factory = mock(PgmqListenerContainerFactory.class);
        var container = mock(PgmqListenerContainer.class);
        when(factory.createListenerContainer(any())).thenReturn(container);
        contextRunner
                .withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean(PlaceholderListener.class, PlaceholderListener::new)
                .withBean("pgmqListenerContainerFactory", PgmqListenerContainerFactory.class, () -> factory)
                .withPropertyValues("test.queue=resolved_queue")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registry = context.getBean(PgmqListenerEndpointRegistry.class);
                    assertThat(registry.getListenerContainer("resolvedListener")).isSameAs(container);
                    var endpoint = org.mockito.ArgumentCaptor.forClass(MethodPgmqListenerEndpoint.class);
                    verify(factory).createListenerContainer(endpoint.capture());
                    assertThat(endpoint.getValue().getQueue()).isEqualTo("resolved_queue");
                    verify(container, never()).start();
                });
    }

    static class PlaceholderListener {
        @PgmqListener(value = "${test.queue}", id = "resolvedListener", autoStartup = "false")
        public void receive(String payload) {}
    }

    private static JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(new DriverManagerDataSource());
    }
}
