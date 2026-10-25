package io.github.idoly.pgmq.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
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
                    assertThat(endpoint.getValue().getBean()).isSameAs(context.getBean(PlaceholderListener.class));
                    assertThat(endpoint.getValue().getBeanName())
                            .isEqualTo(context.getBeanNamesForType(PlaceholderListener.class)[0]);
                    assertThat(endpoint.getValue().getMethod().getName()).isEqualTo("receive");
                    verify(container, never()).start();
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void registersLazyListenersCreatedAfterRefreshExactlyOnce() {
        PgmqListenerContainerFactory<MethodPgmqListenerEndpoint> factory = mock(PgmqListenerContainerFactory.class);
        var container = mock(PgmqListenerContainer.class);
        when(factory.createListenerContainer(any())).thenReturn(container);
        contextRunner
                .withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean("pgmqListenerContainerFactory", PgmqListenerContainerFactory.class, () -> factory)
                .withBean(PlaceholderListener.class, PlaceholderListener::new, definition -> definition.setLazyInit(true))
                .withPropertyValues("test.queue=resolved_queue")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registry = context.getBean(PgmqListenerEndpointRegistry.class);
                    assertThat(registry.getListenerContainers()).isEmpty();
                    context.getBean(PlaceholderListener.class);
                    context.getBean(PlaceholderListener.class);
                    assertThat(registry.getListenerContainer("resolvedListener")).isSameAs(container);
                    verify(factory).createListenerContainer(any());
                    verify(container, never()).start();
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void startsAutoStartupLazyListenersWhenCreatedAfterRefresh() {
        PgmqListenerContainerFactory<MethodPgmqListenerEndpoint> factory = mock(PgmqListenerContainerFactory.class);
        var container = mock(PgmqListenerContainer.class);
        when(container.isAutoStartup()).thenReturn(true);
        when(factory.createListenerContainer(any())).thenReturn(container);
        contextRunner
                .withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean("pgmqListenerContainerFactory", PgmqListenerContainerFactory.class, () -> factory)
                .withBean(AutoLazyListener.class, AutoLazyListener::new, definition -> definition.setLazyInit(true))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registry = context.getBean(PgmqListenerEndpointRegistry.class);
                    assertThat(registry.isRunning()).isTrue();
                    context.getBean(AutoLazyListener.class);
                    assertThat(registry.getListenerContainer("autoLazy")).isSameAs(container);
                    verify(container).start();
                });
    }

    @Test
    void rejectsFutureReturningListenersDuringRegistration() {
        contextRunner.withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean(FutureListener.class, FutureListener::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("must return void");
                });
    }

    @Test
    void rejectsAsyncVoidListenersDuringRegistration() {
        contextRunner.withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean(AsyncVoidListener.class, AsyncVoidListener::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("@Async is not supported");
                });
    }

    @Test
    void generatesConfigurationMetadataUsingTheProcessorPath() throws Exception {
        var metadata = new org.springframework.core.io.ClassPathResource("META-INF/spring-configuration-metadata.json");
        assertThat(metadata.getContentAsString(java.nio.charset.StandardCharsets.UTF_8))
                .contains("pgmq.listener.max-in-flight", "pgmq.listener.visibility-timeout", "pgmq.auto-init");
    }

    @Test
    void springResolvesListenerStringSettingsBeforeOptionsValidation() {
        contextRunner.withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean(SettingsListener.class, SettingsListener::new)
                .withPropertyValues("test.start=false", "test.filter={\"kind\":\"created\"}")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var options = context.getBean(PgmqListenerEndpointRegistry.class)
                            .getListenerContainer("settings").getOptions();
                    assertThat(options.autoStartup()).isFalse();
                    assertThat(options.conditional()).isEqualTo("{\"kind\":\"created\"}");
                });
    }

    @Test
    void rejectsPrototypeListenersInsteadOfCreatingRepeatedConsumers() {
        contextRunner.withBean(JdbcTemplate.class, PgmqAutoConfigurationTest::jdbcTemplate)
                .withBean(AutoLazyListener.class, AutoLazyListener::new,
                        definition -> definition.setScope(ConfigurableBeanFactory.SCOPE_PROTOTYPE))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThatThrownBy(() -> context.getBean(AutoLazyListener.class))
                            .hasStackTraceContaining("PGMQ listener beans must be singletons");
                    assertThat(context.getBean(PgmqListenerEndpointRegistry.class).getListenerContainers()).isEmpty();
                });
    }

    static class SettingsListener {
        @PgmqListener(value = "queue", id = "settings", autoStartup = "${test.start}", conditional = "${test.filter}")
        public void receive(String payload) {}
    }

    static class AutoLazyListener {
        @PgmqListener(value = "queue", id = "autoLazy")
        public void receive(String payload) {}
    }

    static class FutureListener {
        @PgmqListener(value = "queue", autoStartup = "false")
        public java.util.concurrent.CompletableFuture<Void> receive(String payload) {
            return new java.util.concurrent.CompletableFuture<>();
        }
    }

    static class AsyncVoidListener {
        @Async
        @PgmqListener(value = "queue", autoStartup = "false")
        public void receive(String payload) {}
    }

    static class PlaceholderListener {
        @PgmqListener(value = "${test.queue}", id = "resolvedListener", autoStartup = "false")
        public void receive(String payload) {}
    }

    private static JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(new DriverManagerDataSource());
    }
}
