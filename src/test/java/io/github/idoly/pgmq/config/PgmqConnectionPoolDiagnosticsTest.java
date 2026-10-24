package io.github.idoly.pgmq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import com.zaxxer.hikari.HikariDataSource;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerOptions;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;

@ExtendWith(OutputCaptureExtension.class)
class PgmqConnectionPoolDiagnosticsTest {

    @Test
    void aggregatesSharedPoolsAndKeepsOtherPoolsSeparate(CapturedOutput output) {
        try (var context = new GenericApplicationContext();
             var crowdedPool = new HikariDataSource();
             var healthyPool = new HikariDataSource()) {
            crowdedPool.setMaximumPoolSize(4);
            healthyPool.setMaximumPoolSize(5);
            var registry = mock(PgmqListenerEndpointRegistry.class);
            var containers = List.of(
                    container(crowdedPool, 2, 10, true),
                    container(new TransactionAwareDataSourceProxy(crowdedPool), 2, 10, true),
                    container(healthyPool, 1, 1, true),
                    container(healthyPool, 20, 20, false));
            when(registry.getListenerContainers()).thenReturn(containers);

            new PgmqConnectionPoolDiagnostics(registry, context)
                    .onApplicationEvent(new ContextRefreshedEvent(context));

            assertThat(output).contains("up to 4 long-poll connections; maximumPoolSize=4")
                    .contains("Long polling can occupy the entire pool")
                    .contains("up to 1 long-poll connections; maximumPoolSize=5")
                    .contains("remaining capacity before business SQL and acknowledgements=4");
            assertThat(crowdedPool.isRunning()).isFalse();
            assertThat(healthyPool.isRunning()).isFalse();
        }
    }

    @Test
    void unknownPoolDoesNotBorrowConnectionsOrFailStartup(CapturedOutput output) throws Exception {
        try (var context = new GenericApplicationContext()) {
            var dataSource = spy(new DriverManagerDataSource());
            var registry = mock(PgmqListenerEndpointRegistry.class);
            var containers = List.of(container(dataSource, 2, 10, true));
            when(registry.getListenerContainers()).thenReturn(containers);

            new PgmqConnectionPoolDiagnostics(registry, context)
                    .onApplicationEvent(new ContextRefreshedEvent(context));

            verify(dataSource, never()).getConnection();
            assertThat(output).doesNotContain("PGMQ connection budget");
        }
    }

    @Test
    void ignoresEventsFromChildContexts(CapturedOutput output) {
        try (var context = new GenericApplicationContext(); var child = new GenericApplicationContext()) {
            var registry = mock(PgmqListenerEndpointRegistry.class);
            new PgmqConnectionPoolDiagnostics(registry, context)
                    .onApplicationEvent(new ContextRefreshedEvent(child));
            verifyNoInteractions(registry);
            assertThat(output).doesNotContain("PGMQ connection budget");
        }
    }

    private PgmqListenerContainer container(DataSource dataSource, int pollers, int maxInFlight, boolean running) {
        var container = mock(PgmqListenerContainer.class);
        when(container.isRunning()).thenReturn(running);
        when(container.getDataSource()).thenReturn(dataSource);
        var options = new PgmqListenerContainerOptions(
                "listener", "queue", 30, 10, "{}", PgmqListener.Mode.READ,
                5, 100, 1, pollers, maxInFlight, 30, true, AckMode.AUTO);
        when(container.getOptions()).thenReturn(options);
        return container;
    }
}
