package io.github.idoly.pgmq.config;

import java.util.IdentityHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;

/**
 * Reports listener long-poll connection budgets after application startup.
 *
 * <p>Recognizes pools exposing {@code maximumPoolSize}, including HikariCP, without
 * borrowing a connection or changing pool settings. Other pools are skipped.</p>
 */
public class PgmqConnectionPoolDiagnostics implements ApplicationListener<ContextRefreshedEvent> {

    private static final Log logger = LogFactory.getLog(PgmqConnectionPoolDiagnostics.class);

    private final PgmqListenerEndpointRegistry registry;
    private final ApplicationContext applicationContext;

    /**
     * Create startup diagnostics for the registry in this application context.
     *
     * @param registry listener registry
     * @param applicationContext owning application context
     */
    public PgmqConnectionPoolDiagnostics(PgmqListenerEndpointRegistry registry,
            ApplicationContext applicationContext) {
        this.registry = registry;
        this.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (event.getApplicationContext() != applicationContext) {
            return;
        }
        Map<DataSource, Long> pollersByDataSource = new IdentityHashMap<>();
        for (var container : registry.getListenerContainers()) {
            if (!container.isRunning()) {
                continue;
            }
            DataSource dataSource = targetDataSource(container.getDataSource());
            if (dataSource != null) {
                long pollers = container.getOptions().pollConcurrency();
                pollersByDataSource.merge(dataSource, pollers, Long::sum);
            }
        }
        pollersByDataSource.forEach(this::reportBudget);
    }

    private DataSource targetDataSource(DataSource dataSource) {
        while (dataSource instanceof DelegatingDataSource delegating) {
            DataSource target = delegating.getTargetDataSource();
            if (target == dataSource) {
                break;
            }
            dataSource = target;
        }
        return dataSource;
    }

    private void reportBudget(DataSource dataSource, long pollers) {
        var properties = new BeanWrapperImpl(dataSource);
        if (!properties.isReadableProperty("maximumPoolSize")) {
            return;
        }
        Object size = properties.getPropertyValue("maximumPoolSize");
        if (!(size instanceof Number maximum) || maximum.intValue() <= 0) {
            return;
        }
        int maxConnections = maximum.intValue();
        String budget = "PGMQ connection budget for " + dataSource.getClass().getSimpleName()
                + ": active listeners may occupy up to " + pollers
                + " long-poll connections; maximumPoolSize=" + maxConnections;
        if (pollers >= maxConnections) {
            logger.warn(budget + ". Long polling can occupy the entire pool. Reduce poll-concurrency"
                    + " or increase pool capacity, leaving room for acknowledgements, sends and business SQL.");
        } else {
            logger.info(budget + "; remaining capacity before business SQL and acknowledgements="
                    + (maxConnections - pollers));
        }
    }
}
