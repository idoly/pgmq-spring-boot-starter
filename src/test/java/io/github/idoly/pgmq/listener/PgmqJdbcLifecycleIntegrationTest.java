package io.github.idoly.pgmq.listener;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.config.PgmqAutoConfiguration;
import io.github.idoly.pgmq.config.PgmqInitializer;
import io.github.idoly.pgmq.config.PgmqProperties;
import io.github.idoly.pgmq.listener.container.DefaultPgmqListenerExecutorFactory;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerOptions;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.template.PgmqTemplate;

/** Real JDBC checks against an explicitly supplied disposable PostgreSQL database. */
@EnabledIfEnvironmentVariable(named = "PGMQ_TEST_JDBC_URL", matches = ".+")
class PgmqJdbcLifecycleIntegrationTest {

    private static JdbcTemplate admin;
    private static PgmqTemplate adminTemplate;
    private String queue;

    @BeforeAll
    static void initialize() {
        var dataSource = new DriverManagerDataSource(System.getenv("PGMQ_TEST_JDBC_URL"),
                username(), System.getenv().getOrDefault("PGMQ_TEST_PASSWORD", "postgres"));
        admin = new JdbcTemplate(dataSource);
        admin.setQueryTimeout(5);
        new PgmqInitializer(admin, new PgmqProperties()).init();
        adminTemplate = new PgmqTemplate(admin, new ObjectMapper());
    }

    @BeforeEach
    void createQueue() {
        queue = "validation_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        adminTemplate.create(queue);
    }

    @AfterEach
    void dropQueue() {
        adminTemplate.dropQueue(queue);
    }

    @Test
    void zeroPollReadsVisibleMessagesInAllFourModes() {
        long id = adminTemplate.send(queue, "visible", Map.of("x-pgmq-group", "one"));
        var reads = List.<java.util.function.Supplier<List<PgmqMessage>>>of(
                () -> adminTemplate.readWithPoll(queue, 30, 1, 0, 100, "{}"),
                () -> adminTemplate.readGroupedWithPoll(queue, 30, 1, 0, 100),
                () -> adminTemplate.readGroupedRoundRobinWithPoll(queue, 30, 1, 0, 100),
                () -> adminTemplate.readGroupedHeadWithPoll(queue, 30, 1, 0, 100));
        for (var read : reads) {
            adminTemplate.setVt(queue, id, 0);
            assertEquals(id, read.get().get(0).msgId());
        }
    }

    @Test
    void emptyZeroPollWaitsBetweenDatabaseCalls() throws Exception {
        try (var pool = pool(1)) {
            var times = new ArrayList<Long>();
            var threePolls = new CountDownLatch(3);
            var template = new PgmqTemplate(new JdbcTemplate(pool), new ObjectMapper()) {
                @Override
                public List<PgmqMessage> readWithPoll(String q, int vt, int qty, int seconds, int interval, String condition) {
                    synchronized (times) {
                        times.add(System.nanoTime());
                    }
                    threePolls.countDown();
                    return super.readWithPoll(q, vt, qty, seconds, interval, condition);
                }
            };
            var container = container(template, queue, 0, 2, (message, ack) -> fail("queue is empty"),
                    new DefaultPgmqListenerExecutorFactory());
            try {
                container.start();
                assertTrue(threePolls.await(3, TimeUnit.SECONDS));
            } finally {
                container.stop();
            }
            synchronized (times) {
                assertTrue(times.get(1) - times.get(0) >= TimeUnit.MILLISECONDS.toNanos(90));
                assertTrue(times.get(2) - times.get(1) >= TimeUnit.MILLISECONDS.toNanos(90));
            }
        }
    }

    @Test
    void twoListenersMakeProgressWithTwoSharedConnections() throws Exception {
        String other = queue + "_other";
        adminTemplate.create(other);
        try (var pool = pool(2)) {
            var jdbc = new JdbcTemplate(pool);
            jdbc.setQueryTimeout(3);
            var template = new PgmqTemplate(jdbc, new ObjectMapper());
            var consumed = new CountDownLatch(6);
            PgmqMessageListener listener = (message, ack) -> {
                jdbc.execute("select pg_sleep(0.03)");
                consumed.countDown();
            };
            var first = container(template, queue, 1, 3, listener, new DefaultPgmqListenerExecutorFactory());
            var second = container(template, other, 1, 3, listener, new DefaultPgmqListenerExecutorFactory());
            for (int i = 0; i < 3; i++) {
                adminTemplate.send(queue, "one");
                adminTemplate.send(other, "two");
            }
            try {
                first.start();
                second.start();
                assertTrue(consumed.await(10, TimeUnit.SECONDS));
                await(() -> adminTemplate.getMetrics(queue).queueLength() == 0
                        && adminTemplate.getMetrics(other).queueLength() == 0);
                assertTrue(pool.getHikariPoolMXBean().getTotalConnections() <= 2);
            } finally {
                first.stop();
                second.stop();
            }
        } finally {
            adminTemplate.dropQueue(other);
        }
    }

    @Test
    void exhaustedPoolFailsWithinConfiguredAcquisitionTimeout() throws Exception {
        try (var pool = pool(1); var held = pool.getConnection()) {
            long started = System.nanoTime();
            assertThrows(DataAccessResourceFailureException.class,
                    () -> new JdbcTemplate(pool).queryForObject("select 1", Integer.class));
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(millis >= 200 && millis < 2000, "acquisition timeout took " + millis + "ms");
        }
    }

    @Test
    void queryTimeoutCancelsSlowSqlAndReturnsUsableConnection() {
        try (var pool = pool(1)) {
            var jdbc = new JdbcTemplate(pool);
            jdbc.setQueryTimeout(1);
            long started = System.nanoTime();
            var error = assertThrows(DataAccessException.class, () -> jdbc.execute("select pg_sleep(5)"));
            assertEquals("57014", assertInstanceOf(SQLException.class, error.getMostSpecificCause()).getSQLState());
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(4));
            assertEquals(1, jdbc.queryForObject("select 1", Integer.class));
            assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
        }
    }

    @Test
    void stopWithQueryTimeoutDrainsBlockingPoller() throws Exception {
        try (var pool = pool(1)) {
            var jdbc = new JdbcTemplate(pool);
            jdbc.setQueryTimeout(1);
            var executors = new TrackingExecutors();
            var container = container(new PgmqTemplate(jdbc, new ObjectMapper()), queue, 10, 3,
                    (message, ack) -> {}, executors);
            try {
                container.start();
                await(() -> activePolls(pool.getPoolName()) > 0);
                long started = System.nanoTime();
                container.stop();
                assertTrue(executors.poller.isTerminated());
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(4));
                assertEquals(0, activePolls(pool.getPoolName()));
                assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
            } finally {
                container.stop();
            }
        }
    }

    @Test
    void immediateStopDoesNotRequireDriverToCancelOnInterrupt() throws Exception {
        try (var pool = pool(1)) {
            var executors = new TrackingExecutors();
            var container = container(new PgmqTemplate(new JdbcTemplate(pool), new ObjectMapper()), queue, 2, 0,
                    (message, ack) -> {}, executors);
            try {
                container.start();
                await(() -> activePolls(pool.getPoolName()) > 0);
                container.stop();
                System.out.println("PGMQ immediate stop left SQL active: " + (activePolls(pool.getPoolName()) > 0));
                assertTrue(executors.poller.awaitTermination(4, TimeUnit.SECONDS));
                assertEquals(0, activePolls(pool.getPoolName()));
            } finally {
                container.stop();
                assertTrue(executors.poller.awaitTermination(4, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void springProxyRollsBackBusinessWriteAndManualAcknowledgementTogether() {
        String table = "business_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("create table " + table + " (msg_id bigint)");
        var failure = new CountDownLatch(1);
        try (var pool = pool(3)) {
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(PgmqAutoConfiguration.class))
                    .withUserConfiguration(TransactionalListenerConfiguration.class)
                    .withBean(DataSource.class, () -> pool)
                    .withBean(JdbcTemplate.class, () -> new JdbcTemplate(pool))
                    .withBean(PgmqErrorHandler.class, () -> (error, message, ack) -> failure.countDown())
                    .withPropertyValues("validation.queue=" + queue, "validation.table=" + table,
                            "pgmq.listener.max-poll-seconds=0", "pgmq.listener.max-in-flight=1",
                            "pgmq.listener.executor-shutdown-timeout-seconds=3")
                    .run(context -> {
                        assertNull(context.getStartupFailure());
                        assertTrue(AopUtils.isAopProxy(context.getBean(RollbackListener.class)));
                        adminTemplate.send(queue, "transactional");
                        var container = context.getBean(PgmqListenerEndpointRegistry.class)
                                .getListenerContainer("transactional");
                        try {
                            container.start();
                            assertTrue(failure.await(5, TimeUnit.SECONDS));
                        } finally {
                            container.stop();
                        }
                        assertEquals(0, admin.queryForObject("select count(*) from " + table, Integer.class));
                        assertEquals(1L, adminTemplate.getMetrics(queue).queueLength());
                        assertEquals(0, admin.queryForObject("select count(*) from pgmq.a_" + queue, Integer.class));
                    });
        } finally {
            admin.execute("drop table " + table);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TransactionalListenerConfiguration {
        @Bean
        DataSourceTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        RollbackListener rollbackListener(JdbcTemplate jdbc, @Value("${validation.table}") String table) {
            return new RollbackListener(jdbc, table);
        }
    }

    public static class RollbackListener {
        private final JdbcTemplate jdbc;
        private final String table;

        RollbackListener(JdbcTemplate jdbc, String table) {
            this.jdbc = jdbc;
            this.table = table;
        }

        @Transactional
        @PgmqListener(value = "${validation.queue}", id = "transactional", autoStartup = "false", ackMode = AckMode.MANUAL)
        public void receive(PgmqMessage message, PgmqAcknowledgment acknowledgment) {
            jdbc.update("insert into " + table + " values (?)", message.msgId());
            acknowledgment.acknowledge();
            throw new IllegalStateException("roll back business work and archive");
        }
    }

    private static class TrackingExecutors extends DefaultPgmqListenerExecutorFactory {
        private ExecutorService poller;

        @Override
        public ExecutorService createPollExecutor(PgmqListenerContainerOptions options) {
            poller = super.createPollExecutor(options);
            return poller;
        }
    }

    private PgmqListenerContainer container(PgmqTemplate template, String name, int pollSeconds, int shutdownSeconds,
            PgmqMessageListener listener, DefaultPgmqListenerExecutorFactory executors) {
        var options = new PgmqListenerContainerOptions(name, name, 30, 1, "{}", PgmqListener.Mode.READ,
                pollSeconds, 100, 1, 1, 1, shutdownSeconds, true, AckMode.AUTO);
        return new PgmqListenerContainer(template, listener, (error, message, ack) -> {}, options,
                executors, new PgmqListenerMetrics());
    }

    private static String username() {
        return System.getenv().getOrDefault("PGMQ_TEST_USERNAME", "postgres");
    }

    private static HikariDataSource pool(int maxConnections) {
        var pool = new HikariDataSource();
        pool.setJdbcUrl(System.getenv("PGMQ_TEST_JDBC_URL"));
        pool.setUsername(username());
        pool.setPassword(System.getenv().getOrDefault("PGMQ_TEST_PASSWORD", "postgres"));
        pool.setMaximumPoolSize(maxConnections);
        pool.setMinimumIdle(0);
        pool.setConnectionTimeout(250);
        pool.setPoolName("pgmq_validation_" + UUID.randomUUID().toString().replace("-", ""));
        pool.addDataSourceProperty("ApplicationName", pool.getPoolName());
        return pool;
    }

    private static int activePolls(String applicationName) {
        return admin.queryForObject("select count(*) from pg_stat_activity where application_name = ?"
                + " and state = 'active' and query like '%read_with_poll%'", Integer.class, applicationName);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting for database state");
            Thread.sleep(20);
        }
    }
}
