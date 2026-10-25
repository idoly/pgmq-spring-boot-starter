package io.github.idoly.pgmq.listener;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.OffsetDateTime;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.config.PgmqAutoConfiguration;
import io.github.idoly.pgmq.config.PgmqInitializer;
import io.github.idoly.pgmq.config.PgmqProperties;
import io.github.idoly.pgmq.listener.adapter.MethodPgmqMessageListenerAdapter;
import io.github.idoly.pgmq.listener.container.DefaultPgmqListenerExecutorFactory;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainer;
import io.github.idoly.pgmq.listener.container.PgmqListenerContainerOptions;
import io.github.idoly.pgmq.listener.endpoint.PgmqListenerEndpointRegistry;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqListenerMetrics;
import io.github.idoly.pgmq.testsupport.PostgresIntegrationSupport;
import io.github.idoly.pgmq.template.PgmqTemplate;

/** Real JDBC checks against an explicitly supplied disposable PostgreSQL database. */
@EnabledIfEnvironmentVariable(named = "PGMQ_TEST_JDBC_URL", matches = ".+")
class PgmqJdbcLifecycleIntegrationTest extends PostgresIntegrationSupport {

    private static JdbcTemplate admin;
    private static PgmqTemplate adminTemplate;
    private String queue;

    @BeforeAll
    static void initialize() {
        var dataSource = new DriverManagerDataSource(jdbcUrl(), username(), password());
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
    void queueLockFollowsTheCallingTransaction() {
        var contender = new JdbcTemplate(new DriverManagerDataSource(jdbcUrl(), username(), password()));
        var transaction = new TransactionTemplate(new DataSourceTransactionManager(admin.getDataSource()));
        String tryLock = "select pg_try_advisory_xact_lock(hashtext('pgmq.queue_' || ?))";
        transaction.executeWithoutResult(status -> {
            adminTemplate.acquireQueueLock(queue);
            assertFalse(contender.queryForObject(tryLock, Boolean.class, queue));
        });
        assertTrue(contender.queryForObject(tryLock, Boolean.class, queue));
        transaction.executeWithoutResult(status -> {
            adminTemplate.acquireQueueLock(queue);
            assertFalse(contender.queryForObject(tryLock, Boolean.class, queue));
            status.setRollbackOnly();
        });
        assertTrue(contender.queryForObject(tryLock, Boolean.class, queue));
        adminTemplate.acquireQueueLock(queue);
        assertTrue(contender.queryForObject(tryLock, Boolean.class, queue),
                "auto-commit must release the transaction lock at statement completion");
    }

    @Test
    void publicQueueHelpersUseSqlValidationAndPartmanChecks() {
        assertEquals("q_mixedcase", adminTemplate.formatTableName("MixedCase", "Q"));
        assertThrows(DataAccessException.class, () -> adminTemplate.formatTableName("bad;queue", "q"));
        if (!adminTemplate.isPartmanInstalled()) {
            assertThrows(PgmqException.class,
                    () -> adminTemplate.createPartitioned(queue, "1000", "10000", 2));
            assertThrows(PgmqException.class,
                    () -> adminTemplate.convertArchivePartitioned(queue, "1000", "10000", 2));
        }
    }

    @Test
    void publicBatchSendsValidateArraysAndExpandSharedHeaders() {
        assertThrows(IllegalArgumentException.class, () -> adminTemplate.sendBatch(queue, List.of()));
        var headers = Map.<String, Object>of("source", "shared");
        assertThrows(PgmqException.class,
                () -> adminTemplate.sendBatch(queue, List.of("one", "two"), List.of(headers, headers, headers)));
        var ids = adminTemplate.sendBatch(queue, List.of("one", "two"), List.of(headers));
        assertEquals(2, ids.size());
        var messages = adminTemplate.read(queue, 30, 2);
        assertEquals(List.of("one", "two"), messages.stream().map(PgmqMessage::payload).toList());
        for (var message : messages) {
            assertEquals("shared", message.headers().get("source"));
            assertEquals("java.lang.String", message.headers().get(PgmqTemplate.TYPE_HEADER));
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyDetachIsANoopAndLegacyDropPreservesItsSqlReturnValue() {
        long id = adminTemplate.send(queue, "archived");
        adminTemplate.archive(queue, id);
        adminTemplate.detachArchive(queue);
        assertEquals(1, admin.queryForObject("select count(*) from pgmq.a_" + queue, Integer.class));
        assertTrue(adminTemplate.dropQueue(queue, false));
        assertTrue(adminTemplate.dropQueue(queue, true), "legacy SQL returns true even when the queue is missing");
    }

    @Test
    void triggerOnlyFunctionIsCoveredThroughInsertNotificationRegistration() {
        adminTemplate.enableNotifyInsert(queue, 0);
        adminTemplate.send(queue, "notify");
        var throttle = adminTemplate.listNotifyInsertThrottles().stream()
                .filter(row -> row.queueName().equals(queue)).findFirst().orElseThrow();
        assertTrue(throttle.lastNotifiedAt().toInstant().isAfter(java.time.Instant.EPOCH));
        var error = assertThrows(DataAccessException.class,
                () -> admin.queryForObject("select pgmq.notify_queue_listeners()", Object.class));
        assertTrue(error.getMostSpecificCause().getMessage().contains("trigger"));
    }

    @Test
    void partialPollingOverloadsReadVisibleMessages() {
        long id = adminTemplate.send(queue, "visible");
        var reads = List.<java.util.function.Supplier<List<PgmqMessage>>>of(
                () -> adminTemplate.readWithPoll(queue, 30, 1, 1),
                () -> adminTemplate.readWithPoll(queue, 30, 1, 1, 25),
                () -> adminTemplate.readGroupedWithPoll(queue, 30, 1, 1),
                () -> adminTemplate.readGroupedRoundRobinWithPoll(queue, 30, 1, 1),
                () -> adminTemplate.readGroupedHeadWithPoll(queue, 30, 1, 1));
        for (var read : reads) {
            adminTemplate.setVt(queue, id, 0);
            assertEquals(id, read.get().get(0).msgId());
        }
    }

    @Test
    void omittedReadAndPopArgumentsUseServerDefaults() {
        long id = adminTemplate.send(queue, "first");
        var reads = List.<java.util.function.Supplier<List<PgmqMessage>>>of(
                () -> adminTemplate.read(queue, 30, 1),
                () -> adminTemplate.readWithPoll(queue, 30, 1),
                () -> adminTemplate.readGroupedWithPoll(queue, 30, 1),
                () -> adminTemplate.readGroupedRoundRobinWithPoll(queue, 30, 1),
                () -> adminTemplate.readGroupedHeadWithPoll(queue, 30, 1));
        for (var read : reads) {
            adminTemplate.setVt(queue, id, 0);
            assertEquals(id, read.get().get(0).msgId());
        }
        adminTemplate.setVt(queue, id, 0);
        adminTemplate.send(queue, "second");
        var popped = adminTemplate.pop(queue);
        assertEquals(1, popped.size());
        assertEquals(id, popped.get(0).msgId());
        assertEquals(1L, adminTemplate.getMetrics(queue).queueLength());
    }

    @Test
    void sharedHeadersWorkWithDelayAndTimestampForQueueAndTopicBatches() {
        Map<String, String> headers = Map.of("source", "shared");
        var at = OffsetDateTime.now().minusSeconds(1);
        adminTemplate.sendBatch(queue, List.of("batch-delay"), headers, 0);
        adminTemplate.sendBatch(queue, List.of("batch-time"), headers, at);
        adminTemplate.bindTopic("validation.*", queue);
        assertEquals(1, adminTemplate.sendBatchTopic("validation.created", List.of("topic-delay"), headers, 0).size());
        assertEquals(1, adminTemplate.sendBatchTopic("validation.created", List.of("topic-time"), headers, at).size());
        var messages = adminTemplate.read(queue, 30, 10);
        assertEquals(List.of("batch-delay", "batch-time", "topic-delay", "topic-time"),
                messages.stream().map(PgmqMessage::payload).toList());
        for (var message : messages) {
            assertEquals("shared", message.headers().get("source"));
            assertEquals("java.lang.String", message.headers().get(PgmqTemplate.TYPE_HEADER));
        }
        assertEquals(Map.of("source", "shared"), headers);
    }

    @Test
    void publicFifoIndexCreationCanBeCalledRepeatedly() {
        adminTemplate.createFifoIndex(queue);
        adminTemplate.createFifoIndex(queue);
        String index = adminTemplate.formatTableName(queue, "q") + "_fifo_idx";
        assertEquals(1, admin.queryForObject("select count(*) from pg_indexes where schemaname = 'pgmq'"
                + " and indexname = ?", Integer.class, index));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "known", "non-string"})
    void badTypeHintDoesNotHideHealthyBatchMessages(String hintKind) throws Exception {
        Object hint = switch (hintKind) {
            case "missing" -> "missing.producer.Event";
            case "known" -> EventPayload.class.getName();
            default -> 42;
        };
        String headers = new ObjectMapper().writeValueAsString(Map.of(PgmqTemplate.TYPE_HEADER, hint));
        long badId = admin.queryForObject("select * from pgmq.send(?, ?::jsonb, ?::jsonb)", Long.class,
                queue, "{\"id\":\"invalid\",\"kind\":\"created\"}", headers);
        adminTemplate.send(queue, new EventPayload(42, "created"));
        var receiver = new EventReceiver();
        var failed = new CountDownLatch(1);
        var failedMessage = new java.util.concurrent.atomic.AtomicReference<PgmqMessage>();
        try (var pool = pool(3)) {
            var template = new PgmqTemplate(new JdbcTemplate(pool), new ObjectMapper());
            var adapter = new MethodPgmqMessageListenerAdapter(template, receiver,
                    EventReceiver.class.getDeclaredMethod("handle", EventPayload.class));
            var options = new PgmqListenerContainerOptions(queue, queue, 30, 2, "{}", PgmqListener.Mode.READ,
                    0, 100, 1, 1, 2, 3, true, AckMode.AUTO);
            var container = new PgmqListenerContainer(template, adapter, (error, message, ack) -> {
                failedMessage.set(message);
                ack.acknowledge();
                failed.countDown();
            }, options, new DefaultPgmqListenerExecutorFactory(), new PgmqListenerMetrics());
            try {
                container.start();
                assertTrue(failed.await(3, TimeUnit.SECONDS), "conversion failure must reach the message error handler");
                assertTrue(receiver.processed.await(3, TimeUnit.SECONDS), "healthy messages must still be delivered");
                await(() -> adminTemplate.getMetrics(queue).queueLength() == 0);
            } finally {
                container.stop();
            }
        }
        assertEquals(badId, failedMessage.get().msgId());
        assertInstanceOf(Map.class, failedMessage.get().payload());
        assertEquals(new EventPayload(42, "created"), receiver.payload);
    }

    @Test
    void typedDtoListsCreateSeparateQueueAndTopicMessages() {
        List<EventPayload> events = List.of(new EventPayload(1, "created"), new EventPayload(2, "updated"));
        var ids = adminTemplate.sendBatch(queue, events);
        assertEquals(2, ids.size());
        assertEquals(events, adminTemplate.read(queue, 30, 10).stream().map(PgmqMessage::payload).toList());
        assertEquals(2, adminTemplate.archive(queue, ids).size());
        adminTemplate.bindTopic("typed.*", queue);
        assertEquals(2, adminTemplate.sendBatchTopic("typed.created", events).size());
        assertEquals(events, adminTemplate.read(queue, 30, 10).stream().map(PgmqMessage::payload).toList());
    }

    @Test
    void staleTypeHintKeepsGenericJsonForDirectReads() {
        admin.queryForObject("select * from pgmq.send(?, ?::jsonb, ?::jsonb)", Long.class, queue,
                "{\"id\":7,\"kind\":\"created\"}", "{\"_type\":\"previous.release.Event\"}");
        var message = adminTemplate.read(queue, 30, 1).get(0);
        assertEquals(Map.of("id", 7, "kind", "created"), message.payload());
        assertEquals("previous.release.Event", message.headers().get(PgmqTemplate.TYPE_HEADER));
        assertEquals(new EventPayload(7, "created"), adminTemplate.convertMessagePayload(message, EventPayload.class));
    }

    public record EventPayload(int id, String kind) {}

    static class EventReceiver {
        final CountDownLatch processed = new CountDownLatch(1);
        volatile EventPayload payload;

        void handle(EventPayload payload) {
            this.payload = payload;
            processed.countDown();
        }
    }

    @Test
    void sqlNullPayloadAndHeadersRemainReadable() {
        long id = admin.queryForObject("select * from pgmq.send(?, ?::jsonb, ?::jsonb)", Long.class,
                queue, null, null);
        var message = adminTemplate.read(queue, 30, 1).get(0);
        assertEquals(id, message.msgId());
        assertNull(message.payload());
        assertTrue(message.headers().isEmpty());
        assertNotNull(message.lastReadAt());
        assertTrue(adminTemplate.archive(queue, id));
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

    private static HikariDataSource pool(int maxConnections) {
        var pool = new HikariDataSource();
        pool.setJdbcUrl(jdbcUrl());
        pool.setUsername(username());
        pool.setPassword(password());
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
