package io.github.idoly.pgmq.template;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.github.idoly.pgmq.config.PgmqInitializer;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqMetrics;
import io.github.idoly.pgmq.notification.NotifyInsertThrottle;
import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.testsupport.PostgresPartmanContainerSupport;
import io.github.idoly.pgmq.topic.TopicBatchResult;
import io.github.idoly.pgmq.topic.TopicBinding;
import io.github.idoly.pgmq.topic.TopicRoute;

@SpringBootTest(classes = PgmqTemplateTest.TestApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgmqTemplateTest extends PostgresPartmanContainerSupport {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
    }

    @Autowired
    private PgmqTemplate pgmqTemplate;

    private static final String TEST_QUEUE = "test_queue";

    @BeforeAll
    static void setUp(@Autowired PgmqInitializer pgmqInitializer) {
        pgmqInitializer.init();
    }

    @BeforeEach
    void createQueue() {
        pgmqTemplate.createUnlogged(TEST_QUEUE);
    }

    @AfterEach
    void purgeQueue() {
        pgmqTemplate.dropQueue(TEST_QUEUE);
    }

    @Test
    void testSendAndReadSingleMessage() {
        String payload = "hello";
        long msgId = pgmqTemplate.send(TEST_QUEUE, payload);
        assertTrue(msgId > 0);

        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
        assertEquals(1, messages.size());
        assertEquals(payload, messages.get(0).payload());
    }

    @Test
    void testReadWithConditional() {
        EventPayload created = new EventPayload("created", 1);
        pgmqTemplate.send(TEST_QUEUE, created);
        pgmqTemplate.send(TEST_QUEUE, new EventPayload("deleted", 2));

        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 10, "{\"type\":\"created\"}");

        assertEquals(1, messages.size());
        assertEquals(created, messages.get(0).payload());
    }

    @Test
    void testReadWithPoll() {
        pgmqTemplate.send(TEST_QUEUE, "poll");

        List<PgmqMessage> messages = pgmqTemplate.readWithPoll(
                TEST_QUEUE, 10, 1, 1, 50, PgmqTemplate.DEFAULT_CONDITIONAL);

        assertEquals(1, messages.size());
        assertEquals("poll", messages.get(0).payload());
    }

    @Test
    void testSendWithHeadersAndDelay() throws InterruptedException {
        Map<String, Object> headers = Map.of("key", "value");
        long msgId = pgmqTemplate.send(TEST_QUEUE, "delayed", headers, 1);
        assertTrue(msgId > 0);

        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
        assertTrue(messages.isEmpty());

        PgmqMessage msg = awaitVisibleMessage();
        assertEquals("delayed", msg.payload());
        assertEquals("value", msg.headers().get("key"));
        assertEquals("java.lang.String", msg.headers().get("_type"));
    }

    @Test
    void testSendBatch() {
        List<Object> payloads = List.of("a", "b", "c");
        List<Long> ids = pgmqTemplate.sendBatch(TEST_QUEUE, payloads);
        assertEquals(3, ids.size());

        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 3);
        assertEquals(3, messages.size());
        List<String> readPayloads = messages.stream()
                .map(PgmqMessage::payload)
                .map(String.class::cast)
                .toList();
        assertTrue(readPayloads.containsAll(payloads));
    }

    @Test
    void testSendBatchWithPerMessageHeaders() {
        List<Object> payloads = List.of("a", "b");
        List<Map<String, Object>> headers = List.of(
                Map.of("source", "first"),
                Map.of("source", "second")
        );

        List<Long> ids = pgmqTemplate.sendBatch(TEST_QUEUE, payloads, headers);

        assertEquals(2, ids.size());
        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 2);
        assertEquals(2, messages.size());
        assertTrue(messages.stream().anyMatch(message ->
                "a".equals(message.payload())
                        && "first".equals(message.headers().get("source"))
                        && "java.lang.String".equals(message.headers().get(PgmqTemplate.TYPE_HEADER))));
        assertTrue(messages.stream().anyMatch(message ->
                "b".equals(message.payload())
                        && "second".equals(message.headers().get("source"))
                        && "java.lang.String".equals(message.headers().get(PgmqTemplate.TYPE_HEADER))));
    }

    @Test
    void testPopAndDelete() {
        long msgId = pgmqTemplate.send(TEST_QUEUE, "toPop");
        List<PgmqMessage> messages = pgmqTemplate.pop(TEST_QUEUE);
        assertEquals(1, messages.size());
        assertEquals(msgId, messages.get(0).msgId());

        boolean deleted = pgmqTemplate.delete(TEST_QUEUE, msgId);
        assertFalse(deleted);
    }

    @Test
    void testArchive() {
        long msgId = pgmqTemplate.send(TEST_QUEUE, "toArchive");
        boolean archived = pgmqTemplate.archive(TEST_QUEUE, msgId);
        assertTrue(archived);
    }

    @Test
    void testBatchArchiveDeleteAndSetVt() {
        OffsetDateTime visibleAt = OffsetDateTime.now().plusSeconds(30);
        long timestampId = pgmqTemplate.send(TEST_QUEUE, "timestamp-vt");
        PgmqMessage timestampMessage = pgmqTemplate.setVt(TEST_QUEUE, timestampId, visibleAt);
        assertEquals(timestampId, timestampMessage.msgId());
        assertEquals(visibleAt.toInstant().getEpochSecond(), timestampMessage.vt().toInstant().getEpochSecond());

        List<Long> vtIds = pgmqTemplate.sendBatch(TEST_QUEUE, List.of("vt-a", "vt-b"));
        List<PgmqMessage> updatedMessages = pgmqTemplate.setVt(TEST_QUEUE, vtIds, 0);
        assertEquals(2, updatedMessages.size());
        assertTrue(updatedMessages.stream().map(PgmqMessage::msgId).toList().containsAll(vtIds));

        List<PgmqMessage> timestampMessages = pgmqTemplate.setVt(TEST_QUEUE, vtIds, visibleAt);
        assertEquals(2, timestampMessages.size());
        assertTrue(timestampMessages.stream()
                .allMatch(message -> message.vt().toInstant().getEpochSecond() == visibleAt.toInstant().getEpochSecond()));

        List<Long> archiveIds = pgmqTemplate.sendBatch(TEST_QUEUE, List.of("archive-a", "archive-b"));
        List<Long> archivedIds = pgmqTemplate.archive(TEST_QUEUE, archiveIds);
        assertEquals(2, archivedIds.size());
        assertTrue(archivedIds.containsAll(archiveIds));

        List<Long> deleteIds = pgmqTemplate.sendBatch(TEST_QUEUE, List.of("delete-a", "delete-b"));
        List<Long> deletedIds = pgmqTemplate.delete(TEST_QUEUE, deleteIds);
        assertEquals(2, deletedIds.size());
        assertTrue(deletedIds.containsAll(deleteIds));
    }

    @Test
    void testQueueOperations() {
        assertTrue(pgmqTemplate.validateQueueName(TEST_QUEUE));
        assertFalse(pgmqTemplate.dropQueue("missing_queue"));

        List<PgmqQueue> queues = pgmqTemplate.listQueues();
        assertTrue(queues.stream().anyMatch(q -> q.queueName().equals(TEST_QUEUE)));

        PgmqMetrics metrics = pgmqTemplate.getMetrics(TEST_QUEUE);
        assertNotNull(metrics);
        assertNull(metrics.newestMsgAgeSec());
        assertNull(metrics.oldestMsgAgeSec());
        assertNull(metrics.defaultPartitionLength());

        List<PgmqMetrics> allMetrics = pgmqTemplate.getMetricsAll();
        assertTrue(allMetrics.stream().anyMatch(m -> m.queueName().equals(TEST_QUEUE)));

        long purged = pgmqTemplate.purgeQueue(TEST_QUEUE);
        assertEquals(0L, purged);
    }

    @Test
    void testNotifyInsert() {
        pgmqTemplate.enableNotifyInsert(TEST_QUEUE, 100);
        assertEquals(100, notifyThrottle(TEST_QUEUE).throttleIntervalMs());

        pgmqTemplate.updateNotifyInsert(TEST_QUEUE, 200);
        assertEquals(200, notifyThrottle(TEST_QUEUE).throttleIntervalMs());

        pgmqTemplate.disableNotifyInsert(TEST_QUEUE);
        assertTrue(pgmqTemplate.listNotifyInsertThrottles().stream()
                .noneMatch(throttle -> throttle.queueName().equals(TEST_QUEUE)));
    }

    @Test
    void testMonitorCanReadMetricsWithoutMessagePayloads() {
        pgmqTemplate.send(TEST_QUEUE, "private-payload");
        var jdbc = pgmqTemplate.getJdbcTemplate();
        assertFalse(Boolean.TRUE.equals(jdbc.queryForObject(
                "select has_column_privilege('pg_monitor', 'pgmq.q_test_queue', 'message', 'SELECT')",
                Boolean.class)));
        assertFalse(Boolean.TRUE.equals(jdbc.queryForObject(
                "select has_column_privilege('pg_monitor', 'pgmq.q_test_queue', 'headers', 'SELECT')",
                Boolean.class)));
        assertFalse(Boolean.TRUE.equals(jdbc.queryForObject(
                "select has_table_privilege('pg_monitor', 'pgmq.a_test_queue', 'SELECT')",
                Boolean.class)));
        jdbc.execute((org.springframework.jdbc.core.ConnectionCallback<Void>) connection -> {
            try (var statement = connection.createStatement()) {
                statement.execute("set role pg_monitor");
                try (var result = statement.executeQuery("select queue_length from pgmq.metrics('test_queue')")) {
                    assertTrue(result.next());
                    assertEquals(1L, result.getLong(1));
                } finally {
                    statement.execute("reset role");
                }
            }
            return null;
        });
    }

    @Test
    void testMixedCaseQueueInsertUpdatesNotificationTimestamp() {
        String queue = "MixedCaseNotify";
        try {
            pgmqTemplate.create(queue);
            pgmqTemplate.enableNotifyInsert(queue, 0);
            OffsetDateTime before = notifyThrottle(queue).lastNotifiedAt();
            pgmqTemplate.send(queue, "notify");
            assertTrue(notifyThrottle(queue).lastNotifiedAt().isAfter(before));
        } finally {
            pgmqTemplate.dropQueue(queue);
        }
    }

    @Test
    void testTopicRoutingOperations() {
        String pattern = "orders.*";
        String routingKey = "orders.created";

        assertTrue(pgmqTemplate.validateTopicPattern(pattern));
        assertTrue(pgmqTemplate.validateRoutingKey(routingKey));

        pgmqTemplate.bindTopic(pattern, TEST_QUEUE);

        List<TopicBinding> bindings = pgmqTemplate.listTopicBindings(TEST_QUEUE);
        assertTrue(bindings.stream().anyMatch(binding -> binding.pattern().equals(pattern)
                && binding.queueName().equals(TEST_QUEUE)));
        assertTrue(pgmqTemplate.listTopicBindings().stream().anyMatch(binding -> binding.pattern().equals(pattern)
                && binding.queueName().equals(TEST_QUEUE)));

        List<TopicRoute> routes = pgmqTemplate.testRouting(routingKey);
        assertTrue(routes.stream().anyMatch(route -> route.pattern().equals(pattern)
                && route.queueName().equals(TEST_QUEUE)));

        assertEquals(1, pgmqTemplate.sendTopic(routingKey, "topic-one", Map.of("source", "topic")));
        List<TopicBatchResult> batchResults = pgmqTemplate.sendBatchTopic(routingKey, List.of("topic-two", "topic-three"));
        assertEquals(2, batchResults.size());
        assertTrue(batchResults.stream().allMatch(result -> result.queueName().equals(TEST_QUEUE)));

        List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 3);
        List<Object> payloads = messages.stream().map(PgmqMessage::payload).toList();
        assertTrue(payloads.containsAll(List.of("topic-one", "topic-two", "topic-three")));
        assertTrue(messages.stream().anyMatch(message ->
                "topic-one".equals(message.payload()) && "topic".equals(message.headers().get("source"))));

        assertTrue(pgmqTemplate.unbindTopic(pattern, TEST_QUEUE));
        assertFalse(pgmqTemplate.unbindTopic(pattern, TEST_QUEUE));
    }

    @Test
    void testFifoReadModes() {
        pgmqTemplate.createFifoIndex(TEST_QUEUE);
        List<Long> ids = pgmqTemplate.sendBatch(
                TEST_QUEUE,
                List.of("a-1", "a-2", "b-1"),
                List.of(
                        Map.of("x-pgmq-group", "group-a"),
                        Map.of("x-pgmq-group", "group-a"),
                        Map.of("x-pgmq-group", "group-b")
                )
        );

        assertReadIds(pgmqTemplate.readGrouped(TEST_QUEUE, 10, 3), ids);
        makeVisible(ids);

        assertReadIds(pgmqTemplate.readGroupedWithPoll(TEST_QUEUE, 10, 3, 1, 50), ids);
        makeVisible(ids);

        assertReadIds(pgmqTemplate.readGroupedRoundRobin(TEST_QUEUE, 10, 3), ids);
        makeVisible(ids);

        assertReadIds(pgmqTemplate.readGroupedRoundRobinWithPoll(TEST_QUEUE, 10, 3, 1, 50), ids);
    }

    @Test
    void testFifoHeadReadReturnsOnlyVisibleGroupHeads() {
        pgmqTemplate.createFifoIndex(TEST_QUEUE);
        long groupAHead = pgmqTemplate.send(TEST_QUEUE, "a-1", Map.of("x-pgmq-group", "group-a"));
        long groupATail = pgmqTemplate.send(TEST_QUEUE, "a-2", Map.of("x-pgmq-group", "group-a"));
        long groupBHead = pgmqTemplate.send(TEST_QUEUE, "b-1", Map.of("x-pgmq-group", "group-b"));

        List<Long> headIds = pgmqTemplate.readGroupedHead(TEST_QUEUE, 10, 10).stream()
                .map(PgmqMessage::msgId)
                .toList();

        assertEquals(2, headIds.size());
        assertTrue(headIds.containsAll(List.of(groupAHead, groupBHead)));
        assertFalse(headIds.contains(groupATail));

        makeVisible(List.of(groupAHead, groupBHead));
        List<Long> polledHeadIds = pgmqTemplate.readGroupedHeadWithPoll(TEST_QUEUE, 10, 10, 1, 50).stream()
                .map(PgmqMessage::msgId)
                .toList();

        assertEquals(2, polledHeadIds.size());
        assertTrue(polledHeadIds.containsAll(List.of(groupAHead, groupBHead)));
        assertFalse(polledHeadIds.contains(groupATail));
    }

    @Test
    void testStandardQueueCreationVariants() {
        String createdQueue = "created_queue";
        String nonPartitionedQueue = "non_partitioned_queue";
        pgmqTemplate.dropQueue(createdQueue);
        pgmqTemplate.dropQueue(nonPartitionedQueue);

        try {
            pgmqTemplate.create(createdQueue);
            pgmqTemplate.createNonPartitioned(nonPartitionedQueue);

            List<PgmqQueue> queues = pgmqTemplate.listQueues();
            assertTrue(queues.stream().anyMatch(q -> q.queueName().equals(createdQueue)
                    && Boolean.FALSE.equals(q.partitioned())
                    && Boolean.FALSE.equals(q.unlogged())));
            assertTrue(queues.stream().anyMatch(q -> q.queueName().equals(nonPartitionedQueue)
                    && Boolean.FALSE.equals(q.partitioned())
                    && Boolean.FALSE.equals(q.unlogged())));
        } finally {
            pgmqTemplate.dropQueue(createdQueue);
            pgmqTemplate.dropQueue(nonPartitionedQueue);
        }
    }

    @Test
    void testPartitionedQueueCreation() {
        String queue = "partitioned_queue";
        pgmqTemplate.getJdbcTemplate().update("create extension if not exists pg_partman");
        assertTrue(pgmqTemplate.isPartmanInstalled());
        pgmqTemplate.dropQueue(queue);

        try {
            pgmqTemplate.createPartitioned(queue, "10000", "100000", 2);
            pgmqTemplate.createFifoIndexesAll();
            assertNotNull(pgmqTemplate.getMetrics(queue).defaultPartitionLength());
            assertEquals(2, pgmqTemplate.getJdbcTemplate().queryForObject(
                    "select premake from part_config where parent_table = ?", Integer.class, "pgmq.q_" + queue));
            long msgId = pgmqTemplate.send(queue, "partitioned");
            assertEquals(msgId, pgmqTemplate.read(queue, 10, 1).get(0).msgId());

            List<PgmqQueue> queues = pgmqTemplate.listQueues();
            assertTrue(queues.stream().anyMatch(q -> q.queueName().equals(queue)
                    && Boolean.TRUE.equals(q.partitioned())
                    && Boolean.FALSE.equals(q.unlogged())));
        } finally {
            pgmqTemplate.dropQueue(queue);
        }
    }

    private PgmqMessage awaitVisibleMessage() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            List<PgmqMessage> messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
            if (!messages.isEmpty()) {
                return messages.get(0);
            }
            Thread.sleep(50);
        }
        return fail("Timed out waiting for visible message");
    }

    private NotifyInsertThrottle notifyThrottle(String queue) {
        return pgmqTemplate.listNotifyInsertThrottles().stream()
                .filter(throttle -> throttle.queueName().equals(queue))
                .findFirst()
                .orElseGet(() -> fail("Missing notify_insert throttle for queue " + queue));
    }

    private void assertReadIds(List<PgmqMessage> messages, List<Long> expectedIds) {
        List<Long> actualIds = messages.stream().map(PgmqMessage::msgId).toList();
        assertEquals(expectedIds.size(), actualIds.size());
        assertTrue(actualIds.containsAll(expectedIds));
    }

    private void makeVisible(List<Long> msgIds) {
        List<PgmqMessage> messages = pgmqTemplate.setVt(TEST_QUEUE, msgIds, 0);
        assertEquals(msgIds.size(), messages.size());
    }

    public record EventPayload(String type, int id) {}
}
