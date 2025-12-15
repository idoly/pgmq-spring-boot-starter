package io.github.idoly.pgmq.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import io.github.idoly.pgmq.config.PgmqAutoConfiguration;
import io.github.idoly.pgmq.config.PgmqInitializer;

@SpringBootTest(
    classes = { 
        DataSourceAutoConfiguration.class, 
        JdbcTemplateAutoConfiguration.class, 
        PgmqAutoConfiguration.class
    },
    properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/postgres",
        "spring.datasource.username=postgres",
        "spring.datasource.password=postgres",
        "spring.datasource.driver-class-name=org.postgresql.Driver"
    }
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgmqTemplateTest {

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

        List<Message> messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
        assertEquals(1, messages.size());
        assertEquals(payload, messages.get(0).message());
    }

    @Test
    void testSendWithHeadersAndDelay() {
        Map<String, Object> headers = new HashMap<>(){{ put("key", "value"); }};
        long msgId = pgmqTemplate.send(TEST_QUEUE, "delayed", headers, 1);
        assertTrue(msgId > 0);

        List<Message> messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
        assertTrue(messages.size() == 0);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        messages = pgmqTemplate.read(TEST_QUEUE, 10, 1);
        Message msg = messages.get(0);
        assertEquals("delayed", msg.message());
        assertEquals("value", msg.headers().get("key"));
        assertEquals("java.lang.String", msg.headers().get("_type"));
    }

    @Test
    void testSendBatch() {
        List<Object> payloads = List.of("a", "b", "c");
        List<Long> ids = pgmqTemplate.sendBatch(TEST_QUEUE, payloads);
        assertEquals(3, ids.size());

        List<Message> messages = pgmqTemplate.read(TEST_QUEUE, 10, 3);
        List<String> readPayloads = messages.stream()
                .map(Message::message)
                .map(String.class::cast)
                .toList();
        assertTrue(readPayloads.containsAll(payloads));
    }

    @Test
    void testPopAndDelete() {
        long msgId = pgmqTemplate.send(TEST_QUEUE, "toPop");
        List<Message> messages = pgmqTemplate.pop(TEST_QUEUE);
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
    void testQueueOperations() {
        List<Queue> queues = pgmqTemplate.listQueues();
        assertTrue(queues.stream().anyMatch(q -> q.getQueueName().equals(TEST_QUEUE)));

        Metrics metrics = pgmqTemplate.getMetrics(TEST_QUEUE);
        assertNotNull(metrics);

        long purged = pgmqTemplate.purgeQueue(TEST_QUEUE);
        assertTrue(purged >= 0);
    }

    @Test
    void testNotifyInsert() {
        pgmqTemplate.enableNotifyInsert(TEST_QUEUE, 0);
        pgmqTemplate.disableNotifyInsert(TEST_QUEUE);
    }

    @Test
    void testPartitionedQueueCreation() {
        List<Queue> queues = pgmqTemplate.listQueues();
        assertTrue(queues.stream().anyMatch(q -> q.getQueueName().equals(TEST_QUEUE)));
    }
}
