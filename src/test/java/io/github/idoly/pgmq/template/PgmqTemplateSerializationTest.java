package io.github.idoly.pgmq.template;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Type;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.queue.PgmqQueue;

class PgmqTemplateSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void sendAddsTypeHeaderWithoutMutatingCallerHeaders() throws Exception {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);
        Map<String, Object> headers = new HashMap<>();
        headers.put("source", "test");

        long msgId = pgmqTemplate.send("queue", new Payload(1, "created"), headers, OffsetDateTime.parse("2026-07-04T00:00:00Z"));

        assertEquals(1L, msgId);
        assertFalse(headers.containsKey(PgmqTemplate.TYPE_HEADER));

        Map<?, ?> sentHeaders = objectMapper.readValue((String) jdbcTemplate.args[2], Map.class);
        assertEquals("test", sentHeaders.get("source"));
        assertEquals(objectMapper.constructType(Payload.class).toCanonical(), sentHeaders.get(PgmqTemplate.TYPE_HEADER));
    }

    @Test
    void validateQueueNameReturnsBooleanAfterPostgresVoidValidation() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        assertTrue(pgmqTemplate.validateQueueName("queue"));

        assertEquals("select true from (select pgmq.validate_queue_name(?)) validated", jdbcTemplate.sql);
        assertSame(Boolean.class, jdbcTemplate.requiredType);
    }

    @Test
    void validateQueueNameOnlyRejectsEmptyNamesBeforeSqlValidation() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.validateQueueName(""));

        assertTrue(pgmqTemplate.validateQueueName("bad$name"));
        assertEquals("bad$name", jdbcTemplate.args[0]);
    }

    @Test
    void varargBatchMethodsUseTemplateNullPayloadValidation() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        IllegalArgumentException queueException =
                assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.sendBatch("queue", "a", null));
        IllegalArgumentException topicException =
                assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.sendBatchTopic("events.created", "a", null));

        assertEquals("Message payload must not be null", queueException.getMessage());
        assertEquals("Message payload must not be null", topicException.getMessage());
    }

    @Test
    void emptyBatchMethodsUseTemplateValidation() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);
        List<Object> emptyMessages = List.of();

        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.sendBatch("queue", emptyMessages));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.sendBatchTopic("events.created", emptyMessages));
    }

    @Test
    void detachArchiveCallsCurrentSqlFunction() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        pgmqTemplate.detachArchive("queue");

        assertEquals("select pgmq.detach_archive(?)", jdbcTemplate.sql);
        assertSame(Object.class, jdbcTemplate.requiredType);
    }

    @Test
    void createFifoIndexesAllCallsCurrentSqlFunction() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        pgmqTemplate.createFifoIndexesAll();

        assertEquals("select pgmq.create_fifo_indexes_all()", jdbcTemplate.sql);
        assertSame(Object.class, jdbcTemplate.requiredType);
    }

    @Test
    void convertArchivePartitionedCallsCurrentSqlFunction() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        pgmqTemplate.convertArchivePartitioned("queue", "10000", "100000", 10);

        assertEquals("select pgmq.convert_archive_partitioned(?, ?, ?, ?)", jdbcTemplate.sql);
        assertSame(Object.class, jdbcTemplate.requiredType);
        assertEquals("queue", jdbcTemplate.args[0]);
        assertEquals("10000", jdbcTemplate.args[1]);
        assertEquals("100000", jdbcTemplate.args[2]);
        assertEquals(10, jdbcTemplate.args[3]);
    }

    @Test
    void queueDeclarationRejectsConflictingModes() {
        assertThrows(IllegalArgumentException.class, () -> new PgmqQueue("queue", true, true, null));
    }

    @Test
    void queueDeclarationRejectsMissingModeFlags() {
        assertThrows(IllegalArgumentException.class, () -> new PgmqQueue("queue", null, false, null));
        assertThrows(IllegalArgumentException.class, () -> new PgmqQueue("queue", false, null, null));
    }

    @Test
    void bulkLongArrayFunctionsUseExplicitBigintCasts() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        pgmqTemplate.delete("queue", List.of(1L, 2L));

        assertEquals("select * from pgmq.delete(?, ?::bigint[])", jdbcTemplate.sql);
        assertArrayEquals(new Long[] {1L, 2L}, (Long[]) jdbcTemplate.args[1]);

        pgmqTemplate.archive("queue", List.of(3L, 4L));

        assertEquals("select * from pgmq.archive(?, ?::bigint[])", jdbcTemplate.sql);
        assertArrayEquals(new Long[] {3L, 4L}, (Long[]) jdbcTemplate.args[1]);
    }

    @Test
    void batchHeadersMustBeSharedOrMatchMessageCount() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        assertThrows(PgmqException.class, () -> pgmqTemplate.sendBatch(
                "queue",
                List.of("a", "b"),
                List.of(Map.of("source", "a"), Map.of("source", "b"), Map.of("source", "c"))
        ));
        assertThrows(PgmqException.class, () -> pgmqTemplate.sendBatchTopic(
                "events.created",
                List.of("a", "b"),
                List.of(Map.of("source", "a"), Map.of("source", "b"), Map.of("source", "c"))
        ));
    }

    @Test
    void rejectsInvalidTemplateArgumentsBeforeSqlCalls() {
        CapturingJdbcTemplate jdbcTemplate = new CapturingJdbcTemplate();
        PgmqTemplate pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);

        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.read("", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.read("queue", -1, 1));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.read("queue", 1, 0));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.readWithPoll("queue", 1, 1, 1, 0, "{}"));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.send("queue", null));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.send("queue", "payload", -1));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.send("queue", "payload", Map.of("", "value")));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.delete("queue", 0));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.archive("queue", Arrays.asList(1L, null)));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.enableNotifyInsert("queue", -1));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.validateRoutingKey(""));
        assertThrows(IllegalArgumentException.class, () -> pgmqTemplate.validateTopicPattern(""));
    }

    @Test
    void convertMessagePayloadSupportsGenericTargetTypes() throws Exception {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new CapturingJdbcTemplate(), objectMapper);
        PgmqMessage message = new PgmqMessage(1L, 0, null, null, null,
                List.of(Map.of("id", 1, "event", "created")), Map.of());
        Type targetType = ListenerMethod.class.getDeclaredMethod("handle", List.class).getGenericParameterTypes()[0];

        Object converted = pgmqTemplate.convertMessagePayload(message, targetType);

        assertTrue(converted instanceof List<?>);
        assertEquals(List.of(new Payload(1, "created")), converted);
    }

    @Test
    void convertPayloadHandlesNullAndAlreadyAssignableValues() {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new CapturingJdbcTemplate(), objectMapper);
        Payload payload = new Payload(1, "created");

        assertEquals(null, pgmqTemplate.convertPayload(null, Payload.class));
        assertSame(payload, pgmqTemplate.convertPayload(payload, Payload.class));
    }

    public record Payload(int id, String event) {}

    private static class ListenerMethod {
        void handle(List<Payload> payloads) {}
    }

    private static class CapturingJdbcTemplate extends JdbcTemplate {
        private String sql;
        private Class<?> requiredType;
        private Object[] args;

        @Override
        public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
            this.sql = sql;
            this.requiredType = requiredType;
            this.args = args;
            if (requiredType == Long.class) {
                return requiredType.cast(1L);
            }
            if (requiredType == Boolean.class) {
                return requiredType.cast(Boolean.TRUE);
            }
            if (requiredType == Object.class) {
                return requiredType.cast(new Object());
            }
            return null;
        }

        @Override
        public <T> T queryForObject(String sql, Class<T> requiredType) {
            return queryForObject(sql, requiredType, new Object[0]);
        }

        @Override
        public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) {
            this.sql = sql;
            this.requiredType = elementType;
            this.args = args;
            return List.of(elementType.cast(1L));
        }
    }
}
