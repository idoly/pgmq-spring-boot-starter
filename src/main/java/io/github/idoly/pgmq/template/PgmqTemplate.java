package io.github.idoly.pgmq.template;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.jspecify.annotations.NonNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.metrics.PgmqMetrics;
import io.github.idoly.pgmq.notification.NotifyInsertThrottle;
import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.topic.TopicBatchResult;
import io.github.idoly.pgmq.topic.TopicBinding;
import io.github.idoly.pgmq.topic.TopicRoute;

/**
 * JDBC-backed client for PGMQ SQL functions.
 *
 * <p>Provides queue reads, sends, batch operations, topic routing, metrics,
 * notification management, and queue administration through {@link JdbcTemplate}.
 * Payloads and headers are serialized with {@link ObjectMapper}.</p>
 *
 * <p>Messages sent through this template include a {@value #TYPE_HEADER} header
 * so listener arguments can be deserialized with type information. If that hint cannot be used,
 * reads retain generic JSON values and listeners convert them to their declared parameter types.</p>
 *
 * <p>A long-poll duration of zero performs one immediate read without waiting.</p>
 */
public class PgmqTemplate {

    /** Header key that stores the canonical Java payload type. */
    public static final String TYPE_HEADER = "_type";

    /** Default JSON conditional used by PGMQ read functions. */
    public static final String DEFAULT_CONDITIONAL = "{}";

    /** Default long-poll timeout in seconds. */
    public static final int DEFAULT_MAX_POLL_SECONDS = 5;

    /** Default long-poll retry interval in milliseconds. */
    public static final int DEFAULT_POLL_INTERVAL_MS = 100;

    /** JDBC client used to invoke PGMQ SQL functions. */
    private final JdbcTemplate jdbcTemplate;

    /** JSON mapper used for payload and header serialization. */
    private final ObjectMapper objectMapper;

    /**
     * Create a template backed by the given JDBC and JSON components.
     *
     * @param jdbcTemplate JDBC client used for database calls
     * @param objectMapper JSON mapper used for payload conversion
     */
    public PgmqTemplate(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null");
        Assert.notNull(objectMapper, "ObjectMapper must not be null");
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Return the JDBC client used by this template.
     *
     * @return JDBC client
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * Return the JSON mapper used by this template.
     *
     * @return JSON mapper
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * Read visible messages using the default JSON conditional.
     *
     * <p>The JSON conditional is supplied by the SQL function's default.</p>
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> read(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Read visible messages matching a JSON conditional.
     *
     * <p>The conditional is passed to {@code pgmq.read(..., conditional)} and
     * follows PostgreSQL JSON containment semantics.</p>
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @param conditional JSON conditional such as {@code {"type":"created"}}
     * @return read messages
     */
    public List<PgmqMessage> read(@NonNull String queue, int vt, int qty, @NonNull String conditional) {
        validateReadArguments(queue, vt, qty);
        Assert.hasText(conditional, "Conditional must not be empty");
        String sql = "select * from pgmq.read(?, ?, ?, ?::jsonb)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, conditional);
    }

    /**
     * Long-poll for visible messages using default polling options.
     *
     * <p>Polling duration, interval and conditional are supplied by SQL defaults.</p>
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> readWithPoll(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_with_poll(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for visible messages with explicit polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @param maxPollSeconds maximum seconds to wait for messages
     * @param pollIntervalMs milliseconds between poll attempts
     * @param conditional JSON conditional
     * @return read messages
     */
    public List<PgmqMessage> readWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs, @NonNull String conditional) {
        validateMaxPollSeconds(maxPollSeconds);
        validatePollInterval(pollIntervalMs);
        if (maxPollSeconds == 0) {
            return read(queue, vt, qty, conditional);
        }
        validateReadArguments(queue, vt, qty);
        Assert.hasText(conditional, "Conditional must not be empty");
        String sql = "select * from pgmq.read_with_poll(?, ?, ?, ?, ?, ?::jsonb)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs, conditional);
    }

    /**
     * Read with an explicit polling duration and the default polling interval and conditional.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages
     * @param maxPollSeconds maximum seconds to wait
     * @return read messages
     */
    public List<PgmqMessage> readWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds) {
        validateMaxPollSeconds(maxPollSeconds);
        if (maxPollSeconds == 0) {
            return read(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_with_poll(?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds);
    }

    /**
     * Read with explicit polling options and the default JSON conditional.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages
     * @param maxPollSeconds maximum seconds to wait
     * @param pollIntervalMs milliseconds between attempts
     * @return read messages
     */
    public List<PgmqMessage> readWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs) {
        validateMaxPollSeconds(maxPollSeconds);
        validatePollInterval(pollIntervalMs);
        if (maxPollSeconds == 0) {
            return read(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_with_poll(?, ?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs);
    }

    /**
     * Read messages using FIFO group-aware batch selection.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> readGrouped(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO group-aware messages using default polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> readGroupedWithPoll(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_with_poll(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO group-aware messages with explicit polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @param maxPollSeconds maximum seconds to wait for messages
     * @param pollIntervalMs milliseconds between poll attempts
     * @return read messages
     */
    public List<PgmqMessage> readGroupedWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs) {
        validateMaxPollSeconds(maxPollSeconds);
        validatePollInterval(pollIntervalMs);
        if (maxPollSeconds == 0) {
            return readGrouped(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_with_poll(?, ?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs);
    }

    /**
     * Read grouped messages with an explicit polling duration and default polling interval.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages
     * @param maxPollSeconds maximum seconds to wait
     * @return read messages
     */
    public List<PgmqMessage> readGroupedWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds) {
        validateMaxPollSeconds(maxPollSeconds);
        if (maxPollSeconds == 0) {
            return readGrouped(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_with_poll(?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds);
    }

    /**
     * Read visible FIFO groups in round-robin order.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> readGroupedRoundRobin(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_rr(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO round-robin messages using default polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @return read messages
     */
    public List<PgmqMessage> readGroupedRoundRobinWithPoll(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_rr_with_poll(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO round-robin messages with explicit polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages to read
     * @param maxPollSeconds maximum seconds to wait for messages
     * @param pollIntervalMs milliseconds between poll attempts
     * @return read messages
     */
    public List<PgmqMessage> readGroupedRoundRobinWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs) {
        validateMaxPollSeconds(maxPollSeconds);
        validatePollInterval(pollIntervalMs);
        if (maxPollSeconds == 0) {
            return readGroupedRoundRobin(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_rr_with_poll(?, ?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs);
    }

    /**
     * Read the head message from up to {@code qty} FIFO groups.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of group heads to read
     * @return read messages
     */
    public List<PgmqMessage> readGroupedHead(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_head(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO group heads using default polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of group heads to read
     * @return read messages
     */
    public List<PgmqMessage> readGroupedHeadWithPoll(@NonNull String queue, int vt, int qty) {
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_head_with_poll(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty);
    }

    /**
     * Long-poll for FIFO group heads with explicit polling options.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of group heads to read
     * @param maxPollSeconds maximum seconds to wait for messages
     * @param pollIntervalMs milliseconds between poll attempts
     * @return read messages
     */
    public List<PgmqMessage> readGroupedHeadWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs) {
        validateMaxPollSeconds(maxPollSeconds);
        validatePollInterval(pollIntervalMs);
        if (maxPollSeconds == 0) {
            return readGroupedHead(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_head_with_poll(?, ?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs);
    }

    /**
     * Read round-robin groups with an explicit polling duration and default polling interval.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages
     * @param maxPollSeconds maximum seconds to wait
     * @return read messages
     */
    public List<PgmqMessage> readGroupedRoundRobinWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds) {
        validateMaxPollSeconds(maxPollSeconds);
        if (maxPollSeconds == 0) {
            return readGroupedRoundRobin(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_rr_with_poll(?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds);
    }

    /**
     * Read FIFO group heads with an explicit polling duration and default polling interval.
     *
     * @param queue the queue name
     * @param vt visibility timeout in seconds
     * @param qty maximum number of messages
     * @param maxPollSeconds maximum seconds to wait
     * @return read messages
     */
    public List<PgmqMessage> readGroupedHeadWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds) {
        validateMaxPollSeconds(maxPollSeconds);
        if (maxPollSeconds == 0) {
            return readGroupedHead(queue, vt, qty);
        }
        validateReadArguments(queue, vt, qty);
        String sql = "select * from pgmq.read_grouped_head_with_poll(?, ?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds);
    }

    /**
     * Archive one message.
     *
     * @param queue the queue name
     * @param msgId the message ID to archive
     * @return whether a message was archived
     */
    public Boolean archive(@NonNull String queue, long msgId) {
        validateQueueNameArgument(queue);
        validateMessageId(msgId);
        String sql = "select pgmq.archive(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue, msgId);
    }

    /**
     * Archive multiple messages.
     *
     * @param queue the queue name
     * @param msgIds the message IDs to archive
     * @return archived message IDs
     */
    public List<Long> archive(@NonNull String queue, @NonNull List<Long> msgIds) {
        validateQueueNameArgument(queue);
        validateMessageIds(msgIds);
        String sql = "select * from pgmq.archive(?, ?::bigint[])";
        return jdbcTemplate.queryForList(sql, Long.class, queue, msgIds.toArray(new Long[0]));
    }

    /**
     * Delete one message without archiving it.
     *
     * @param queue the queue name
     * @param msgId the message ID to delete
     * @return whether a message was deleted
     */
    public Boolean delete(@NonNull String queue, long msgId) {
        validateQueueNameArgument(queue);
        validateMessageId(msgId);
        String sql = "select pgmq.delete(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue, msgId);
    }

    /**
     * Delete multiple messages without archiving them.
     *
     * @param queue the queue name
     * @param msgIds the message IDs to delete
     * @return deleted message IDs
     */
    public List<Long> delete(@NonNull String queue, @NonNull List<Long> msgIds) {
        validateQueueNameArgument(queue);
        validateMessageIds(msgIds);
        String sql = "select * from pgmq.delete(?, ?::bigint[])";
        return jdbcTemplate.queryForList(sql, Long.class, queue, msgIds.toArray(new Long[0]));
    }

    /**
     * Send one message immediately with no custom headers.
     *
     * @param queue the target queue name
     * @param msg message payload
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg) {
        return send(queue, msg, Map.of(), 0);
    }

    /**
     * Send one message immediately with custom headers.
     *
     * @param queue the target queue name
     * @param msg message payload
     * @param headers custom headers
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String, ?> headers) {
        return send(queue, msg, headers, 0);
    }

    /**
     * Send one message with a relative delay and no custom headers.
     *
     * @param queue the target queue name
     * @param msg message payload
     * @param delay delay in seconds before visibility
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg, int delay) {
        return send(queue, msg, Map.of(), delay);
    }

    /**
     * Send one message with an absolute visibility timestamp and no custom headers.
     *
     * @param queue the target queue name
     * @param msg message payload
     * @param visibleAt timestamp when the message becomes visible
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull OffsetDateTime visibleAt) {
        return send(queue, msg, Map.of(), visibleAt);
    }

    /**
     * Send one message with custom headers and a relative delay.
     *
     * @param queue the target queue name
     * @param msg message payload
     * @param headers custom headers
     * @param delay delay in seconds before visibility
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String, ?> headers, int delay) {
        validateDelay(delay);
        return sendInternal(queue, msg, headers, delay);
    }

    /**
     * Send one message with custom headers and an absolute visibility timestamp.
     *
     * <p>The supplied headers are copied and enriched with {@link #TYPE_HEADER};
     * the caller's map is not mutated.</p>
     *
     * @param queue the target queue name
     * @param msg message payload
     * @param headers custom headers
     * @param visibleAt timestamp when the message becomes visible
     * @return inserted message ID
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String, ?> headers,
            @NonNull OffsetDateTime visibleAt) {
        Assert.notNull(visibleAt, "Visibility timestamp must not be null");
        return sendInternal(queue, msg, headers, visibleAt);
    }

    private long sendInternal(String queue, Object msg, Map<String, ?> headers, Object visibilityValue) {
        validateQueueNameArgument(queue);
        validateMessage(msg);
        validateHeaders(headers);
        Map<String, Object> messageHeaders = messageHeaders(msg, headers);

        String sql = "select * from pgmq.send(?, ?::jsonb, ?::jsonb, ?)";
        return jdbcTemplate.queryForObject(
                sql, Long.class, queue, toJson(msg), toJson(messageHeaders), visibilityValue);
    }

    /**
     * Send messages immediately with no custom headers.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull Object... msgs) {
        Assert.notNull(msgs, "Messages must not be null");
        return sendBatch(queue, Arrays.stream(msgs).toList(), List.of());
    }

    /**
     * Send messages immediately with no custom headers.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs) {
        return sendBatch(queue, msgs, List.of());
    }

    /**
     * Send messages immediately with one shared header map.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param headers shared headers
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs, @NonNull Map<String, ?> headers) {
        return sendBatch(queue, msgs, Collections.singletonList(headers));
    }

    /**
     * Send a batch with shared headers and an integer delay.
     *
     * @param queue the target queue
     * @param msgs message payloads
     * @param headers shared headers
     * @param delay delay in seconds
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs,
            @NonNull Map<String, ?> headers, int delay) {
        return sendBatch(queue, msgs, Collections.singletonList(headers), delay);
    }

    /**
     * Send a batch with shared headers and an absolute visibility timestamp.
     *
     * @param queue the target queue
     * @param msgs message payloads
     * @param headers shared headers
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs,
            @NonNull Map<String, ?> headers, @NonNull OffsetDateTime visibleAt) {
        return sendBatch(queue, msgs, Collections.singletonList(headers), visibleAt);
    }

    /**
     * Send messages immediately with shared or per-message headers.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs, @NonNull List<? extends Map<String, ?>> headers) {
        return sendBatch(queue, msgs, headers, 0);
    }

    /**
     * Send messages with a relative delay and no custom headers.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param delay delay in seconds before visibility
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs, int delay) {
        return sendBatch(queue, msgs, List.of(), delay);
    }

    /**
     * Send messages with an absolute visibility timestamp and no custom headers.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs,
            @NonNull OffsetDateTime visibleAt) {
        return sendBatch(queue, msgs, List.of(), visibleAt);
    }

    /**
     * Send messages with shared or per-message headers and a relative delay.
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @param delay delay in seconds before visibility
     * @return inserted message IDs
     * @throws PgmqException if {@code headers} contains more than one element and its size does not match {@code msgs}
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs, @NonNull List<? extends Map<String, ?>> headers, int delay) {
        validateDelay(delay);
        return sendBatchInternal(queue, msgs, headers, delay);
    }

    /**
     * Send messages with headers and an absolute visibility timestamp.
     *
     * <p>Passing zero or one header map applies shared headers to all messages.
     * Passing multiple header maps requires the header list size to match the
     * message list size.</p>
     *
     * @param queue the target queue name
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs
     * @throws PgmqException if {@code headers} contains more than one element and its size does not match {@code msgs}
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<?> msgs,
            @NonNull List<? extends Map<String, ?>> headers, @NonNull OffsetDateTime visibleAt) {
        Assert.notNull(visibleAt, "Visibility timestamp must not be null");
        return sendBatchInternal(queue, msgs, headers, visibleAt);
    }

    private List<Long> sendBatchInternal(String queue, List<?> msgs,
            List<? extends Map<String, ?>> headers, Object visibilityValue) {
        validateQueueNameArgument(queue);
        validateMessages(msgs);
        validateHeaderList(headers);
        List<Map<String, Object>> messageHeaders = messageHeaders(msgs, headers);

        String sql = "select * from pgmq.send_batch(?, ?::jsonb[], ?::jsonb[], ?)";
        return jdbcTemplate.queryForList(
                sql, Long.class, queue, toJsonArray(msgs), toJsonArray(messageHeaders), visibilityValue);
    }

    /**
     * Return metrics for one queue.
     *
     * @param queue the queue name
     * @return queue metrics
     */
    public PgmqMetrics getMetrics(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select * from pgmq.metrics(?)";
        return jdbcTemplate.queryForObject(sql, new MetricsResultRowMapper(), queue);
    }

    /**
     * Return metrics for all queues.
     *
     * @return queue metrics
     */
    public List<PgmqMetrics> getMetricsAll() {
        String sql = "select * from pgmq.metrics_all()";
        return jdbcTemplate.query(sql, new MetricsResultRowMapper());
    }

    /**
     * List queue metadata from {@code pgmq.meta}.
     *
     * @return queue metadata
     */
    public List<PgmqQueue> listQueues() {
        String sql = "select * from pgmq.list_queues()";
        return jdbcTemplate.query(sql, new QueueRecordRowMapper());
    }

    /**
     * Permanently delete all messages from a queue.
     *
     * @param queue the queue name
     * @return deleted message count
     */
    public long purgeQueue(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.purge_queue(?)";
        return jdbcTemplate.queryForObject(sql, Long.class, queue);
    }

    /**
     * Pop one visible message, deleting it from the queue.
     *
     * <p>This is equivalent to {@link #pop(String, int)} with {@code qty = 1}.</p>
     *
     * @param queue the queue name
     * @return popped messages
     */
    public List<PgmqMessage> pop(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select * from pgmq.pop(?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue);
    }

    /**
     * Pop visible messages, deleting them from the queue.
     *
     * @param queue the queue name
     * @param qty maximum number of messages to pop
     * @return popped messages
     */
    public List<PgmqMessage> pop(@NonNull String queue, int qty) {
        validateQueueNameArgument(queue);
        validateQuantity(qty);
        String sql = "select * from pgmq.pop(?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, qty);
    }

    /**
     * Set one message visibility timeout with a relative duration.
     *
     * @param queue the queue name
     * @param msgId the message ID
     * @param vt seconds from now
     * @return updated message
     */
    public PgmqMessage setVt(@NonNull String queue, long msgId, int vt) {
        validateQueueNameArgument(queue);
        validateMessageId(msgId);
        validateVisibilityTimeout(vt);
        String sql = "select * from pgmq.set_vt(?, ?, ?)";
        return jdbcTemplate.queryForObject(sql, new MessageRecordMapper(), queue, msgId, vt);
    }

    /**
     * Set one message visibility timeout with an absolute timestamp.
     *
     * @param queue the queue name
     * @param msgId the message ID
     * @param vt visibility timestamp
     * @return updated message
     */
    public PgmqMessage setVt(@NonNull String queue, long msgId, @NonNull OffsetDateTime vt) {
        validateQueueNameArgument(queue);
        validateMessageId(msgId);
        Assert.notNull(vt, "Visibility timestamp must not be null");
        String sql = "select * from pgmq.set_vt(?, ?, ?)";
        return jdbcTemplate.queryForObject(sql, new MessageRecordMapper(), queue, msgId, vt);
    }

    /**
     * Set multiple message visibility timeouts with a relative duration.
     *
     * @param queue the queue name
     * @param msgIds the message IDs
     * @param vt seconds from now
     * @return updated messages
     */
    public List<PgmqMessage> setVt(@NonNull String queue, @NonNull List<Long> msgIds, int vt) {
        validateQueueNameArgument(queue);
        validateMessageIds(msgIds);
        validateVisibilityTimeout(vt);
        String sql = "select * from pgmq.set_vt(?, ?::bigint[], ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, msgIds.toArray(new Long[0]), vt);
    }

    /**
     * Set multiple message visibility timeouts with an absolute timestamp.
     *
     * @param queue the queue name
     * @param msgIds the message IDs
     * @param vt visibility timestamp
     * @return updated messages
     */
    public List<PgmqMessage> setVt(@NonNull String queue, @NonNull List<Long> msgIds, @NonNull OffsetDateTime vt) {
        validateQueueNameArgument(queue);
        validateMessageIds(msgIds);
        Assert.notNull(vt, "Visibility timestamp must not be null");
        String sql = "select * from pgmq.set_vt(?, ?::bigint[], ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, msgIds.toArray(new Long[0]), vt);
    }

    /**
     * Drop a queue and its archive table.
     *
     * @param queue the queue name
     * @return whether the queue existed and was dropped
     */
    public Boolean dropQueue(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.drop_queue(?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue);
    }

    /**
     * Acquire the transaction-level advisory lock used by PGMQ queue administration.
     *
     * <p>Use a transaction on this template's data source when the lock must cover subsequent
     * operations. In auto-commit mode PostgreSQL releases it when this statement completes.</p>
     *
     * @param queue the queue name
     */
    public void acquireQueueLock(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.acquire_queue_lock(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Format a PGMQ table name using the SQL validator and lowercasing rules.
     *
     * @param queue the queue name
     * @param prefix table prefix, for example {@code q} or {@code a}
     * @return the unqualified table name
     */
    public String formatTableName(@NonNull String queue, @NonNull String prefix) {
        validateQueueNameArgument(queue);
        Assert.hasText(prefix, "Table prefix must not be empty");
        String sql = "select pgmq.format_table_name(?, ?)";
        return jdbcTemplate.queryForObject(sql, String.class, queue, prefix);
    }

    /**
     * Call the legacy queue-drop overload, whose partitioned flag is ignored by PGMQ 1.13.1.
     *
     * @param queue the queue name
     * @param partitioned legacy partitioned flag
     * @return the result returned by the legacy SQL overload
     * @deprecated use {@link #dropQueue(String)}; upstream removes this overload in PGMQ 2.0
     */
    @Deprecated(since = "2.1-1.13.1")
    public Boolean dropQueue(@NonNull String queue, boolean partitioned) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.drop_queue(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue, partitioned);
    }

    /**
     * Call the deprecated SQL archive-detach function.
     *
     * @param queue the queue name
     * @deprecated PGMQ 1.13.1 implements this as a no-op and removes it in PGMQ 2.0
     */
    @Deprecated(since = "2.1-1.13.1")
    public void detachArchive(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.detach_archive(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Validate a queue name through {@code pgmq.validate_queue_name()}.
     *
     * <p>The SQL function returns {@code void}; invalid names are reported by
     * PostgreSQL exceptions. The outer query returns {@link Boolean#TRUE} after
     * the validation call completes successfully.</p>
     *
     * @param queueName the queue name
     * @return {@link Boolean#TRUE} when validation succeeds
     */
    public Boolean validateQueueName(@NonNull String queueName) {
        validateQueueNameArgument(queueName);
        String sql = "select true from (select pgmq.validate_queue_name(?)) validated";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queueName);
    }

    /**
     * Return whether the {@code pg_partman} extension is installed.
     *
     * @return whether {@code pg_partman} is installed
     */
    public Boolean isPartmanInstalled() {
        String sql = "select exists (select 1 from pg_extension where extname = 'pg_partman')";
        return jdbcTemplate.queryForObject(sql, Boolean.class);
    }

    /**
     * Create a durable non-partitioned queue.
     *
     * @param queue the queue name
     */
    public void createNonPartitioned(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.create_non_partitioned(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Create an unlogged queue.
     *
     * <p>Unlogged queues trade durability for write throughput and can lose data
     * after a database crash.</p>
     *
     * @param queue the queue name
     */
    public void createUnlogged(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.create_unlogged(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Create a partitioned queue with custom partition and retention settings.
     *
     * <p>{@code partitionInterval} and {@code retentionInterval} accept numeric values
     * for message-count partitioning or PostgreSQL interval values for time-based partitioning.</p>
     *
     * <p>This method checks that the {@code pg_partman} extension is installed before
     * calling {@code pgmq.create_partitioned()}.</p>
     *
     * @param queue the queue name
     * @param partitionInterval the partition size as a numeric value or time interval
     * @param retentionInterval the retention size as a numeric value or time interval
     */
    public void createPartitioned(@NonNull String queue, @NonNull String partitionInterval, @NonNull String retentionInterval) {
        validateQueueNameArgument(queue);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        Assert.hasText(retentionInterval, "Retention interval must not be empty");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned queues");
        }

        String sql = "select pgmq.create_partitioned(?, ?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue, partitionInterval, retentionInterval);
    }

    /**
     * Create a partitioned queue with an explicit number of future partitions.
     *
     * @param queue the queue name
     * @param partitionInterval the partition size as a numeric value or time interval
     * @param retentionInterval the retention size as a numeric value or time interval
     * @param premake the number of partitions to create in advance (at least one)
     */
    public void createPartitioned(@NonNull String queue, @NonNull String partitionInterval,
                                  @NonNull String retentionInterval, int premake) {
        validateQueueNameArgument(queue);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        Assert.hasText(retentionInterval, "Retention interval must not be empty");
        Assert.isTrue(premake > 0, "Premake must be greater than 0");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned queues");
        }

        String sql = "select pgmq.create_partitioned(?, ?, ?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue, partitionInterval, retentionInterval, premake);
    }

    /**
     * Create a partitioned queue with default partition and retention settings.
     *
     * @param queue the queue name
     */
    public void createPartitioned(@NonNull String queue) {
        validateQueueNameArgument(queue);
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned queues");
        }

        String sql = "select pgmq.create_partitioned(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Create a partitioned queue, leaving retention and premake defaults to PostgreSQL.
     *
     * @param queue the queue name
     * @param partitionInterval partition size as a numeric value or time interval
     */
    public void createPartitioned(@NonNull String queue, @NonNull String partitionInterval) {
        validateQueueNameArgument(queue);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned queues");
        }
        String sql = "select pgmq.create_partitioned(?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue, partitionInterval);
    }

    /**
     * Create a standard durable queue.
     *
     * @param queue the queue name
     */
    public void create(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.create(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Convert an existing archive table to a partitioned archive.
     *
     * <p>This method checks that the {@code pg_partman} extension is installed before
     * calling {@code pgmq.convert_archive_partitioned()}.</p>
     *
     * @param tableName the queue name whose archive is converted
     * @param partitionInterval the partition size as a numeric value or time interval
     * @param retentionInterval the retention size as a numeric value or time interval
     * @param leadingPartition the number of partitions to create in advance
     */
    public void convertArchivePartitioned(@NonNull String tableName, @NonNull String partitionInterval, @NonNull String retentionInterval, int leadingPartition) {
        validateQueueNameArgument(tableName);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        Assert.hasText(retentionInterval, "Retention interval must not be empty");
        Assert.isTrue(leadingPartition > 0, "Leading partition must be greater than 0");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned archives");
        }

        String sql = "select pgmq.convert_archive_partitioned(?, ?, ?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, tableName, partitionInterval, retentionInterval, leadingPartition);
    }

    /**
     * Convert an existing archive table with default partition settings.
     *
     * @param tableName the queue name whose archive is converted
     */
    public void convertArchivePartitioned(@NonNull String tableName) {
        validateQueueNameArgument(tableName);
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned archives");
        }

        String sql = "select pgmq.convert_archive_partitioned(?)";
        jdbcTemplate.queryForObject(sql, Object.class, tableName);
    }

    /**
     * Convert an archive, leaving retention and leading-partition defaults to PostgreSQL.
     *
     * @param tableName the queue name whose archive is converted
     * @param partitionInterval partition size as a numeric value or time interval
     */
    public void convertArchivePartitioned(@NonNull String tableName, @NonNull String partitionInterval) {
        validateQueueNameArgument(tableName);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned archives");
        }
        String sql = "select pgmq.convert_archive_partitioned(?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, tableName, partitionInterval);
    }

    /**
     * Convert an archive, leaving the leading-partition default to PostgreSQL.
     *
     * @param tableName the queue name whose archive is converted
     * @param partitionInterval partition size as a numeric value or time interval
     * @param retentionInterval retention size as a numeric value or time interval
     */
    public void convertArchivePartitioned(@NonNull String tableName, @NonNull String partitionInterval,
            @NonNull String retentionInterval) {
        validateQueueNameArgument(tableName);
        Assert.hasText(partitionInterval, "Partition interval must not be empty");
        Assert.hasText(retentionInterval, "Retention interval must not be empty");
        if (!Boolean.TRUE.equals(isPartmanInstalled())) {
            throw new PgmqException("pg_partman extension is required for partitioned archives");
        }
        String sql = "select pgmq.convert_archive_partitioned(?, ?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, tableName, partitionInterval, retentionInterval);
    }

    /**
     * Create the FIFO helper index for one queue.
     *
     * @param queue the queue name
     */
    public void createFifoIndex(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.create_fifo_index(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Create FIFO helper indexes for all queues.
     */
    public void createFifoIndexesAll() {
        String sql = "select pgmq.create_fifo_indexes_all()";
        jdbcTemplate.queryForObject(sql, Object.class);
    }

    /**
     * Enable insert notifications for a queue.
     *
     * @param queue the queue name
     * @param throttleIntervalMs minimum milliseconds between notifications
     */
    public void enableNotifyInsert(@NonNull String queue, int throttleIntervalMs) {
        validateQueueNameArgument(queue);
        Assert.isTrue(throttleIntervalMs >= 0, "Throttle interval must be greater than or equal to 0");
        String sql = "select pgmq.enable_notify_insert(?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue, throttleIntervalMs);
    }

    /**
     * Enable insert notifications with PGMQ's default throttle interval.
     *
     * @param queue the queue name
     */
    public void enableNotifyInsert(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.enable_notify_insert(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * Disable insert notifications for a queue.
     *
     * @param queue the queue name
     */
    public void disableNotifyInsert(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select pgmq.disable_notify_insert(?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue);
    }

    /**
     * List queues that have insert notifications enabled.
     *
     * @return notification throttle settings
     */
    public List<NotifyInsertThrottle> listNotifyInsertThrottles() {
        String sql = "select * from pgmq.list_notify_insert_throttles()";
        return jdbcTemplate.query(sql, new NotifyInsertThrottleRowMapper());
    }

    /**
     * Update notification throttle settings for a queue.
     *
     * @param queue the queue name
     * @param throttleIntervalMs minimum milliseconds between notifications
     */
    public void updateNotifyInsert(@NonNull String queue, int throttleIntervalMs) {
        validateQueueNameArgument(queue);
        Assert.isTrue(throttleIntervalMs >= 0, "Throttle interval must be greater than or equal to 0");
        String sql = "select pgmq.update_notify_insert(?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, queue, throttleIntervalMs);
    }

    /**
     * Validate a topic routing key.
     *
     * @param routingKey the routing key
     * @return whether the routing key is valid
     */
    public Boolean validateRoutingKey(@NonNull String routingKey) {
        validateRoutingKeyArgument(routingKey);
        String sql = "select pgmq.validate_routing_key(?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, routingKey);
    }

    /**
     * Validate a topic binding pattern.
     *
     * @param pattern the topic binding pattern
     * @return whether the pattern is valid
     */
    public Boolean validateTopicPattern(@NonNull String pattern) {
        validateTopicPatternArgument(pattern);
        String sql = "select pgmq.validate_topic_pattern(?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, pattern);
    }

    /**
     * Bind a topic pattern to a queue.
     *
     * @param pattern the topic pattern
     * @param queue the queue name
     */
    public void bindTopic(@NonNull String pattern, @NonNull String queue) {
        validateTopicPatternArgument(pattern);
        validateQueueNameArgument(queue);
        String sql = "select pgmq.bind_topic(?, ?)";
        jdbcTemplate.queryForObject(sql, Object.class, pattern, queue);
    }

    /**
     * Remove a topic binding.
     *
     * @param pattern the topic pattern
     * @param queue the queue name
     * @return whether a binding was removed
     */
    public Boolean unbindTopic(@NonNull String pattern, @NonNull String queue) {
        validateTopicPatternArgument(pattern);
        validateQueueNameArgument(queue);
        String sql = "select pgmq.unbind_topic(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, pattern, queue);
    }

    /**
     * Return topic bindings that match a routing key.
     *
     * @param routingKey the routing key
     * @return matching routes
     */
    public List<TopicRoute> testRouting(@NonNull String routingKey) {
        validateRoutingKeyArgument(routingKey);
        String sql = "select * from pgmq.test_routing(?)";
        return jdbcTemplate.query(sql, new TopicRouteRowMapper(), routingKey);
    }

    /**
     * Send one topic message to queues matching the routing key.
     *
     * @param routingKey the routing key
     * @param msg message payload
     * @return number of matched queues
     */
    public int sendTopic(@NonNull String routingKey, @NonNull Object msg) {
        return sendTopic(routingKey, msg, Map.of(), 0);
    }

    /**
     * Send one topic message with a relative delay.
     *
     * @param routingKey the routing key
     * @param msg message payload
     * @param delay delay in seconds before visibility
     * @return number of matched queues
     */
    public int sendTopic(@NonNull String routingKey, @NonNull Object msg, int delay) {
        return sendTopic(routingKey, msg, Map.of(), delay);
    }

    /**
     * Send one topic message with custom headers.
     *
     * @param routingKey the routing key
     * @param msg message payload
     * @param headers custom headers
     * @return number of matched queues
     */
    public int sendTopic(@NonNull String routingKey, @NonNull Object msg, @NonNull Map<String, ?> headers) {
        return sendTopic(routingKey, msg, headers, 0);
    }

    /**
     * Send one topic message with custom headers and a relative delay.
     *
     * <p>The supplied headers are copied and enriched with {@link #TYPE_HEADER};
     * the caller's map is not mutated.</p>
     *
     * @param routingKey the routing key
     * @param msg message payload
     * @param headers custom headers
     * @param delay delay in seconds before visibility
     * @return number of matched queues
     */
    public int sendTopic(@NonNull String routingKey, @NonNull Object msg, @NonNull Map<String, ?> headers, int delay) {
        validateRoutingKeyArgument(routingKey);
        validateMessage(msg);
        validateHeaders(headers);
        validateDelay(delay);
        Map<String, Object> messageHeaders = messageHeaders(msg, headers);

        String sql = "select pgmq.send_topic(?, ?::jsonb, ?::jsonb, ?)";
        Integer matchedQueues = jdbcTemplate.queryForObject(sql, Integer.class, routingKey, toJson(msg), toJson(messageHeaders), delay);
        return matchedQueues == null ? 0 : matchedQueues;
    }

    /**
     * List all topic bindings.
     *
     * @return topic bindings
     */
    public List<TopicBinding> listTopicBindings() {
        String sql = "select * from pgmq.list_topic_bindings()";
        return jdbcTemplate.query(sql, new TopicBindingRowMapper());
    }

    /**
     * List topic bindings for one queue.
     *
     * @param queue the queue name
     * @return topic bindings
     */
    public List<TopicBinding> listTopicBindings(@NonNull String queue) {
        validateQueueNameArgument(queue);
        String sql = "select * from pgmq.list_topic_bindings(?)";
        return jdbcTemplate.query(sql, new TopicBindingRowMapper(), queue);
    }

    /**
     * Send topic messages immediately with no custom headers.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull Object... msgs) {
        Assert.notNull(msgs, "Messages must not be null");
        return sendBatchTopic(routingKey, Arrays.stream(msgs).toList(), List.of());
    }

    /**
     * Send topic messages immediately with no custom headers.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs) {
        return sendBatchTopic(routingKey, msgs, List.of());
    }

    /**
     * Send topic messages immediately with one shared header map.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared headers
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs, @NonNull Map<String, ?> headers) {
        return sendBatchTopic(routingKey, msgs, Collections.singletonList(headers));
    }

    /**
     * Send a topic batch with shared headers and an integer delay.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared headers
     * @param delay delay in seconds
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs,
            @NonNull Map<String, ?> headers, int delay) {
        return sendBatchTopic(routingKey, msgs, Collections.singletonList(headers), delay);
    }

    /**
     * Send a topic batch with shared headers and an absolute visibility timestamp.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared headers
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs,
            @NonNull Map<String, ?> headers, @NonNull OffsetDateTime visibleAt) {
        return sendBatchTopic(routingKey, msgs, Collections.singletonList(headers), visibleAt);
    }

    /**
     * Send topic messages immediately with shared or per-message headers.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs, @NonNull List<? extends Map<String, ?>> headers) {
        return sendBatchTopic(routingKey, msgs, headers, 0);
    }

    /**
     * Send topic messages with a relative delay and no custom headers.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param delay delay in seconds before visibility
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs, int delay) {
        return sendBatchTopic(routingKey, msgs, List.of(), delay);
    }

    /**
     * Send topic messages with an absolute visibility timestamp and no custom headers.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs with queue names
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs,
            @NonNull OffsetDateTime visibleAt) {
        return sendBatchTopic(routingKey, msgs, List.of(), visibleAt);
    }

    /**
     * Send topic messages with shared or per-message headers and a relative delay.
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @param delay delay in seconds before visibility
     * @return inserted message IDs with queue names
     * @throws PgmqException if {@code headers} contains more than one element and its size does not match {@code msgs}
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs, @NonNull List<? extends Map<String, ?>> headers, int delay) {
        validateDelay(delay);
        return sendBatchTopicInternal(routingKey, msgs, headers, delay);
    }

    /**
     * Send topic messages with headers and an absolute visibility timestamp.
     *
     * <p>Passing zero or one header map applies shared headers to all messages.
     * Passing multiple header maps requires the header list size to match the
     * message list size.</p>
     *
     * @param routingKey the routing key
     * @param msgs message payloads
     * @param headers shared or per-message headers
     * @param visibleAt timestamp when messages become visible
     * @return inserted message IDs with queue names
     * @throws PgmqException if {@code headers} contains more than one element and its size does not match {@code msgs}
     */
    public List<TopicBatchResult> sendBatchTopic(@NonNull String routingKey, @NonNull List<?> msgs,
            @NonNull List<? extends Map<String, ?>> headers, @NonNull OffsetDateTime visibleAt) {
        Assert.notNull(visibleAt, "Visibility timestamp must not be null");
        return sendBatchTopicInternal(routingKey, msgs, headers, visibleAt);
    }

    private List<TopicBatchResult> sendBatchTopicInternal(String routingKey, List<?> msgs,
            List<? extends Map<String, ?>> headers, Object visibilityValue) {
        validateRoutingKeyArgument(routingKey);
        validateMessages(msgs);
        validateHeaderList(headers);
        List<Map<String, Object>> messageHeaders = messageHeaders(msgs, headers);

        String sql = "select * from pgmq.send_batch_topic(?, ?::jsonb[], ?::jsonb[], ?)";
        return jdbcTemplate.query(sql, new TopicBatchResultRowMapper(),
                routingKey, toJsonArray(msgs), toJsonArray(messageHeaders), visibilityValue);
    }

    /**
     * Convert a message payload for a listener argument.
     *
     * @param message the message record
     * @param targetType the target Java type
     * @return converted payload
     */
    public Object convertMessagePayload(@NonNull PgmqMessage message, @NonNull Type targetType) {
        return convertPayload(message.payload(), targetType);
    }

    /**
     * Convert a message payload to a concrete class.
     *
     * @param message the message record
     * @param targetType the target Java type
     * @param <T> the target type
     * @return converted payload
     */
    public <T> T convertMessagePayload(@NonNull PgmqMessage message, @NonNull Class<T> targetType) {
        return targetType.cast(convertMessagePayload(message, (Type) targetType));
    }

    /**
     * Convert an arbitrary payload object to the requested Java type.
     *
     * @param payload the payload object
     * @param targetType the target Java type
     * @return converted payload
     */
    public Object convertPayload(Object payload, @NonNull Type targetType) {
        Assert.notNull(targetType, "Target type must not be null");
        if (payload == null) {
            return null;
        }

        JavaType javaType = objectMapper.getTypeFactory().constructType(targetType);
        Class<?> rawClass = javaType.getRawClass();
        if (!javaType.hasGenericTypes() && rawClass.isInstance(payload)) {
            return payload;
        }

        try {
            return objectMapper.convertValue(payload, javaType);
        } catch (IllegalArgumentException e) {
            throw new PgmqException("failed to convert message payload to type: " + javaType.toCanonical(), e);
        }
    }

    /**
     * Validate queue names before passing them to dynamic SQL functions.
     */
    private static void validateQueueNameArgument(String queueName) {
        Assert.hasText(queueName, "Queue name must not be empty");
    }

    /** Validate the arguments shared by all message reads. */
    private static void validateReadArguments(String queue, int vt, int qty) {
        validateQueueNameArgument(queue);
        validateVisibilityTimeout(vt);
        validateQuantity(qty);
    }

    /**
     * Validate one message id.
     */
    private static void validateMessageId(long msgId) {
        Assert.isTrue(msgId > 0, "Message id must be greater than 0");
    }

    /**
     * Validate message id batch arguments.
     */
    private static void validateMessageIds(List<Long> msgIds) {
        Assert.notEmpty(msgIds, "Message ids must not be empty");
        for (Long msgId : msgIds) {
            Assert.notNull(msgId, "Message id must not be null");
            validateMessageId(msgId);
        }
    }

    /**
     * Validate one payload.
     */
    private static void validateMessage(Object msg) {
        Assert.notNull(msg, "Message payload must not be null");
    }

    /**
     * Validate message batch arguments.
     */
    private static void validateMessages(List<?> msgs) {
        Assert.notEmpty(msgs, "Messages must not be empty");
        for (Object msg : msgs) {
            validateMessage(msg);
        }
    }

    /**
     * Validate one header map.
     */
    private static void validateHeaders(Map<String, ?> headers) {
        Assert.notNull(headers, "Headers must not be null");
        for (String headerName : headers.keySet()) {
            Assert.hasText(headerName, "Header name must not be empty");
        }
    }

    /**
     * Validate batch header arguments.
     */
    private static void validateHeaderList(List<? extends Map<String, ?>> headers) {
        Assert.notNull(headers, "Headers must not be null");
        for (Map<String, ?> header : headers) {
            validateHeaders(header);
        }
    }

    /**
     * Validate a visibility timeout in seconds.
     */
    private static void validateVisibilityTimeout(int vt) {
        Assert.isTrue(vt >= 0, "Visibility timeout must be greater than or equal to 0");
    }

    /**
     * Validate a read or pop quantity.
     */
    private static void validateQuantity(int qty) {
        Assert.isTrue(qty > 0, "Quantity must be greater than 0");
    }

    /**
     * Validate a relative delay in seconds.
     */
    private static void validateDelay(int delay) {
        Assert.isTrue(delay >= 0, "Delay must be greater than or equal to 0");
    }

    /**
     * Validate a long-poll timeout.
     */
    private static void validateMaxPollSeconds(int maxPollSeconds) {
        Assert.isTrue(maxPollSeconds >= 0, "Max poll seconds must be greater than or equal to 0");
    }

    /**
     * Validate the sleep interval between poll attempts.
     */
    private static void validatePollInterval(int pollIntervalMs) {
        Assert.isTrue(pollIntervalMs > 0, "Poll interval must be greater than 0");
    }

    /**
     * Validate a topic routing key before SQL validation.
     */
    private static void validateRoutingKeyArgument(String routingKey) {
        Assert.hasText(routingKey, "Routing key must not be empty");
    }

    /**
     * Validate a topic binding pattern before SQL validation.
     */
    private static void validateTopicPatternArgument(String pattern) {
        Assert.hasText(pattern, "Topic pattern must not be empty");
    }

    /** Maps {@code pgmq.queue_record} rows. */
    private class QueueRecordRowMapper implements RowMapper<PgmqQueue> {
        /**
         * Map one queue metadata row.
         */
        @Override
        public PgmqQueue mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new PgmqQueue(
                    rs.getString("queue_name"),
                    rs.getBoolean("is_partitioned"),
                    rs.getBoolean("is_unlogged"),
                    rs.getObject("created_at", OffsetDateTime.class)
            );
        }
    }

    /** Maps {@code pgmq.list_topic_bindings()} rows. */
    private class TopicBindingRowMapper implements RowMapper<TopicBinding> {
        /**
         * Map one topic binding row.
         */
        @Override
        public TopicBinding mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new TopicBinding(
                    rs.getString("pattern"),
                    rs.getString("queue_name"),
                    rs.getObject("bound_at", OffsetDateTime.class),
                    rs.getString("compiled_regex")
            );
        }
    }

    /** Maps {@code pgmq.send_batch_topic()} rows. */
    private class TopicBatchResultRowMapper implements RowMapper<TopicBatchResult> {
        /**
         * Map one topic batch delivery row.
         */
        @Override
        public TopicBatchResult mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new TopicBatchResult(
                    rs.getString("queue_name"),
                    rs.getLong("msg_id")
            );
        }
    }

    /** Maps {@code pgmq.test_routing()} rows. */
    private class TopicRouteRowMapper implements RowMapper<TopicRoute> {
        /**
         * Map one topic route row.
         */
        @Override
        public TopicRoute mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new TopicRoute(
                    rs.getString("pattern"),
                    rs.getString("queue_name"),
                    rs.getString("compiled_regex")
            );
        }
    }

    /** Maps {@code pgmq.list_notify_insert_throttles()} rows. */
    private class NotifyInsertThrottleRowMapper implements RowMapper<NotifyInsertThrottle> {
        /**
         * Map one notification throttle row.
         */
        @Override
        public NotifyInsertThrottle mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new NotifyInsertThrottle(
                    rs.getString("queue_name"),
                    rs.getInt("throttle_interval_ms"),
                    rs.getObject("last_notified_at", OffsetDateTime.class)
            );
        }
    }

    /** Maps {@code pgmq.message_record} rows and deserializes payloads. */
    private class MessageRecordMapper implements RowMapper<PgmqMessage> {

        /**
         * Map one message row and deserialize headers before payload conversion.
         */
        @Override
        public PgmqMessage mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String, Object> headers = headers(rs);
            Object payload = payload(rs, headers);

            return new PgmqMessage(
                    rs.getLong("msg_id"),
                    rs.getInt("read_ct"),
                    rs.getObject("enqueued_at", OffsetDateTime.class),
                    rs.getObject("last_read_at", OffsetDateTime.class),
                    rs.getObject("vt", OffsetDateTime.class),
                    payload,
                    headers
            );
        }

        /**
         * Deserialize the JSON headers column.
         */
        private Map<String, Object> headers(ResultSet rs) throws SQLException {
            String headers = rs.getString("headers");
            if (headers != null) {
                try {
                    return objectMapper.readValue(headers, new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                    throw new PgmqException("failed to deserialize headers for Map<String, Object>", e);
                }
            }
            return Map.of();
        }

        /**
         * Deserialize the message JSON using type information when available.
         */
        private Object payload(ResultSet rs, Map<String, Object> headers) throws SQLException {
            String payload = rs.getString("message");
            if (payload != null && headers != null && headers.get(TYPE_HEADER) instanceof String typeName) {
                try {
                    JavaType javaType = objectMapper.getTypeFactory().constructFromCanonical(typeName);
                    return objectMapper.readValue(payload, javaType);
                } catch (Exception ignored) {
                    // Producer type hints can be stale or incompatible. Keep the JSON value so
                    // the listener's parameter conversion can fail at the per-message boundary.
                }
            }

            if (payload != null) {
                try {
                    return objectMapper.readValue(payload, Object.class);
                } catch (Exception e) {
                    throw new PgmqException("failed to deserialize message", e);
                }
            }

            return null;
        }
    }

    /** Maps {@code pgmq.metrics_result} rows. */
    private class MetricsResultRowMapper implements RowMapper<PgmqMetrics> {
        /**
         * Map one queue metrics row.
         */
        @Override
        public PgmqMetrics mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new PgmqMetrics(
                    rs.getString("queue_name"),
                    rs.getLong("queue_length"),
                    rs.getObject("newest_msg_age_sec", Integer.class),
                    rs.getObject("oldest_msg_age_sec", Integer.class),
                    rs.getLong("total_messages"),
                    rs.getObject("scrape_time", OffsetDateTime.class),
                    rs.getLong("queue_visible_length"),
                    rs.getObject("default_partition_length", Long.class)
            );
        }
    }

    /**
     * Serialize an object as JSON.
     */
    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new PgmqException("failed to convert object to json: " + obj, e);
        }
    }

    /**
     * Serialize objects as PostgreSQL jsonb array elements.
     */
    private String[] toJsonArray(List<?> objs) {
        return objs.stream().map(this::toJson).toArray(String[]::new);
    }

    /**
     * Copy caller headers and add the payload type header.
     */
    private Map<String, Object> messageHeaders(Object msg, Map<String, ?> headers) {
        Map<String, Object> messageHeaders = new HashMap<>(headers);
        messageHeaders.put(TYPE_HEADER, objectMapper.constructType(msg.getClass()).toCanonical());
        return messageHeaders;
    }

    /**
     * Build one enriched header map per batch message.
     */
    private List<Map<String, Object>> messageHeaders(List<?> msgs, List<? extends Map<String, ?>> headers) {
        List<? extends Map<String, ?>> expandedHeaders = expandHeaders(msgs, headers);
        return IntStream.range(0, msgs.size())
                .mapToObj(index -> messageHeaders(msgs.get(index), expandedHeaders.get(index)))
                .toList();
    }

    /**
     * Expand empty or single-entry headers into one logical header map per message.
     */
    private List<? extends Map<String, ?>> expandHeaders(List<?> msgs, List<? extends Map<String, ?>> headers) {
        if (headers.size() <= 1) {
            Map<String, ?> sharedHeaders = headers.isEmpty() ? Map.of() : headers.get(0);
            return Collections.nCopies(msgs.size(), sharedHeaders);
        }

        if (msgs.size() != headers.size()) {
            throw new PgmqException("msgs and headers size mismatch");
        }

        return headers;
    }

}
