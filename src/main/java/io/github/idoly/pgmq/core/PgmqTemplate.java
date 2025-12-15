package io.github.idoly.pgmq.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.PgmqException;

/**
 * Core template for PGMQ message operations.
 *
 * <p>This class provides methods to send messages to a PGMQ queue with optional headers
 * and delayed visibility. It relies on {@link JdbcTemplate} for database operations
 * and {@link ObjectMapper} for JSON serialization.</p>
 *
 * <p>Messages sent through this template will have a "_type" header automatically added,
 * indicating the Java type of the payload for type-aware deserialization.</p>
 * */
public class PgmqTemplate {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Constructs a PgmqTemplate with the given {@link JdbcTemplate} and {@link ObjectMapper}.
     *
     * @param jdbcTemplate the JDBC template for database access
     * @param objectMapper the ObjectMapper for JSON serialization/deserialization
     */
    public PgmqTemplate(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Sends a message to the specified queue with no headers and no delay.
     *
     * @param queue the target queue name
     * @param msg   the message payload
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg) {
        return send(queue, msg, new HashMap<>(), 0);
    }

    /**
     * Sends a message to the specified queue with custom headers and no delay.
     *
     * @param queue   the target queue name
     * @param msg     the message payload
     * @param headers the message headers
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String, Object> headers) {
        return send(queue, msg, headers, 0);
    }

    /**
     * Sends a message to the specified queue with a delay in seconds and no custom headers.
     *
     * @param queue the target queue name
     * @param msg   the message payload
     * @param delay delay in seconds before the message becomes visible
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg, int delay) {
        return send(queue, msg, new HashMap<>(), delay);
    }

    /**
     * Sends a message to the specified queue with a {@link OffsetDateTime} delay and no custom headers.
     *
     * @param queue the target queue name
     * @param msg   the message payload
     * @param delay the timestamp after which the message becomes visible
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull OffsetDateTime delay) {
        return send(queue, msg, new HashMap<>(), delay);
    }

    /**
     * Sends a message to the specified queue with headers and a delay in seconds.
     *
     * @param queue   the target queue name
     * @param msg     the message payload
     * @param headers the message headers
     * @param delay   delay in seconds before the message becomes visible
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String,Object> headers, int delay) {
        return send(queue, msg, headers, OffsetDateTime.now().plusSeconds(delay));
    }

    /**
     * Sends a single message to the specified queue with headers and a visibility timestamp.
     *
     * <p>This method automatically inserts a "_type" header representing the canonical
     * Java type of the message object, allowing type-aware deserialization.</p>
     *
     * @param queue   the target queue name
     * @param msg     the message payload
     * @param headers the message headers; modified to include "_type"
     * @param delay   the timestamp after which the message becomes visible
     * @return the ID of the inserted message
     */
    public long send(@NonNull String queue, @NonNull Object msg, @NonNull Map<String,Object> headers, @NonNull OffsetDateTime delay) {
        headers.put("_type", objectMapper.constructType(msg.getClass()).toCanonical());

        String sql = "select * from pgmq.send(?, ?::jsonb, ?::jsonb, ?)";
        return jdbcTemplate.queryForObject(sql, Long.class, queue, toJson(msg), toJson(headers), delay);
    }

    /**
     * Sends multiple messages to the specified queue with variable headers and delay.
     *
     * @param queue the queue name where messages will be sent
     * @param msgs  a variable number of message payloads
     * @return a list of message IDs generated for the sent messages
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull Object... msgs) {
        return sendBatch(queue, Arrays.asList(msgs), List.of());
    }

    /**
     * Sends a list of messages to the specified queue with no headers and no delay.
     *
     * @param queue the queue name where messages will be sent
     * @param msgs  the list of message payloads
     * @return a list of message IDs generated for the sent messages
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs) {
        return sendBatch(queue, msgs, List.of());
    }

    /**
     * Sends a list of messages with a single shared header map and no delay.
     *
     * @param queue   the queue name
     * @param msgs    the list of message payloads
     * @param headers a single header map to apply to all messages
     * @return list of message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, @NonNull Map<String,Object> headers) {
        return sendBatch(queue, msgs, List.of(headers));
    }

    /**
     * Sends a list of messages with individual header maps and no delay.
     *
     * @param queue   the queue name
     * @param msgs    the list of message payloads
     * @param headers list of header maps, one per message
     * @return list of message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, @NonNull List<Map<String,Object>> headers) {
        return sendBatch(queue, msgs, headers, 0);
    }

    /**
     * Sends a list of messages with a delay in seconds and no headers.
     *
     * @param queue the queue name
     * @param msgs  the list of message payloads
     * @param delay delay in seconds before messages become visible
     * @return list of message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, int delay) {
        return sendBatch(queue, msgs, List.of(), 0);
    }

    /**
     * Sends a list of messages with a {@link OffsetDateTime} delay and no headers.
     *
     * @param queue the queue name
     * @param msgs  the list of message payloads
     * @param delay the timestamp after which messages become visible
     * @return list of message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, @NonNull OffsetDateTime delay) {
        return sendBatch(queue, msgs, List.of(), delay);
    }

    /**
     * Sends a list of messages with individual headers and a delay in seconds.
     *
     * @param queue   the queue name
     * @param msgs    the list of message payloads
     * @param headers list of header maps, one per message
     * @param delay   delay in seconds before messages become visible
     * @return list of message IDs
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, @NonNull List<Map<String,Object>> headers, int delay) {
        return sendBatch(queue, msgs, headers, OffsetDateTime.now().plusSeconds(delay));
    }

    /**
     * Send multiple messages to a queue with individual headers and a shared delay.
     *
     * <p>Each message will be assigned its own header map, and this method will
     * automatically insert a "_type" field into each header, representing the
     * canonical Java type of the corresponding message object. The size of
     * {@code msgs} and {@code headers} must match.
     *
     * @param queue   the queue name
     * @param msgs    the list of message payloads
     * @param headers the list of header maps, one per message
     * @param delay   the visibility delay for all messages
     * @return a list of message IDs returned by the server, in the same order
     * @throws PgmqException if the size of {@code msgs} and {@code headers} does not match
     */
    public List<Long> sendBatch(@NonNull String queue, @NonNull List<Object> msgs, List<Map<String,Object>> headers, @NonNull OffsetDateTime delay) {
        if (msgs.isEmpty()) return Collections.emptyList();

        if (headers.size() <= 1) {
            Map<String, Object> temp = headers.isEmpty() ? Collections.emptyMap() : headers.get(0);
            headers = IntStream.range(0, msgs.size()).mapToObj(i -> (Map<String, Object>) new HashMap<>(temp)).toList();
        }

        if (msgs.size() != headers.size()) {
            throw new PgmqException("msgs and headers size mismatch");
        }

        for (int i = 0; i < msgs.size(); i++) {
            headers.get(i).put("_type", objectMapper.constructType(msgs.get(i).getClass()).toCanonical());
        }
        
        String sql = "select * from pgmq.send_batch(?, ?::jsonb[], ?::jsonb[], ?)";
        return jdbcTemplate.queryForList(sql, Long.class, queue, toJsonArray(msgs), toJsonArray(headers), delay);
    }

    /**
     * Reads messages from the specified queue with a default JSON filter "{}".
     *
     * @param queue the name of the queue
     * @param vt visibility timeout in seconds
     * @param qty number of messages to read
     * @return list of messages retrieved from the queue
     */
    public List<Message> read(@NonNull String queue, int vt, int qty) {
        return read(queue, vt, qty, "{}");
    }

    /**
     * Reads messages from the specified queue using a JSON-based conditional filter.
     *
     * <p>The {@code conditional} parameter allows filtering messages based on their JSON content.
     * For example, you can provide '{"key":"value"}' to only retrieve messages with that key-value pair.
     * </p>
     *
     * @param queue       the name of the queue
     * @param vt          visibility timeout in seconds
     * @param qty         number of messages to read
     * @param conditional JSON filter string, e.g., '{"key":"value"}'; default is "{}"
     * @return list of messages matching the filter criteria
     */
    public List<Message> read(@NonNull String queue, int vt, int qty, @NonNull String conditional) {
        String sql = "select * from pgmq.read(?, ?, ?, ?::jsonb)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, conditional);
    }

    /**
     * Reads messages from the specified queue using long-polling.
     *
     * <p>This method will attempt to read messages multiple times until either the specified
     * quantity is reached or the poll timeout is hit. It uses default conditional "{}".</p>
     *
     * @param queue the name of the queue
     * @param vt visibility timeout in seconds
     * @param qty number of messages to read
     * @return list of messages retrieved from the queue
     */
    public List<Message> readWithPoll(@NonNull String queue, int vt, int qty) {
        return readWithPoll(queue, vt, qty, 5, 100, "{}");
    }

    /**
     * Read messages with long-polling and options.
     *
     * @param queue           Name of the queue
     * @param vt              Visibility timeout in seconds
     * @param qty             Number of messages to read
     * @param maxPollSeconds  Maximum seconds to wait for new messages if queue is empty
     * @param pollIntervalMs  Milliseconds between internal poll attempts
     * @param conditional JSON filter, e.g., '{"key":"value"}'. Default '{}'
     * @return List of messages matching the filter
     */
    public List<Message> readWithPoll(@NonNull String queue, int vt, int qty, int maxPollSeconds, int pollIntervalMs, @NonNull String conditional) {
        String sql = "select * from pgmq.read_with_poll(?, ?, ?, ?, ?, ?::jsonb)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, vt, qty, maxPollSeconds, pollIntervalMs, conditional);
    }

    /**
     * Pop a single message from the specified queue.
     *
     * <p>This is a convenience method equivalent to {@link #pop(String, int)} with qty = 1.</p>
     *
     * @param queue the name of the queue
     * @return a list containing at most one message popped from the queue
     */
    public List<Message> pop(@NonNull String queue) {
        return pop(queue, 1);
    }

    /**
     * Pop multiple messages from the queue.
     *
     * @param queue Name of the queue
     * @param qty   Number of messages to pop
     * @return List of messages popped
     */
    public List<Message> pop(@NonNull String queue, int qty) {
        String sql = "select * from pgmq.pop(?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, qty);
    }

    /**
     * Delete a single message from the queue.
     *
     * @param queue Name of the queue
     * @param msgId ID of the message to delete
     * @return true if the message was deleted successfully, false otherwise
     */
    public boolean delete(@NonNull String queue, long msgId) {
        String sql = "select pgmq.delete(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue, msgId);
    }

    /**
     * Delete multiple messages from the queue.
     *
     * @param queue  Name of the queue
     * @param msgIds List of message IDs to delete
     * @return List of message IDs that were successfully deleted
     */
    public List<Long> delete(@NonNull String queue, @NonNull List<Long> msgIds) {
        String sql = "select * from pgmq.delete(?, ?)";
        return jdbcTemplate.queryForList(sql, Long.class, queue, msgIds.toArray(new Long[0]));
    }

    /**
     * Permanently delete all messages in a queue.
     *
     * @param queue Name of the queue
     * @return Number of messages that were deleted
     */
    public long purgeQueue(@NonNull String queue) {
        String sql = "select pgmq.purge_queue(?)";
        return jdbcTemplate.queryForObject(sql, Long.class, queue);
    }

    /**
     * Archive a single message from the queue.
     *
     * @param queue Name of the queue
     * @param msgId ID of the message to archive
     * @return true if successfully archived, false otherwise
     */
    public boolean archive(@NonNull String queue, long msgId) {
        String sql = "select pgmq.archive(?, ?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue, msgId);
    }

    /**
     * Archive multiple messages from the queue.
     *
     * @param queue  Name of the queue
     * @param msgIds List of message IDs to archive
     * @return List of message IDs that were successfully archived
     */
    public List<Long> archive(@NonNull String queue, @NonNull List<Long> msgIds) {
        String sql = "select * from pgmq.archive(?, ?)";
        return jdbcTemplate.queryForList(sql, Long.class, queue, msgIds.toArray(new Long[0]));
    }

    /**
     * Check if the pg_partman extension is installed in PostgreSQL.
     *
     * @return true if installed, false otherwise
     */
    public boolean isPartmanInstalled() {
        String sql = "select exists (select 1 from pg_extension where extname = 'pg_partman')";
        return jdbcTemplate.queryForObject(sql, Boolean.class);
    }

    /**
     * Create a standard (logged) PGMQ queue.
     *
     * The queue is durable and writes are persisted.
     *
     * @param queue The name of the queue (max 47 characters)
     */
    public void create(@NonNull String queue) {
        String sql = "SELECT pgmq.create(?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.execute();
            }

            return null;
        });
    }

    /**
     * Create a non-partitioned PGMQ queue.
     *
     * Useful when you want a simple queue without automatic partitioning.
     * The queue is durable.
     *
     * @param queue The name of the queue (max 47 characters)
     */
    public void createNonPartitioned(@NonNull String queue) {
        String sql = "SELECT pgmq.create_non_partitioned(?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.execute();
            }

            return null;
        });
    }

    /**
     * Create a partitioned PGMQ queue with custom partition and retention settings.
     *
     * Parameters:
     *   queue               The name of the queue (max 47 characters)
     *   partitionInterval   Partition size:
     *                       - Numeric value (e.g., "10000"): messages per partition
     *                       - Time interval (e.g., "1 day"): timestamp-based partitioning
     *   retentionInterval   How many messages or how long to retain old partitions:
     *                       - Same format as partitionInterval
     *
     * Example:
     *   createPartitioned("my_queue", "100000", "10000000")
     *   - Each partition contains 100,000 messages
     *   - Retain the latest 10,000,000 messages
     *   - Older partitions are automatically dropped
     *
     * Requires the `pg_partman` extension to be installed.
     *
     * @param queue               The name of the queue
     * @param partitionInterval   Partition size (numeric or time interval)
     * @param retentionInterval   Retention size (numeric or time interval)
     */
    public void createPartitioned(@NonNull String queue, @NonNull String partitionInterval, @NonNull String retentionInterval) {
        if (!isPartmanInstalled()) {
            throw new PgmqException("pg_partman extension is required for partitioned queues");
        }

        String sql = "SELECT pgmq.create_partitioned(?, ?, ?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.setString(2, partitionInterval);
                ps.setString(3, retentionInterval);
                ps.execute();
            }
            return null;
        });
    }

    /**
     * Create a partitioned queue.
     *
     * @param queue the queue name
     */
    public void createPartitioned(@NonNull String queue) {
        createPartitioned(queue, "10000", "10000000");
    }

    /**
     * Create an unlogged PGMQ queue.
     *
     * Unlogged queues are faster for writes but not durable.
     * Data is lost if the database crashes.
     *
     * Use for temporary queues or high-throughput scenarios where durability is not required.
     *
     * @param queue The name of the queue (max 47 characters)
     */
    public void createUnlogged(@NonNull String queue) {
        String sql = "select pgmq.create_unlogged(?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.execute();
            }
            return null;
        });
    }

    /**
     * Convert an existing non-partitioned archive table to a partitioned archive.
     *
     * Requires the pg_partman extension to be installed.
     *
     * @param tableName          The name of the archive table to convert
     * @param partitionInterval  Partition size (numeric or time interval, default "10000")
     * @param retentionInterval  Retention size (numeric or time interval, default "100000")
     * @param leadingPartition   Number of partitions to create in advance (default 10)
     */
    public void convertArchivePartitioned(@NonNull String tableName, @NonNull String partitionInterval, @NonNull String retentionInterval, int leadingPartition) {
        if (!isPartmanInstalled()) {
            throw new PgmqException("pg_partman extension is required for partitioned archives");
        }
        
        String sql = "select pgmq.convert_archive_partitioned(?, ?, ?, ?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableName);
                ps.setString(2, partitionInterval);
                ps.setString(3, retentionInterval);
                ps.setInt(4, leadingPartition);
                ps.execute();
            }
            return null;
        });
    }

    /**
     * Convert an archived partitioned table for further processing.
     *
     * @param tableName the table to convert
     */
    public void convertArchivePartitioned(@NonNull String tableName) {
        convertArchivePartitioned(tableName, "10000", "100000", 10);
    }

    /**
     * Delete a queue and its archive table.
     *
     * Automatically detects whether the queue is partitioned.
     *
     * @param queue The name of the queue to delete
     * @return true if the queue was successfully deleted, false otherwise
     */
    public boolean dropQueue(@NonNull String queue) {
        String sql = "select pgmq.drop_queue(?)";
        return jdbcTemplate.queryForObject(sql, Boolean.class, queue);
    }

    /**
     * Set the visibility timeout (VT) of a single message.
     *
     * @param queue   Queue name
     * @param msgId   ID of the message
     * @param vt      Duration in seconds from now to set the VT
     * @return The updated message record
     */
    public Message setVt(@NonNull String queue, long msgId, int vt) {
        String sql = "select * from pgmq.set_vt(?, ?, ?)";
        return jdbcTemplate.queryForObject(sql, new MessageRecordMapper(), queue, msgId, vt);
    }

    /**
     * Set the visibility timeout of multiple messages in batch.
     *
     * @param queue   Queue name
     * @param msgIds  List of message IDs
     * @param vt      Duration in seconds from now to set the VT
     * @return List of updated message records
     */
    public List<Message> setVt(@NonNull String queue, @NonNull List<Long> msgIds, int vt) {
        String sql = "select * from pgmq.set_vt(?, ?, ?)";
        return jdbcTemplate.query(sql, new MessageRecordMapper(), queue, msgIds.toArray(new Long[0]), vt);
    }

    /**
     * List all existing queues.
     *
     * @return List of queues with metadata (partitioned, unlogged, creation time)
     */
    public List<Queue> listQueues() {
        String sql = "select * from pgmq.list_queues()";
        return jdbcTemplate.query(sql, new QueueRecordRowMapper());
    }

    /**
     * Get metrics for a specific queue.
     *
     * @param queue Queue name
     * @return Metrics result
     */
    public Metrics getMetrics(@NonNull String queue) {
        String sql = "select * from pgmq.metrics(?)";
        return jdbcTemplate.queryForObject(sql, new MetricsResultRowMapper(), queue);
    }

    /**
     * Get metrics for all queues.
     *
     * @return List of metrics results
     */
    public List<Metrics> getMetricsAll() {
        String sql = "select * from pgmq.metrics_all()";
        return jdbcTemplate.query(sql, new MetricsResultRowMapper());
    }

    /**
     * Enable PostgreSQL NOTIFY triggers for a queue with optional throttling.
     *
     * @param queue             Queue name
     * @param throttleInterval  Minimum milliseconds between notifications (default 250, 0 to disable throttling)
     */
    public void enableNotifyInsert(@NonNull String queue, int throttleInterval) {
        String sql = "select pgmq.enable_notify_insert(?, ?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.setInt(2, throttleInterval);
                ps.execute();
            }

            return null;
        });
    }

    /**
     * Enable PostgreSQL NOTIFY triggers for the specified queue with default throttling.
     *
     * <p>This is equivalent to {@link #enableNotifyInsert(String, int)} with a default
     * throttle interval of 250 milliseconds.</p>
     *
     * @param queue the name of the queue
     */
    public void enableNotifyInsert(@NonNull String queue) {
        enableNotifyInsert(queue, 250);
    }

    /**
     * Disable PostgreSQL NOTIFY triggers for a queue.
     *
     * @param queue Queue name
     */
    public void disableNotifyInsert(@NonNull String queue) {
        String sql = "select pgmq.disable_notify_insert(?)";
        jdbcTemplate.execute((Connection conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, queue);
                ps.execute();
            }

            return null;
        });
    }

    /**
     * RowMapper implementation to map a ResultSet row to a {@link Queue} object.
     */
    private class QueueRecordRowMapper implements RowMapper<Queue> {
        @Override
        public Queue mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Queue(
                    rs.getString("queue_name"),
                    rs.getBoolean("is_partitioned"),
                    rs.getBoolean("is_unlogged"),
                    rs.getObject("created_at", OffsetDateTime.class)
            );
        }
    }

    /**
     * RowMapper implementation to map a ResultSet row to a {@link Message} object.
     * Handles JSON deserialization of headers and message content.
     */
    private class MessageRecordMapper implements RowMapper<Message> {

        @Override
        public Message mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String, Object> headers = headers(rs);
            Object message = message(rs, headers);

            return new Message(
                    rs.getLong("msg_id"),
                    rs.getInt("read_ct"),
                    rs.getObject("enqueued_at", OffsetDateTime.class),
                    rs.getObject("vt", OffsetDateTime.class),
                    message,
                    headers
            );
        }

        /**
         * Extracts and deserializes the headers column as a Map.
         *
         * @param rs the ResultSet
         * @return a Map of headers or null if the column is empty
         * @throws SQLException if reading from ResultSet fails
         * @throws PgmqException if JSON deserialization fails
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
            return null;
        }

        /**
         * Deserializes the message column based on the "_type" header.
         *
         * @param rs      the ResultSet
         * @param headers the deserialized headers
         * @return the message object or null if not available
         * @throws SQLException if reading from ResultSet fails
         * @throws PgmqException if JSON deserialization fails
         */
        private Object message(ResultSet rs, Map<String, Object> headers) throws SQLException {
            String message = rs.getString("message");
            if (message != null && headers != null && headers.containsKey("_type")) {
                String _type = (String) headers.get("_type");
                try {
                    JavaType javaType = objectMapper.getTypeFactory().constructFromCanonical(_type);
                    return objectMapper.readValue(message, javaType);
                } catch (Exception e) {
                    throw new PgmqException("failed to deserialize message for type: " + _type, e);
                }
            }
            return null;
        }
    }

    /**
     * RowMapper implementation to map a ResultSet row to a {@link Metrics} object.
     */
    private class MetricsResultRowMapper implements RowMapper<Metrics> {
        @Override
        public Metrics mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Metrics(
                    rs.getString("queue_name"),
                    rs.getLong("queue_length"),
                    rs.getInt("newest_msg_age_sec"),
                    rs.getInt("oldest_msg_age_sec"),
                    rs.getLong("total_messages"),
                    rs.getObject("scrape_time", OffsetDateTime.class),
                    rs.getLong("queue_visible_length")
            );
        }
    }

    /**
     * Converts an object to its JSON string representation.
     *
     * @param obj the object to serialize
     * @return JSON string representation of the object
     * @throws RuntimeException if serialization fails
     */
    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("failed to convert object to json: " + obj, e);
        }
    }

    /**
     * Converts a list of objects to a JSON string array.
     *
     * @param objs the list of objects
     * @return an array of JSON string representations
     */
    private String[] toJsonArray(List<?> objs) {
        return objs.stream().map(this::toJson).toArray(String[]::new);
    }

    /**
     * Returns the underlying JdbcTemplate used by this template.
     *
     * @return the JdbcTemplate instance
     */
    public JdbcTemplate getjdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * Returns the ObjectMapper used for JSON serialization/deserialization.
     *
     * @return the ObjectMapper instance
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

}
