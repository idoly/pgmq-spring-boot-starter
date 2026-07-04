package io.github.idoly.pgmq.config;

import java.util.Objects;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.template.PgmqTemplate;

/**
 * Configuration properties under the {@code pgmq} prefix.
 *
 * <p>Controls database initialization, bundled SQL script location, and default
 * options for annotation-driven listener containers.</p>
 */
@ConfigurationProperties(prefix = "pgmq")
public class PgmqProperties {

    /**
     * Whether to automatically initialize PGMQ on application startup.
     * Default is {@code false}.
     */
    private boolean autoInit = false;

    /**
     * Path to the SQL script used to initialize PGMQ if it is not available.
     * Default is {@code sql/pgmq.sql}.
     */
    private String sqlScript = "sql/pgmq.sql";

    /**
     * Listener defaults used by {@link io.github.idoly.pgmq.annotation.PgmqListener}.
     */
    private Listener listener = new Listener();

    public boolean isAutoInit() {
        return autoInit;
    }

    public void setAutoInit(boolean autoInit) {
        this.autoInit = autoInit;
    }

    public String getSqlScript() {
        return sqlScript;
    }

    public void setSqlScript(String sqlScript) {
        Assert.hasText(sqlScript, "SQL script must not be empty");
        this.sqlScript = sqlScript;
    }

    /**
     * Return listener defaults, creating them when configuration binding set the nested object to {@code null}.
     */
    public Listener getListener() {
        listener = Objects.requireNonNullElseGet(listener, Listener::new);
        return listener;
    }

    /**
     * Store listener defaults, replacing {@code null} with a default instance.
     */
    public void setListener(Listener listener) {
        this.listener = Objects.requireNonNullElseGet(listener, Listener::new);
    }

    /**
     * Default options for annotation-driven listener containers.
     */
    public static class Listener {

        /**
         * Whether annotation-based listeners are enabled.
         * Default is {@code true}.
         */
        private boolean enabled = true;

        /**
         * Default visibility timeout in seconds.
         * Default is {@code 30}.
         */
        private int visibilityTimeout = 30;

        /**
         * Default number of messages to read per batch.
         * Default is {@code 10}.
         */
        private int batchSize = 10;

        /**
         * Default JSON filter for message content.
         * Default is {@code {}}.
         */
        private String conditional = PgmqTemplate.DEFAULT_CONDITIONAL;

        /**
         * Default listener read mode.
         * Default is {@link PgmqListener.Mode#READ}.
         */
        private PgmqListener.Mode mode = PgmqListener.Mode.READ;

        /**
         * Default number of concurrent listener threads.
         * Default is {@code 1}.
         */
        private int concurrency = 1;

        /**
         * Whether listeners start automatically.
         * Default is {@code true}.
         */
        private boolean autoStartup = true;

        /**
         * Default acknowledgement mode.
         * Default is {@link AckMode#AUTO}.
         */
        private AckMode ackMode = AckMode.AUTO;

        /**
         * Maximum seconds to wait for messages when polling.
         * Default is {@code 5}.
         */
        private int maxPollSeconds = PgmqTemplate.DEFAULT_MAX_POLL_SECONDS;

        /**
         * Milliseconds between poll attempts.
         * Default is {@code 100}.
         */
        private int pollIntervalMs = PgmqTemplate.DEFAULT_POLL_INTERVAL_MS;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getVisibilityTimeout() {
            return visibilityTimeout;
        }

        public void setVisibilityTimeout(int visibilityTimeout) {
            Assert.isTrue(visibilityTimeout > 0, "Visibility timeout must be greater than 0");
            this.visibilityTimeout = visibilityTimeout;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            Assert.isTrue(batchSize > 0, "Batch size must be greater than 0");
            this.batchSize = batchSize;
        }

        public String getConditional() {
            return conditional;
        }

        public void setConditional(String conditional) {
            this.conditional = conditional;
        }

        public PgmqListener.Mode getMode() {
            return mode;
        }

        public void setMode(PgmqListener.Mode mode) {
            this.mode = mode;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            Assert.isTrue(concurrency > 0, "Concurrency must be greater than 0");
            this.concurrency = concurrency;
        }

        public boolean isAutoStartup() {
            return autoStartup;
        }

        public void setAutoStartup(boolean autoStartup) {
            this.autoStartup = autoStartup;
        }

        public AckMode getAckMode() {
            return ackMode;
        }

        public void setAckMode(AckMode ackMode) {
            this.ackMode = ackMode;
        }

        public int getMaxPollSeconds() {
            return maxPollSeconds;
        }

        public void setMaxPollSeconds(int maxPollSeconds) {
            Assert.isTrue(maxPollSeconds >= 0, "Max poll seconds must be greater than or equal to 0");
            this.maxPollSeconds = maxPollSeconds;
        }

        public int getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(int pollIntervalMs) {
            Assert.isTrue(pollIntervalMs > 0, "Poll interval must be greater than 0");
            this.pollIntervalMs = pollIntervalMs;
        }
    }

}
