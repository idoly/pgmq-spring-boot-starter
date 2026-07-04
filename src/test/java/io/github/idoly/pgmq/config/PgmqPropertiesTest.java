package io.github.idoly.pgmq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqPropertiesTest {

    @Test
    void defaultsMatchDocumentedConfiguration() {
        PgmqProperties properties = new PgmqProperties();
        PgmqProperties.Listener listener = properties.getListener();

        assertFalse(properties.isAutoInit());
        assertEquals("sql/pgmq.sql", properties.getSqlScript());
        assertTrue(listener.isEnabled());
        assertEquals(30, listener.getVisibilityTimeout());
        assertEquals(10, listener.getBatchSize());
        assertEquals(PgmqTemplate.DEFAULT_CONDITIONAL, listener.getConditional());
        assertEquals(PgmqListener.Mode.READ, listener.getMode());
        assertEquals(1, listener.getConcurrency());
        assertTrue(listener.isAutoStartup());
        assertEquals(AckMode.AUTO, listener.getAckMode());
        assertEquals(PgmqTemplate.DEFAULT_MAX_POLL_SECONDS, listener.getMaxPollSeconds());
        assertEquals(PgmqTemplate.DEFAULT_POLL_INTERVAL_MS, listener.getPollIntervalMs());
    }

    @Test
    void nullListenerIsReplacedWithDefaults() {
        PgmqProperties properties = new PgmqProperties();

        properties.setListener(null);

        assertNotNull(properties.getListener());
        assertEquals(30, properties.getListener().getVisibilityTimeout());
    }

    @Test
    void rejectsInvalidConfigurationValues() {
        PgmqProperties properties = new PgmqProperties();
        PgmqProperties.Listener listener = properties.getListener();

        assertThrows(IllegalArgumentException.class, () -> properties.setSqlScript(""));
        assertThrows(IllegalArgumentException.class, () -> listener.setVisibilityTimeout(0));
        assertThrows(IllegalArgumentException.class, () -> listener.setBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> listener.setConcurrency(0));
        assertThrows(IllegalArgumentException.class, () -> listener.setMaxPollSeconds(-1));
        assertThrows(IllegalArgumentException.class, () -> listener.setPollIntervalMs(0));
    }
}
