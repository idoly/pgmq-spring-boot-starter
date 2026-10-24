package io.github.idoly.pgmq.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.idoly.pgmq.PgmqException;

class PgmqInitializerTest {

    @Test
    void installedExtensionVersionMismatchWarnsAndContinues() {
        StubJdbcTemplate jdbcTemplate = new StubJdbcTemplate();
        jdbcTemplate.pgmqAvailable = true;
        jdbcTemplate.pgmqInstalled = true;
        jdbcTemplate.extensionVersion = "1.10.0";

        assertDoesNotThrow(() -> new PgmqInitializer(jdbcTemplate, new PgmqProperties()).init());
    }

    @Test
    void availableExtensionIsEnabledWhenMissing() {
        StubJdbcTemplate jdbcTemplate = new StubJdbcTemplate();
        jdbcTemplate.pgmqAvailable = true;
        jdbcTemplate.pgmqInstalled = false;

        new PgmqInitializer(jdbcTemplate, new PgmqProperties()).init();

        assertTrue(jdbcTemplate.updates.contains("create extension pgmq"));
    }

    @Test
    void bundledSchemaContinuesWithoutVersionMetadata() {
        StubJdbcTemplate jdbcTemplate = new StubJdbcTemplate();
        jdbcTemplate.pgmqAvailable = false;
        jdbcTemplate.bundledSchemaInstalled = true;

        assertDoesNotThrow(() -> new PgmqInitializer(jdbcTemplate, new PgmqProperties()).init());
        assertEquals(0, jdbcTemplate.executions);
    }

    @Test
    void bundledSchemaIsInstalledWhenMissing() {
        StubJdbcTemplate jdbcTemplate = new StubJdbcTemplate();
        jdbcTemplate.pgmqAvailable = false;
        jdbcTemplate.bundledSchemaInstalled = false;

        new PgmqInitializer(jdbcTemplate, new PgmqProperties()).init();

        assertEquals(1, jdbcTemplate.executions);
    }

    @Test
    void missingConfiguredSqlScriptFailsInitialization() {
        StubJdbcTemplate jdbcTemplate = new StubJdbcTemplate();
        jdbcTemplate.pgmqAvailable = false;
        jdbcTemplate.bundledSchemaInstalled = false;
        PgmqProperties properties = new PgmqProperties();
        properties.setSqlScript("missing-pgmq.sql");

        assertThrows(PgmqException.class, () -> new PgmqInitializer(jdbcTemplate, properties).init());
    }

    private static class StubJdbcTemplate extends JdbcTemplate {

        private boolean pgmqAvailable;

        private boolean pgmqInstalled;

        private String extensionVersion = "1.13.1";

        private boolean bundledSchemaInstalled = true;

        private final List<String> updates = new ArrayList<>();

        private int executions;

        @Override
        public <T> T queryForObject(String sql, Class<T> requiredType) {
            if (sql.contains("pg_available_extensions")) {
                return requiredType.cast(pgmqAvailable);
            }
            if (sql.contains("select exists (select 1 from pg_extension")) {
                return requiredType.cast(pgmqInstalled);
            }
            if (sql.contains("select extversion")) {
                return requiredType.cast(extensionVersion);
            }
            if (sql.contains("to_regprocedure('pgmq.send(text,jsonb)'")) {
                return requiredType.cast(bundledSchemaInstalled);
            }
            throw new IllegalArgumentException("Unexpected SQL: " + sql);
        }

        @Override
        public int update(String sql) {
            updates.add(sql);
            if (sql.startsWith("create extension pgmq")) {
                pgmqInstalled = true;
            }
            return 0;
        }

        @Override
        public void execute(String sql) {
            executions++;
            bundledSchemaInstalled = true;
        }
    }
}
