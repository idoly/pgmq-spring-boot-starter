package io.github.idoly.pgmq.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import io.github.idoly.pgmq.PgmqException;

/**
 * Initializes the database objects required by PGMQ.
 *
 * <p>If the PostgreSQL extension is available, it enables the extension when
 * needed. Otherwise, it applies the configured SQL script when the bundled schema
 * is not already installed.</p>
 */
public class PgmqInitializer {

    /** PGMQ version this starter is built against. */
    private static final String PGMQ_VERSION = "1.13.1";

    /** Logger used for version mismatch warnings. */
    private static final Log logger = LogFactory.getLog(PgmqInitializer.class);

    /** JDBC client used for extension and schema checks. */
    private final JdbcTemplate jdbcTemplate;

    /** Initialization properties. */
    private final PgmqProperties properties;

    /**
     * Create an initializer backed by JDBC and starter properties.
     *
     * @param jdbcTemplate JDBC client used for database checks and script execution
     * @param properties initializer configuration
     */
    public PgmqInitializer(JdbcTemplate jdbcTemplate, PgmqProperties properties) {
        Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null");
        Assert.notNull(properties, "PgmqProperties must not be null");
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    /**
     * Initialize PGMQ if it is not already available to the application.
     */
    public void init() {
        try {
            if (isPgmqAvailable()) {
                if (!isPgmqInstalled()) {
                    enableExtension();
                }
                validateInstalledVersion();
                return;
            }

            if (!isPgmqSchemaInstalled()) {
                executeSql(properties.getSqlScript());
            }
        } catch (PgmqException e) {
            throw e;
        } catch (Exception e) {
            throw new PgmqException("failed to initialize PGMQ", e);
        }
    }

    /**
     * Enable the PostgreSQL extension.
     */
    private void enableExtension() throws DataAccessException {
        jdbcTemplate.update("create extension pgmq");
    }

    /**
     * Return whether the PGMQ extension can be installed by PostgreSQL.
     */
    private boolean isPgmqAvailable() {
        Boolean available = jdbcTemplate.queryForObject(
                "select exists (select 1 from pg_available_extensions where name = 'pgmq')", Boolean.class);
        return Boolean.TRUE.equals(available);
    }

    /**
     * Return whether the PGMQ extension is already installed.
     */
    private boolean isPgmqInstalled() {
        Boolean installed = jdbcTemplate.queryForObject(
                "select exists (select 1 from pg_extension where extname = 'pgmq')", Boolean.class);
        return Boolean.TRUE.equals(installed);
    }

    /**
     * Warn when the installed PGMQ extension version does not match this starter.
     */
    private void validateInstalledVersion() {
        String installedVersion = jdbcTemplate.queryForObject(
                "select extversion from pg_extension where extname = 'pgmq'", String.class);
        if (!PGMQ_VERSION.equals(installedVersion)) {
            logger.warn("PGMQ extension version mismatch: installed "
                    + installedVersion + ", expected " + PGMQ_VERSION);
        }
    }

    /**
     * Return whether the bundled SQL schema appears to be installed.
     */
    private boolean isPgmqSchemaInstalled() {
        String sql = """
                select to_regclass('pgmq.meta') is not null
                   and to_regprocedure('pgmq.send(text,jsonb)') is not null
                """;
        Boolean installed = jdbcTemplate.queryForObject(sql, Boolean.class);
        return Boolean.TRUE.equals(installed);
    }

    /**
     * Execute the configured classpath SQL script.
     */
    private void executeSql(String sqlScript) throws IOException {
        Resource resource = new ClassPathResource(sqlScript);
        if (!resource.exists()) {
            throw new PgmqException("SQL script not found: " + sqlScript);
        }

        String sql = resource.getContentAsString(StandardCharsets.UTF_8);
        jdbcTemplate.execute(sql);
    }

}
