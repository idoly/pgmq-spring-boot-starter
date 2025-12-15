package io.github.idoly.pgmq.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.idoly.pgmq.PgmqException;

/**
 * Initializes the PGMQ PostgreSQL extension and optionally executes
 * a SQL script defined in {@link PgmqProperties}.
 *
 * <p>Automatically runs if {@code autoInit} property is enabled.</p>
 *
 * <p>Depends on {@link org.springframework.jdbc.core.JdbcTemplate} and
 * {@link io.github.idoly.pgmq.config.PgmqProperties}.</p>
 *
 * @see PgmqProperties
 */
public class PgmqInitializer {

    private static final Log logger = LogFactory.getLog(PgmqListenerContainer.class);

    private final JdbcTemplate jdbcTemplate;
    private final PgmqProperties properties;

    /**
     * Constructs a PgmqInitializer.
     *
     * @param jdbcTemplate the JdbcTemplate used for executing SQL
     * @param properties the PGMQ configuration properties
     */
    public PgmqInitializer(JdbcTemplate jdbcTemplate, PgmqProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    /**
     * Initializes PGMQ by executing the SQL script if the extension is not available,
     * and enabling the PGMQ extension if it is not yet installed.
     */
    public void init() {
        try {
            if (!isPgmqAvailable()) {
                executeSql(properties.getSqlScript());
            } else if (!isPgmqInstalled()) {
                enableExtension();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * Enables the PGMQ PostgreSQL extension if it is not already installed.
     *
     * @throws DataAccessException if executing the SQL fails
     */
    private void enableExtension() throws DataAccessException {
        jdbcTemplate.execute("create extension pgmq");
    }

    /**
     * Checks if the PGMQ extension is available in the PostgreSQL instance.
     *
     * @return true if available, false otherwise
     */
    private boolean isPgmqAvailable() {
        return jdbcTemplate.queryForObject("select exists (select 1 from pg_available_extensions where name = 'pgmq')", Boolean.class);
    }

    /**
     * Checks if the PGMQ extension is installed.
     *
     * @return true if installed, false otherwise
     */
    private boolean isPgmqInstalled() {
        return jdbcTemplate.queryForObject("select exists (select 1 from pg_extension where extname = 'pgmq')", Boolean.class);
    }

    /**
     * Executes a SQL script from the classpath using the DataSource from JdbcTemplate.
     *
     * @param sqlScript path of the SQL script in the classpath
     * @throws IOException if the script cannot be read
     * @throws PgmqException if the script does not exist or execution fails
     */
    private void executeSql(String sqlScript) throws IOException {
        Resource resource = new ClassPathResource(sqlScript);
        if (!resource.exists()) {
            throw new PgmqException("SQL script not found: " + sqlScript);
        }

        String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        jdbcTemplate.execute(sql);
    }
}
