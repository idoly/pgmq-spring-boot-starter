package io.github.idoly.pgmq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for PGMQ.
 *
 * <p>Properties under the {@code pgmq} prefix control PGMQ initialization
 * behavior and SQL script configuration.</p>
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
     * Default constructor for PgmqProperties.
     *
     * <p>Required for Spring Boot's {@link ConfigurationProperties} binding.</p>
     */
    public PgmqProperties() {}

    /**
     * Get whether auto-initialization is enabled.
     *
     * @return {@code true} if PGMQ should be auto-initialized, {@code false} otherwise
     */
    public boolean isAutoInit() {
        return autoInit;
    }

    /**
     * Set whether auto-initialization is enabled.
     *
     * @param autoInit {@code true} to enable auto-initialization, {@code false} to disable
     */
    public void setAutoInit(boolean autoInit) {
        this.autoInit = autoInit;
    }

    /**
     * Get the path to the SQL script used for initialization.
     *
     * @return the SQL script path
     */
    public String getSqlScript() {
        return sqlScript;
    }

    /**
     * Set the path to the SQL script used for initialization.
     *
     * @param sqlScript the SQL script path
     */
    public void setSqlScript(String sqlScript) {
        this.sqlScript = sqlScript;
    }
}
