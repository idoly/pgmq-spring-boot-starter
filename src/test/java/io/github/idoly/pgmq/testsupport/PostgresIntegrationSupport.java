package io.github.idoly.pgmq.testsupport;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.Assert;

/** JDBC settings shared by the integration tests; the caller owns the disposable database. */
public abstract class PostgresIntegrationSupport {

    public static String jdbcUrl() {
        String url = System.getenv("PGMQ_TEST_JDBC_URL");
        Assert.hasText(url, "PGMQ_TEST_JDBC_URL must identify a disposable PostgreSQL database");
        return url;
    }

    public static String username() {
        return System.getenv().getOrDefault("PGMQ_TEST_USERNAME", "postgres");
    }

    public static String password() {
        return System.getenv().getOrDefault("PGMQ_TEST_PASSWORD", "postgres");
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PostgresIntegrationSupport::jdbcUrl);
        registry.add("spring.datasource.username", PostgresIntegrationSupport::username);
        registry.add("spring.datasource.password", PostgresIntegrationSupport::password);
    }
}
