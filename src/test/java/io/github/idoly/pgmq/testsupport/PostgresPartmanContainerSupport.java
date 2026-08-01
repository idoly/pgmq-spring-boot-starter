package io.github.idoly.pgmq.testsupport;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared PostgreSQL container configuration for integration tests.
 */
public abstract class PostgresPartmanContainerSupport {

    private static final DockerImageName POSTGRES_PARTMAN_IMAGE = DockerImageName.parse(
                    "ghcr.io/dbsystel/postgresql-partman@"
                            + "sha256:c12e31bdd5e16ea72b88f30bbf96b539368f747a8c7896880a22b3cf0741a6f0")
            .asCompatibleSubstituteFor("postgres");

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_PARTMAN_IMAGE)
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        // Spring resolves these suppliers before inherited @Container fields are started.
        POSTGRES.start();
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
    }

}
