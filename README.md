# Project Overview

This is a Spring Boot starter for PostgreSQL's PGMQ (Postgres Message Queue). It wraps database function calls and provides annotation-based listeners, template methods, and Micrometer metrics integration, making it easier to use PGMQ features in a Spring application.

## Quickstart

### Dependency

```xml
<dependency>
	<groupId>io.github.idoly</groupId>
	<artifactId>pgmq-spring-boot-starter</artifactId>
	<version>1.0-1.18.0</version>
</dependency>
```

### Configuration

| Property          | Type      | Default        | Required | Description                                                                                                                          |
| ----------------- | --------- | -------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `pgmq.auto-init`  | `boolean` | `false`        | No       | Whether to automatically initialize the PGMQ schema on application startup. Set to `true` for local development or first-time setup. |
| `pgmq.sql-script` | `string`  | `sql/pgmq.sql` | No       | Classpath location of the SQL script used to initialize PGMQ when `pgmq.auto-init` is enabled.                                       |

### Micrometer Metrics

You can access the following PGMQ-related metrics via the Actuator:

```
/actuator/metrics/pgmq.queue.length
/actuator/metrics/pgmq.queue.visible_length
/actuator/metrics/pgmq.queue.newest_msg_age_sec
/actuator/metrics/pgmq.queue.oldest_msg_age_sec
/actuator/metrics/pgmq.queue.total_messages
```

### Example

```java
@SpringBootApplication
public class Application {

	@Resource
	private PgmqTemplate pgmqTemplate;

	@Bean
	public Queue q1() {
		return Queue.create("q1");
	}

	@PgmqListener(queue = "q1")
	public void listenter(Message message) {
		System.out.println("received message from q1: " + message);
	}

	@Bean
	public ApplicationRunner runner() {
		return args -> pgmqTemplate.send("q1", "{\"foo\":\"bar\"");
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
```

## Test 

### Environment Setup

```shell
# 1. Run PostgreSQL with pg_partman extension in a Podman container
podman run -d --name postgresql-partman -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/dbsystel/postgresql-partman

# 2. Enter the container's shell
podman exec -it postgresql-partman bash

# 3. Connect to PostgreSQL
psql -U postgres

# 4. Install the pg_partman extension
create extension pg_partman;
```

### Running Tests

```shell
mvn clean test
```

