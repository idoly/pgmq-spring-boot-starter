package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.PgmqException;
import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.config.PgmqProperties;
import io.github.idoly.pgmq.listener.endpoint.MethodPgmqListenerEndpoint;
import io.github.idoly.pgmq.template.PgmqTemplate;

class SimplePgmqListenerContainerFactoryTest {

    @Test
    void createsContainerFromAnnotationAndProperties() throws Exception {
        SimplePgmqListenerContainerFactory factory = factory();
        Handler handler = new Handler();

        PgmqListenerContainer container = factory.createListenerContainer(endpoint(handler, "handle"));

        assertEquals("handle", container.getId());
        assertFalse(container.isAutoStartup());
    }

    @Test
    void allowsZeroMaxPollSeconds() throws Exception {
        SimplePgmqListenerContainerFactory factory = factory();
        Handler handler = new Handler();

        assertDoesNotThrow(() -> factory.createListenerContainer(endpoint(handler, "zeroMaxPollSeconds")));
    }

    @Test
    void rejectsInvalidAnnotationOptions() throws Exception {
        SimplePgmqListenerContainerFactory factory = factory();
        Handler handler = new Handler();

        assertThrows(PgmqException.class, () -> factory.createListenerContainer(endpoint(handler, "invalidPollInterval")));
    }

    private static SimplePgmqListenerContainerFactory factory() {
        return new SimplePgmqListenerContainerFactory(
                new PgmqTemplate(new JdbcTemplate(), new ObjectMapper()),
                new PgmqProperties(),
                (error, message, acknowledgment) -> {}
        );
    }

    private static MethodPgmqListenerEndpoint endpoint(Handler handler, String methodName) throws NoSuchMethodException {
        Method method = Handler.class.getDeclaredMethod(methodName, String.class);
        PgmqListener annotation = method.getAnnotation(PgmqListener.class);
        return new MethodPgmqListenerEndpoint(methodName, handler, method, "handler", annotation.value(), annotation);
    }

    private static class Handler {

        @PgmqListener(value = "queue", autoStartup = "false")
        void handle(String payload) {
        }

        @PgmqListener(value = "queue", maxPollSeconds = 0)
        void zeroMaxPollSeconds(String payload) {
        }

        @PgmqListener(value = "queue", pollIntervalMs = 0)
        void invalidPollInterval(String payload) {
        }
    }
}
