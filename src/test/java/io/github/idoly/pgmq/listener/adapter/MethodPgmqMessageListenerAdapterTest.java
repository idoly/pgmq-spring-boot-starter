package io.github.idoly.pgmq.listener.adapter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.annotation.PgmqHeader;
import io.github.idoly.pgmq.annotation.PgmqHeaders;
import io.github.idoly.pgmq.annotation.PgmqPayload;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;
import io.github.idoly.pgmq.listener.PgmqAcknowledgment;

class MethodPgmqMessageListenerAdapterTest {

    @Test
    void invokesMethodWithResolvedArguments() throws Exception {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());
        Handler handler = new Handler();
        MethodPgmqMessageListenerAdapter adapter = new MethodPgmqMessageListenerAdapter(
                pgmqTemplate,
                handler,
                Handler.class.getDeclaredMethod(
                        "handle", Payload.class, String.class, Integer.class, Map.class, PgmqMessage.class, PgmqAcknowledgment.class)
        );
        PgmqMessage message = new PgmqMessage(
                1L,
                0,
                null,
                null,
                null,
                Map.of("id", 7, "event", "created"),
                Map.of("source", "test", "attempt", 3)
        );
        TestAcknowledgment acknowledgment = new TestAcknowledgment();

        adapter.onMessage(message, acknowledgment);

        assertEquals(new Payload(7, "created"), handler.payload);
        assertEquals("test", handler.source);
        assertEquals(3, handler.attempt);
        assertEquals(message.headers(), handler.headers);
        assertSame(message, handler.message);
        assertSame(acknowledgment, handler.acknowledgment);
        assertTrue(acknowledgment.completed);
    }

    @Test
    void unannotatedMapParameterIsPayload() throws Exception {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());
        MapPayloadHandler handler = new MapPayloadHandler();
        MethodPgmqMessageListenerAdapter adapter = new MethodPgmqMessageListenerAdapter(
                pgmqTemplate,
                handler,
                MapPayloadHandler.class.getDeclaredMethod("handle", Map.class)
        );
        Map<String, Object> payload = Map.of("id", 7, "event", "created");
        PgmqMessage message = new PgmqMessage(1L, 0, null, null, null,
                payload, Map.of("source", "headers"));

        adapter.onMessage(message, new TestAcknowledgment());

        assertEquals(payload, handler.payload);
    }

    @Test
    void rejectsHeadersAnnotationOnNonMapParameter() throws Exception {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());

        assertThrows(IllegalArgumentException.class, () -> new MethodPgmqMessageListenerAdapter(
                pgmqTemplate,
                new InvalidHeadersHandler(),
                InvalidHeadersHandler.class.getDeclaredMethod("handle", String.class)
        ));
    }

    @Test
    void rethrowsListenerMethodException() throws Exception {
        PgmqTemplate pgmqTemplate = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());
        FailingHandler handler = new FailingHandler();
        MethodPgmqMessageListenerAdapter adapter = new MethodPgmqMessageListenerAdapter(
                pgmqTemplate,
                handler,
                FailingHandler.class.getDeclaredMethod("handle", Payload.class)
        );
        PgmqMessage message = new PgmqMessage(1L, 0, null, null, null,
                Map.of("id", 7, "event", "created"), Map.of());

        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> adapter.onMessage(message, new TestAcknowledgment()));
        assertEquals("failed", exception.getMessage());
    }

    @Test
    void rejectsAsyncReturnValuesBeforeInvocation() throws Exception {
        var template = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());
        var handler = new FutureHandler();
        var error = assertThrows(IllegalArgumentException.class, () -> new MethodPgmqMessageListenerAdapter(
                template, handler, FutureHandler.class.getDeclaredMethod("handle", String.class)));
        assertTrue(error.getMessage().contains("must return void"));
        assertFalse(handler.invoked);
    }

    @Test
    void rejectsMethodClassMetaAndTargetAsyncAnnotations() throws Exception {
        var template = new PgmqTemplate(new JdbcTemplate(), new ObjectMapper());
        var proxyFactory = new ProxyFactory(new AsyncImplementation());
        proxyFactory.setInterfaces(AsyncContract.class);
        var interfaceProxyFactory = new ProxyFactory(new InterfaceAsyncImplementation());
        interfaceProxyFactory.setInterfaces(InterfaceAsyncContract.class);
        var cases = java.util.List.of(
                new MethodTarget(new MethodAsyncHandler(), MethodAsyncHandler.class.getDeclaredMethod("handle", String.class)),
                new MethodTarget(new ClassAsyncHandler(), ClassAsyncHandler.class.getDeclaredMethod("handle", String.class)),
                new MethodTarget(new MetaAsyncHandler(), MetaAsyncHandler.class.getDeclaredMethod("handle", String.class)),
                new MethodTarget(proxyFactory.getProxy(), AsyncContract.class.getMethod("handle", String.class)),
                new MethodTarget(interfaceProxyFactory.getProxy(), InterfaceAsyncContract.class.getMethod("handle", String.class)));
        for (var candidate : cases) {
            var error = assertThrows(IllegalArgumentException.class, () -> new MethodPgmqMessageListenerAdapter(
                    template, candidate.bean(), candidate.method()));
            assertTrue(error.getMessage().contains("@Async"));
        }
    }

    private record MethodTarget(Object bean, java.lang.reflect.Method method) {}

    private static class FutureHandler {
        boolean invoked;
        public java.util.concurrent.CompletableFuture<Void> handle(String payload) {
            invoked = true;
            return new java.util.concurrent.CompletableFuture<>();
        }
    }

    private static class MethodAsyncHandler {
        @Async
        void handle(String payload) {}
    }

    @Async
    private static class ClassAsyncHandler {
        void handle(String payload) {}
    }

    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.METHOD)
    @Async
    private @interface Background {}

    private static class MetaAsyncHandler {
        @Background
        void handle(String payload) {}
    }

    public interface AsyncContract {
        void handle(String payload);
    }

    private static class AsyncImplementation implements AsyncContract {
        @Async
        @Override
        public void handle(String payload) {}
    }

    public interface InterfaceAsyncContract {
        @Async
        void handle(String payload);
    }

    private static class InterfaceAsyncImplementation implements InterfaceAsyncContract {
        @Override
        public void handle(String payload) {}
    }

    public record Payload(int id, String event) {}

    private static class Handler {
        private Payload payload;
        private String source;
        private Integer attempt;
        private Map<String, Object> headers;
        private PgmqMessage message;
        private PgmqAcknowledgment acknowledgment;

        void handle(
                @PgmqPayload Payload payload,
                @PgmqHeader("source") String source,
                @PgmqHeader("attempt") Integer attempt,
                @PgmqHeaders Map<String, Object> headers,
                PgmqMessage message,
                PgmqAcknowledgment acknowledgment) {
            this.payload = payload;
            this.source = source;
            this.attempt = attempt;
            this.headers = headers;
            this.message = message;
            this.acknowledgment = acknowledgment;
            acknowledgment.acknowledge();
        }
    }

    private static class MapPayloadHandler {
        private Map<String, Object> payload;

        void handle(Map<String, Object> payload) {
            this.payload = payload;
        }
    }

    private static class InvalidHeadersHandler {

        void handle(@PgmqHeaders String headers) {
        }
    }

    private static class FailingHandler {

        void handle(Payload payload) {
            throw new IllegalStateException("failed");
        }
    }

    private static class TestAcknowledgment implements PgmqAcknowledgment {
        private boolean completed;

        @Override
        public void acknowledge() {
            this.completed = true;
        }

        @Override
        public void delete() {
            this.completed = true;
        }

        @Override
        public void requeueAfter(int visibilityTimeoutSeconds) {
            this.completed = true;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }
    }
}
