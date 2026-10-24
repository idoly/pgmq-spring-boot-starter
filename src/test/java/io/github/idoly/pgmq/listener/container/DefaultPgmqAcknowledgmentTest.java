package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;

class DefaultPgmqAcknowledgmentTest {

    @Test
    void firstCompletionActionWins() {
        TrackingTemplate template = new TrackingTemplate();
        DefaultPgmqAcknowledgment acknowledgment = new DefaultPgmqAcknowledgment(template, "queue", 1L);

        acknowledgment.acknowledge();
        acknowledgment.delete();
        acknowledgment.requeueAfter(30);

        assertTrue(acknowledgment.isCompleted());
        assertEquals(List.of("archive:queue:1"), template.actions);
    }

    @Test
    void requeueCompletesMessage() {
        TrackingTemplate template = new TrackingTemplate();
        DefaultPgmqAcknowledgment acknowledgment = new DefaultPgmqAcknowledgment(template, "queue", 2L);

        acknowledgment.requeueAfter(15);

        assertTrue(acknowledgment.isCompleted());
        assertEquals(List.of("setVt:queue:2:15"), template.actions);
    }

    @Test
    void failedDatabaseActionsCanBeRetried() {
        for (var action : List.<java.util.function.Consumer<DefaultPgmqAcknowledgment>>of(
                DefaultPgmqAcknowledgment::acknowledge,
                DefaultPgmqAcknowledgment::delete,
                acknowledgment -> acknowledgment.requeueAfter(15))) {
            PgmqTemplate template = mock(PgmqTemplate.class);
            when(template.archive("queue", 1L)).thenThrow(new IllegalStateException("database unavailable"))
                    .thenReturn(true);
            when(template.delete("queue", 1L)).thenThrow(new IllegalStateException("database unavailable"))
                    .thenReturn(true);
            when(template.setVt("queue", 1L, 15)).thenThrow(new IllegalStateException("database unavailable"))
                    .thenReturn(null);
            var acknowledgment = new DefaultPgmqAcknowledgment(template, "queue", 1L);

            assertThrows(IllegalStateException.class, () -> action.accept(acknowledgment));
            assertFalse(acknowledgment.isCompleted());
            action.accept(acknowledgment);
            assertTrue(acknowledgment.isCompleted());
        }
    }

    @Test
    void concurrentCompletionWaitsForSuccessfulDatabaseAction() throws Exception {
        PgmqTemplate template = mock(PgmqTemplate.class);
        var databaseStarted = new java.util.concurrent.CountDownLatch(1);
        var releaseDatabase = new java.util.concurrent.CountDownLatch(1);
        when(template.archive("queue", 1L)).thenAnswer(invocation -> {
            databaseStarted.countDown();
            assertTrue(releaseDatabase.await(2, java.util.concurrent.TimeUnit.SECONDS));
            return true;
        });
        var deleteAttempted = new java.util.concurrent.CountDownLatch(1);
        var deleteStarted = new java.util.concurrent.CountDownLatch(1);
        when(template.delete("queue", 1L)).thenAnswer(invocation -> {
            deleteStarted.countDown();
            return true;
        });
        var acknowledgment = new DefaultPgmqAcknowledgment(template, "queue", 1L);
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var archive = executor.submit(acknowledgment::acknowledge);
            assertTrue(databaseStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(acknowledgment.isCompleted(), "completion must wait for the database result");
            var delete = executor.submit(() -> {
                deleteAttempted.countDown();
                acknowledgment.delete();
            });
            assertTrue(deleteAttempted.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(deleteStarted.await(200, java.util.concurrent.TimeUnit.MILLISECONDS),
                    "concurrent completion must not execute SQL before the first action finishes");
            releaseDatabase.countDown();
            archive.get(2, java.util.concurrent.TimeUnit.SECONDS);
            delete.get(2, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(acknowledgment.isCompleted());
            verify(template, times(1)).archive("queue", 1L);
            verify(template, never()).delete("queue", 1L);
        } finally {
            releaseDatabase.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    private static class TrackingTemplate extends PgmqTemplate {

        private final List<String> actions = new ArrayList<>();

        TrackingTemplate() {
            super(new JdbcTemplate(), new ObjectMapper());
        }

        @Override
        public Boolean archive(String queue, long msgId) {
            actions.add("archive:" + queue + ":" + msgId);
            return Boolean.TRUE;
        }

        @Override
        public Boolean delete(String queue, long msgId) {
            actions.add("delete:" + queue + ":" + msgId);
            return Boolean.TRUE;
        }

        @Override
        public PgmqMessage setVt(String queue, long msgId, int vt) {
            actions.add("setVt:" + queue + ":" + msgId + ":" + vt);
            return null;
        }
    }
}
