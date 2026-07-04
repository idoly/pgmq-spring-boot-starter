package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
