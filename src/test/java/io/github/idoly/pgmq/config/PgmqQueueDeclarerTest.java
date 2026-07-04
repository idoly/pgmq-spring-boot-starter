package io.github.idoly.pgmq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.idoly.pgmq.queue.PgmqQueue;
import io.github.idoly.pgmq.template.PgmqTemplate;

class PgmqQueueDeclarerTest {

    @Test
    void autoInitRunsBeforeQueueDeclarations() {
        List<String> events = new ArrayList<>();
        PgmqProperties properties = new PgmqProperties();
        properties.setAutoInit(true);
        TrackingInitializer initializer = new TrackingInitializer(properties, events);
        TrackingTemplate template = new TrackingTemplate(events);
        PgmqQueueDeclarer declarer = new PgmqQueueDeclarer(
                template,
                initializer,
                properties,
                queueProvider(PgmqQueue.create("jobs"))
        );

        declarer.afterSingletonsInstantiated();

        assertEquals(List.of("init", "create:jobs"), events);
    }

    @Test
    void declaresAllQueueModesWithoutAutoInitWhenDisabled() {
        List<String> events = new ArrayList<>();
        PgmqProperties properties = new PgmqProperties();
        TrackingInitializer initializer = new TrackingInitializer(properties, events);
        TrackingTemplate template = new TrackingTemplate(events);
        PgmqQueueDeclarer declarer = new PgmqQueueDeclarer(
                template,
                initializer,
                properties,
                queueProvider(
                        PgmqQueue.create("jobs"),
                        PgmqQueue.createUnlogged("fast_jobs"),
                        PgmqQueue.createPartitioned("history_jobs")
                )
        );

        declarer.afterSingletonsInstantiated();

        assertEquals(List.of("create:jobs", "createUnlogged:fast_jobs", "createPartitioned:history_jobs"), events);
    }

    @Test
    void autoInitRunsEvenWithoutQueueDeclarations() {
        List<String> events = new ArrayList<>();
        PgmqProperties properties = new PgmqProperties();
        properties.setAutoInit(true);
        TrackingInitializer initializer = new TrackingInitializer(properties, events);
        TrackingTemplate template = new TrackingTemplate(events);
        PgmqQueueDeclarer declarer = new PgmqQueueDeclarer(
                template,
                initializer,
                properties,
                queueProvider()
        );

        declarer.afterSingletonsInstantiated();

        assertEquals(List.of("init"), events);
    }

    private static ObjectProvider<PgmqQueue> queueProvider(PgmqQueue... queues) {
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        for (int i = 0; i < queues.length; i++) {
            beanFactory.addBean("queue" + i, queues[i]);
        }
        return beanFactory.getBeanProvider(PgmqQueue.class);
    }

    private static class TrackingInitializer extends PgmqInitializer {

        private final List<String> events;

        TrackingInitializer(PgmqProperties properties, List<String> events) {
            super(new JdbcTemplate(), properties);
            this.events = events;
        }

        @Override
        public void init() {
            events.add("init");
        }
    }

    private static class TrackingTemplate extends PgmqTemplate {

        private final List<String> events;

        TrackingTemplate(List<String> events) {
            super(new JdbcTemplate(), new ObjectMapper());
            this.events = events;
        }

        @Override
        public void create(String queue) {
            events.add("create:" + queue);
        }

        @Override
        public void createUnlogged(String queue) {
            events.add("createUnlogged:" + queue);
        }

        @Override
        public void createPartitioned(String queue) {
            events.add("createPartitioned:" + queue);
        }
    }
}
