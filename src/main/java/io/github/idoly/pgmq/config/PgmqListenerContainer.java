package io.github.idoly.pgmq.config;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.Lifecycle;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.core.Message;
import io.github.idoly.pgmq.core.PgmqTemplate;

/**
 * PgmqListenerContainer manages the lifecycle of a PGMQ message listener for a specific queue.
 * It reads messages from the queue and invokes the annotated listener method concurrently.
 *
 * <p>Implements Spring's {@link org.springframework.context.Lifecycle} interface to provide start/stop control.</p>
 */
public class PgmqListenerContainer implements Lifecycle {

    private static final Log logger = LogFactory.getLog(PgmqListenerContainer.class);

    /** Flag indicating whether the listener container is currently running. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** PGMQ template used for reading and deleting messages. */
    private final PgmqTemplate pgmqTemplate;

    /** The Spring bean containing the listener method. */
    private final Object bean;

    /** The method annotated with {@link PgmqListener}. */
    private final Method method;

    /** Queue name to listen to. */
    private final String queue;

    /** Visibility timeout in seconds. */
    private final int vt;

    /** Number of messages to read per batch. */
    private final int qty;

    /** Conditional filter in JSON format (optional). */
    private final String conditional;

    /** Number of concurrent threads for processing messages. */
    private final int concurrency;

    /** Executor service for running listener threads. */
    private final ExecutorService executor;

    /**
     * Construct a listener container for the specified queue and listener method.
     *
     * @param pgmqTemplate the PgmqTemplate used to interact with the queue
     * @param bean the Spring bean containing the listener method
     * @param method the listener method to invoke for each message
     * @param annotation the {@link PgmqListener} annotation with listener configuration
     */
    public PgmqListenerContainer(PgmqTemplate pgmqTemplate, Object bean, Method method, PgmqListener annotation) {
        this.pgmqTemplate = pgmqTemplate;
        this.bean = bean;
        this.method = method;

        this.queue = annotation.queue();
        this.vt = annotation.vt();
        this.qty = annotation.qty();
        this.conditional = annotation.conditional();
        this.concurrency = annotation.concurrency();

        this.executor = Executors.newFixedThreadPool(this.concurrency, r -> {
            Thread t = new Thread(r);
            t.setName("pgmq-listener-" + queue + "-" + t.getId());
            return t;
        });
    }

    /**
     * Start the listener container. Creates a fixed number of threads as defined by
     * {@link PgmqListener#concurrency()} to continuously poll the queue and invoke the listener method.
     */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        logger.info("starting PgmqListenerContainer for queue: " + queue);

        for (int i = 0; i < concurrency; i++) {
            executor.submit(() -> {
                while (isRunning()) {
                    List<Message> records = pgmqTemplate.readWithPoll(queue, vt, qty, 5, 100, conditional);
                    if (records.isEmpty()) {
                        continue;
                    }

                    List<Long> successIds = new ArrayList<>(records.size());
                    for (Message record : records) {
                        try {
                            method.invoke(bean, buildArgs(method.getParameters(), record));
                            successIds.add(record.msgId());
                        } catch (Exception e) {
                            logger.error("failed to process message [" + record.msgId() + "] for queue " + queue + ": " + e.getMessage());
                        }
                    }

                    if (!successIds.isEmpty()) {
                        pgmqTemplate.archive(queue, successIds);
                    }
                }
            });
        }
    }

    /**
     * Build the argument array to pass to the listener method based on its parameter types.
     * Supports {@link Message}, message body type, and headers map.
     *
     * @param params the method parameters
     * @param record the message record
     * @return the argument array for invoking the listener method
     * @throws Exception if argument mapping fails
     */
    private Object[] buildArgs(Parameter[] params, Message record) throws Exception {
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            Class<?> paramType = params[i].getType();
            if (paramType.equals(Message.class)) {
                args[i] = record;
            } else if (paramType.equals(record.message().getClass())) {
                args[i] = record.message();
            } else if (Map.class.isAssignableFrom(paramType) && "headers".equals(params[i].getName())) {
                args[i] = record.headers();
            }
        }
        return args;
    }

    /**
     * Stop the listener container and shutdown all listener threads immediately.
     */
    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        logger.info("stopping PgmqListenerContainer for queue " + queue);
        executor.shutdownNow();
    }

    /**
     * Check if the listener container is currently running.
     *
     * @return {@code true} if running, {@code false} otherwise
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }
}
