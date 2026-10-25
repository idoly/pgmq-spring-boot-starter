package io.github.idoly.pgmq.metrics;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import com.sun.management.ThreadMXBean;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** Manual component benchmark. Not a throughput claim or a timing assertion in the test suite. */
public final class ListenerMetricsBenchmark {

    private ListenerMetricsBenchmark() {
    }

    public static void main(String[] args) {
        int count = args.length == 0 ? 200_000 : Integer.parseInt(args[0]);
        if (count <= 0) {
            throw new IllegalArgumentException("Iteration count must be positive");
        }
        if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean allocation)
                || !allocation.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("This benchmark requires a JDK with thread allocation counters");
        }
        allocation.setThreadAllocatedMemoryEnabled(true);
        var registry = new SimpleMeterRegistry();
        try {
            var metrics = new PgmqListenerMetrics();
            metrics.bindTo(registry);
            for (int i = 0; i < 3; i++) {
                record(metrics, count);
            }
            long[] bytes = new long[5];
            long[] nanos = new long[5];
            long thread = Thread.currentThread().getId();
            for (int i = 0; i < bytes.length; i++) {
                long allocatedBefore = allocation.getThreadAllocatedBytes(thread);
                long started = System.nanoTime();
                record(metrics, count);
                nanos[i] = System.nanoTime() - started;
                bytes[i] = allocation.getThreadAllocatedBytes(thread) - allocatedBefore;
                System.out.printf("round=%d events=%d bytes=%d nanos=%d%n", i, count * 2L, bytes[i], nanos[i]);
            }
            double recorded = registry.find("pgmq.listener.messages").counters().stream()
                    .mapToDouble(counter -> counter.count()).sum();
            if (recorded != count * 8L) {
                throw new AssertionError("Metric count changed: " + recorded);
            }
            Arrays.sort(bytes);
            Arrays.sort(nanos);
            System.out.printf("median_events=%d median_bytes=%d median_nanos=%d%n",
                    count * 2L, bytes[bytes.length / 2], nanos[nanos.length / 2]);
        } finally {
            registry.close();
        }
    }

    private static void record(PgmqListenerMetrics metrics, int count) {
        String[] listeners = {"one", "two", "three", "four"};
        for (int i = 0; i < count; i++) {
            String listener = listeners[i & 3];
            metrics.recordPoll(listener, "orders", 1_000, "success");
            metrics.recordProcessing(listener, "orders", 1_000, "success");
        }
    }
}
