package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;

class PgmqListenerContainerOptionsTest {

    @Test
    void validatesResolvedRuntimeOptions() {
        assertDoesNotThrow(() -> options(0, 1, 1, 1, 10, 0));

        assertThrows(IllegalArgumentException.class, () -> options(-1, 1, 1, 1, 10, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 0, 1, 1, 10, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 1, 0, 1, 10, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 1, 1, 0, 10, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 1, 2, 1, 1, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 1, 1, 2, 1, 30));
        assertThrows(IllegalArgumentException.class, () -> options(0, 1, 1, 1, 10, -1));
        assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                "listener", "queue", 30, 10, "{}", null, 0, 1,
                1, 1, 10, 30, true, AckMode.AUTO));
    }

    @Test
    void rejectsEmptyIdentityAndConditionalAtTheRuntimeBoundary() {
        for (String blank : new String[] {null, "", " "}) {
            assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                    blank, "queue", 30, 10, "{}", PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, AckMode.AUTO));
            assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                    "listener", blank, 30, 10, "{}", PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, AckMode.AUTO));
            assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                    "listener", "queue", 30, 10, blank, PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, AckMode.AUTO));
        }
        assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                "listener", "queue", 0, 10, "{}", PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, AckMode.AUTO));
        assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                "listener", "queue", 30, 0, "{}", PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, AckMode.AUTO));
        assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                "listener", "queue", 30, 10, "{}", PgmqListener.Mode.READ, 0, 1, 1, 1, 10, 30, true, null));
    }

    private static PgmqListenerContainerOptions options(int maxPollSeconds, int pollIntervalMs,
            int consumerConcurrency, int pollConcurrency, int maxInFlight, int shutdownTimeoutSeconds) {
        return new PgmqListenerContainerOptions(
                "listener", "queue", 30, 10, "{}", PgmqListener.Mode.READ,
                maxPollSeconds, pollIntervalMs, consumerConcurrency, pollConcurrency,
                maxInFlight, shutdownTimeoutSeconds, true, AckMode.AUTO);
    }
}
