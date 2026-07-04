package io.github.idoly.pgmq.listener.container;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.github.idoly.pgmq.annotation.PgmqListener;
import io.github.idoly.pgmq.listener.AckMode;

class PgmqListenerContainerOptionsTest {

    @Test
    void validatesResolvedRuntimeOptions() {
        assertDoesNotThrow(() -> options(0, 1));

        assertThrows(IllegalArgumentException.class, () -> options(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> options(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new PgmqListenerContainerOptions(
                "listener",
                "queue",
                30,
                10,
                "{}",
                null,
                0,
                1,
                1,
                true,
                AckMode.AUTO
        ));
    }

    private static PgmqListenerContainerOptions options(int maxPollSeconds, int pollIntervalMs) {
        return new PgmqListenerContainerOptions(
                "listener",
                "queue",
                30,
                10,
                "{}",
                PgmqListener.Mode.READ,
                maxPollSeconds,
                pollIntervalMs,
                1,
                true,
                AckMode.AUTO
        );
    }
}
