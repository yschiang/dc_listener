package dc.listener;

import dc.listener.reconcile.SingleSessionReconciler;
import dc.listener.session.FakeNatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.session.ObservedState;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    // ---------- StartupConfig: env parsing / validation ----------

    private static Map<String, String> env(String... kv) {
        var m = new HashMap<String, String>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    @Test
    void rejectsMissingSessionName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> StartupConfig.fromEnv(env()));
        assertTrue(ex.getMessage().contains("SESSION_NAME"), ex.getMessage());
    }

    @Test
    void rejectsBlankSessionName() {
        assertThrows(IllegalArgumentException.class,
                () -> StartupConfig.fromEnv(env("SESSION_NAME", "   ")));
    }

    @Test
    void appliesDefaultsWhenOnlySessionNameProvided() {
        var cfg = StartupConfig.fromEnv(env("SESSION_NAME", "tool-a"));
        assertEquals("tool-a", cfg.sessionName());
        assertEquals("nats://localhost:4222", cfg.natsUrl());
        assertEquals(Path.of("config/sessions.yaml"), cfg.sessionsFile());
        assertEquals(200, cfg.processDelayMs());
        assertEquals(8080, cfg.statusPort());
        assertEquals(Duration.ofSeconds(30), cfg.shutdownTimeout());
    }

    @Test
    void parsesAllProvidedValues() {
        var cfg = StartupConfig.fromEnv(env(
                "SESSION_NAME", "tool-b",
                "NATS_URL", "nats://nats:4222",
                "SESSIONS_FILE", "/etc/sessions.yaml",
                "PROCESS_DELAY_MS", "50",
                "STATUS_PORT", "9090",
                "SHUTDOWN_TIMEOUT_MS", "5000"));
        assertEquals("tool-b", cfg.sessionName());
        assertEquals("nats://nats:4222", cfg.natsUrl());
        assertEquals(Path.of("/etc/sessions.yaml"), cfg.sessionsFile());
        assertEquals(50, cfg.processDelayMs());
        assertEquals(9090, cfg.statusPort());
        assertEquals(Duration.ofMillis(5000), cfg.shutdownTimeout());
    }

    @Test
    void rejectsInvalidNumericWithClearError() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> StartupConfig.fromEnv(env("SESSION_NAME", "tool-a", "PROCESS_DELAY_MS", "abc")));
        assertTrue(ex.getMessage().contains("PROCESS_DELAY_MS"), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> StartupConfig.fromEnv(env("SESSION_NAME", "tool-a", "STATUS_PORT", "notaport")));
    }

    @Test
    void rejectsOutOfRangeStatusPort() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> StartupConfig.fromEnv(env("SESSION_NAME", "tool-a", "STATUS_PORT", "70000")));
        assertTrue(ex.getMessage().contains("STATUS_PORT"), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> StartupConfig.fromEnv(env("SESSION_NAME", "tool-a", "STATUS_PORT", "-1")));
    }

    // ---------- ShutdownCoordinator: graceful, bounded, idempotent ----------

    private static ListenerSession activeSession(FakeNatsLink link) {
        var session = new ListenerSession("tool-a", link, 0);
        var rec = new SingleSessionReconciler(Path.of("/nonexistent"), session);
        rec.applySnapshot("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-tool-a
            """);
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        return session;
    }

    @Test
    void gracefulShutdownDrainsClosesNatsAndTerminatesOneLoop() {
        var link = new FakeNatsLink();
        var session = activeSession(link);
        var stopped = new AtomicBoolean();
        var coord = new ShutdownCoordinator(session, () -> stopped.set(true), Duration.ofSeconds(5));

        coord.shutdown();

        assertTrue(session.isTerminated(), "the one actor loop terminates");
        assertFalse(link.isConnected(), "NATS connection is closed");
        assertEquals(ObservedState.STOPPED, session.snapshot().observedState(),
                "graceful STOPPED path retains the durable (no delete)");
        assertTrue(stopped.get(), "status server is stopped");
    }

    @Test
    void returnsWhenSessionDoesNotTerminateWithinTimeout() {
        // an undriven session never consumes the Shutdown event, so it never terminates
        var session = new ListenerSession("tool-a", new FakeNatsLink(), 0);
        var stopped = new AtomicBoolean();
        var coord = new ShutdownCoordinator(session, () -> stopped.set(true), Duration.ofMillis(150));

        long t0 = System.nanoTime();
        coord.shutdown();
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        assertFalse(session.isTerminated());
        assertTrue(stopped.get(), "status server is stopped even when drain times out");
        assertTrue(elapsedMs >= 120, "waited up to the bound, elapsed=" + elapsedMs);
        assertTrue(elapsedMs < 3000, "returned on timeout instead of hanging, elapsed=" + elapsedMs);
    }

    @Test
    void secondShutdownIsNoOp() {
        var link = new FakeNatsLink();
        var session = activeSession(link);
        var stops = new AtomicInteger();
        var coord = new ShutdownCoordinator(session, stops::incrementAndGet, Duration.ofSeconds(5));

        coord.shutdown();
        coord.shutdown();

        assertEquals(1, stops.get(), "second shutdown must be a no-op: no second loop, no second stop");
        assertTrue(session.isTerminated());
    }
}
