package dc.listener.reconcile;

import dc.listener.Await;
import dc.listener.session.FakeNatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.session.ObservedState;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ReconcilerTest {

    final Map<String, FakeNatsLink> links = new ConcurrentHashMap<>();
    final Reconciler rec = new Reconciler(Path.of("/nonexistent"),
            name -> new ListenerSession(name, links.computeIfAbsent(name, n -> new FakeNatsLink()), 0));

    static String yaml(String name, String desired, String subject, String ver) {
        return """
            sessions:
              %s:
                desiredState: %s
                configVersion: %s
                config:
                  subject: %s
                  durable: dur-%s
            """.formatted(name, desired, ver, subject, name);
    }

    @Test
    void addStartsSessionAndConverges() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        assertNotNull(s);
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
    }

    @Test
    void unchangedSpecIsNotRedelivered() {
        String y = yaml("tool-a", "RUNNING", "tool.a.events", "v1");
        rec.apply(y);
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply(y);
        rec.apply(y);
        assertEquals(1, links.get("tool-a").connectCalls.get());
    }

    @Test
    void configChangeTriggersReconnectWithNewVersion() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events.v2", "v2"));
        Await.until(() -> "v2".equals(s.snapshot().appliedConfigVersion())
                && s.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        assertEquals(2, links.get("tool-a").connectCalls.get());
    }

    @Test
    void removedEntryTerminatesSessionAndDeletesConsumer() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply("sessions: {}");
        Await.until(s::isTerminated, 3000);
        assertTrue(links.get("tool-a").consumerDeleted);
        rec.apply("sessions: {}");
        assertFalse(rec.sessions().containsKey("tool-a"));
    }

    @Test
    void brokenYamlKeepsLastGoodDeclaration() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply("sessions: [broken");
        assertNotNull(rec.specError());
        assertTrue(rec.sessions().containsKey("tool-a"));
        assertEquals(ObservedState.ACTIVE, s.snapshot().observedState());
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        assertNull(rec.specError());
    }

    @Test
    void invalidEntryCreatesFailedSession() {
        rec.apply("""
            sessions:
              tool-x:
                desiredState: RUNNING
                configVersion: v1
                config:
                  durable: dur-x
            """);
        var s = rec.sessions().get("tool-x");
        assertNotNull(s);
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 2000);
        assertTrue(s.snapshot().reason().startsWith("INVALID_SPEC"));
    }

    @Test
    void rapidRemoveAndReaddReplacesTerminatingSession() {
        var link = new FakeNatsLink();
        link.messages.addAll(List.of("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"));
        var local = new Reconciler(Path.of("/nonexistent"),
                name -> new ListenerSession(name, link, 100));
        String y = yaml("tool-a", "RUNNING", "tool.a.events", "v1");

        local.apply(y);
        var original = local.sessions().get("tool-a");
        Await.until(() -> original.snapshot().admittedCount() >= 1, 2000);
        local.apply("sessions: {}");
        Await.until(() -> original.snapshot().observedState() == ObservedState.DRAINING, 2000);
        local.apply(y);

        Await.until(() -> {
            var replacement = local.sessions().get("tool-a");
            return replacement != null && replacement != original
                    && replacement.snapshot().observedState() == ObservedState.ACTIVE;
        }, 5000);
        assertTrue(original.isTerminated());
    }

    @Test
    void retryAfterFactoryFailureDoesNotLoseLaterSessionChange() {
        var localLinks = new ConcurrentHashMap<String, FakeNatsLink>();
        var failToolAOnce = new AtomicBoolean(true);
        var local = new Reconciler(Path.of("/nonexistent"), name -> {
            if (name.equals("tool-a") && failToolAOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("injected factory failure");
            }
            return new ListenerSession(name,
                    localLinks.computeIfAbsent(name, n -> new FakeNatsLink()), 0);
        });
        local.apply(yaml("tool-b", "RUNNING", "tool.b.events", "v1"));
        var toolB = local.sessions().get("tool-b");
        Await.until(() -> toolB.snapshot().observedState() == ObservedState.ACTIVE, 2000);

        String desired = """
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-tool-a
              tool-b:
                desiredState: RUNNING
                configVersion: v2
                config:
                  subject: tool.b.events.v2
                  durable: dur-tool-b
            """;
        assertThrows(IllegalStateException.class, () -> local.apply(desired));
        local.apply(desired);

        Await.until(() -> local.sessions().get("tool-a") != null
                && local.sessions().get("tool-a").snapshot().observedState() == ObservedState.ACTIVE, 2000);
        Await.until(() -> "v2".equals(toolB.snapshot().appliedConfigVersion())
                && toolB.snapshot().observedState() == ObservedState.ACTIVE, 3000);
    }

    @Test
    void replacementFactoryFailureIsRetriedWithoutAnotherReload() {
        var attempts = new AtomicInteger();
        var local = new Reconciler(Path.of("/nonexistent"), name -> {
            int attempt = attempts.incrementAndGet();
            if (attempt == 2) throw new IllegalStateException("injected replacement failure");
            var link = new FakeNatsLink();
            if (attempt == 1) {
                link.messages.addAll(List.of("m1", "m2", "m3", "m4", "m5",
                        "m6", "m7", "m8", "m9", "m10"));
            }
            return new ListenerSession(name, link, 100);
        });
        String y = yaml("tool-a", "RUNNING", "tool.a.events", "v1");

        local.apply(y);
        var original = local.sessions().get("tool-a");
        Await.until(() -> original.snapshot().admittedCount() >= 1, 2000);
        local.apply("sessions: {}");
        Await.until(() -> original.snapshot().observedState() == ObservedState.DRAINING, 2000);
        local.apply(y);

        Await.until(() -> {
            var replacement = local.sessions().get("tool-a");
            return replacement != null && replacement != original
                    && replacement.snapshot().observedState() == ObservedState.ACTIVE;
        }, 5000);
        assertEquals(3, attempts.get());
    }
}
