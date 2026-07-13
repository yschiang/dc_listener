package dc.listener.reconcile;

import dc.listener.Await;
import dc.listener.session.FakeNatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.session.ObservedState;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SingleSessionReconcilerTest {

    final FakeNatsLink link = new FakeNatsLink();
    final ListenerSession session = new ListenerSession("tool-a", link, 0);
    final SingleSessionReconciler rec = new SingleSessionReconciler(Path.of("/nonexistent"), session);

    static String yaml(String name, String desired, String subject, String ver, String durable) {
        return """
            sessions:
              %s:
                desiredState: %s
                configVersion: %s
                config:
                  subject: %s
                  durable: %s
            """.formatted(name, desired, ver, subject, durable);
    }

    static String twoEntryYaml(String otherDesired, String otherSubject, String otherVer, String otherDurable) {
        return """
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-tool-a
              tool-b:
                desiredState: %s
                configVersion: %s
                config:
                  subject: %s
                  durable: %s
            """.formatted(otherDesired, otherVer, otherSubject, otherDurable);
    }

    @Test
    void constructionStartsTheOwnedSessionAndExposesIdentity() {
        assertEquals("tool-a", rec.sessionName());
        assertSame(session, rec.session());
        // session loop is already running (idempotent start); redundant start() must not add a second loop.
        session.start();
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
    }

    @Test
    void selectsOnlyOwnEntryAndIgnoresOtherEntryChangesIncludingDurable() {
        rec.applySnapshot(twoEntryYaml("RUNNING", "tool.b.events", "v1", "dur-tool-b"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        assertEquals("tool.a.events", session.snapshot().subject());
        int connects = link.connectCalls.get();

        // Mutate the other entry's durable and subject/version repeatedly; tool-a must be unaffected.
        rec.applySnapshot(twoEntryYaml("RUNNING", "tool.b.events.v2", "v2", "dur-tool-b-CHANGED"));
        rec.applySnapshot(twoEntryYaml("STOPPED", "tool.b.events.v3", "v3", "dur-tool-b-CHANGED-AGAIN"));
        try { Thread.sleep(200); } catch (InterruptedException e) { throw new RuntimeException(e); }

        assertEquals(connects, link.connectCalls.get(), "other-entry changes must never reconnect the owned session");
        assertEquals(ObservedState.ACTIVE, session.snapshot().observedState());
    }

    @Test
    void deduplicatesUnchangedSelectedSpec() {
        String y = yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a");
        rec.applySnapshot(y);
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.applySnapshot(y);
        rec.applySnapshot(y);
        assertEquals(1, link.connectCalls.get());
    }

    @Test
    void appliesSelectedDynamicConfigChange() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events.v2", "v2", "dur-tool-a"));
        Await.until(() -> "v2".equals(session.snapshot().appliedConfigVersion())
                && session.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        assertEquals(2, link.connectCalls.get());
    }

    @Test
    void malformedYamlKeepsLastGoodActiveStateAndReportsSpecError() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        assertNull(rec.specError());

        rec.applySnapshot("sessions: [broken");
        assertNotNull(rec.specError());
        assertEquals(ObservedState.ACTIVE, session.snapshot().observedState());

        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        assertNull(rec.specError());
    }

    @Test
    void missingSelectedEntryFailsClosedWithoutTerminatingOrMutatingIdentity() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);

        rec.applySnapshot("sessions: {}");
        Await.until(() -> session.snapshot().observedState() == ObservedState.FAILED, 3000);
        assertTrue(session.snapshot().reason().startsWith("INVALID_SPEC"), session.snapshot().reason());
        assertEquals("declaration missing", session.snapshot().reason().substring("INVALID_SPEC: ".length()),
                session.snapshot().reason());
        assertFalse(session.isTerminated(), "missing declaration must not terminate the process");
        assertSame(session, rec.session());
        assertEquals("tool-a", rec.sessionName(), "identity is never mutated by a config-only disappearance");
    }

    @Test
    void invalidSelectedEntryFailsClosed() {
        rec.applySnapshot("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  durable: dur-tool-a
            """);
        Await.until(() -> session.snapshot().observedState() == ObservedState.FAILED, 2000);
        assertTrue(session.snapshot().reason().startsWith("INVALID_SPEC"), session.snapshot().reason());
        assertFalse(session.isTerminated());
    }

    @Test
    void restoringDeclarationRecoversSameSessionWithNoStaleInFlightHandles() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        link.messages.add("m1");
        link.messages.add("m2");
        Await.until(() -> link.acked.size() >= 2, 2000);

        rec.applySnapshot("sessions: {}");
        Await.until(() -> session.snapshot().observedState() == ObservedState.FAILED, 3000);

        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v2", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        assertSame(session, rec.session(), "recovery reuses the same session instance");

        link.messages.add("m3");
        Await.until(() -> link.acked.contains("m3"), 2000);
        assertTrue(link.staleAcks.isEmpty(), "no handle from a previous connection may be acked after recovery");
    }

    @Test
    void reAppliedSelectedEntryWithChangedDurableFailsClosedWithoutSilentPreFilter() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);

        // same selected entry, later durable mutation → the reconciler must NOT silently pre-filter it;
        // it delivers the SpecChanged and the state machine fails it closed at the ownership boundary.
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v2", "dur-tool-a-NEW"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.FAILED, 3000);
        assertTrue(session.snapshot().reason().startsWith("INVALID_SPEC"), session.snapshot().reason());
        assertSame(session, rec.session(), "no replacement session is created for a durable mutation");
    }

    @Test
    void deduplicatesRepeatedMissingEntryInvalidState() {
        rec.applySnapshot(yaml("tool-a", "RUNNING", "tool.a.events", "v1", "dur-tool-a"));
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);

        rec.applySnapshot("sessions: {}");
        Await.until(() -> session.snapshot().observedState() == ObservedState.FAILED, 3000);
        var firstTransitionTime = session.snapshot().lastTransitionTime();

        rec.applySnapshot("sessions: {}");
        rec.applySnapshot("sessions: {}");
        try { Thread.sleep(200); } catch (InterruptedException e) { throw new RuntimeException(e); }
        assertEquals(firstTransitionTime, session.snapshot().lastTransitionTime(),
                "identical missing-declaration state must not be redelivered");
    }
}
