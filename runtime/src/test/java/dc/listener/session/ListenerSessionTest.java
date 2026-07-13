package dc.listener.session;

import dc.listener.Await;
import dc.listener.spec.DesiredState;
import dc.listener.spec.SessionSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ListenerSessionTest {

    static SessionSpec spec(DesiredState d, Duration retryInterval, int maxAttempts) {
        return new SessionSpec("t", d, "v1", "tool.t.events", "dur-t",
                retryInterval, maxAttempts, Duration.ofSeconds(5));
    }

    static SessionSpec spec(DesiredState d, String ver, String subject, String durable, Duration retry) {
        return new SessionSpec("t", d, ver, subject, durable, retry, 10, Duration.ofSeconds(5));
    }

    @Test
    void connectsConsumesAndAcks() {
        var link = new FakeNatsLink();
        link.messages.addAll(List.of("m1", "m2", "m3"));
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        Await.until(() -> s.snapshot().admittedCount() == 3, 2000);
        assertEquals(List.of("m1", "m2", "m3"), link.acked);
        var st = s.snapshot();
        assertTrue(st.admissionAllowed());
        assertTrue(st.connectionReady());
        assertTrue(st.consumerReady());
    }

    @Test
    void degradedRetriesThenEscalatesToFailed() {
        var link = new FakeNatsLink();
        link.connectFailures = 99;
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, Duration.ofMillis(20), 2)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 3000);
        assertEquals("RETRY_EXHAUSTED", s.snapshot().reason());
        assertEquals(3, link.connectCalls.get());   // 初次 + 2 次重試
    }

    @Test
    void drainsToStandbyAndStopsConsuming() {
        var link = new FakeNatsLink();
        for (int i = 0; i < 20; i++) link.messages.add("m" + i);
        var s = new ListenerSession("t", link, 20);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().admittedCount() >= 2, 3000);
        s.deliver(new Event.SpecChanged(spec(DesiredState.STANDBY, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.STANDBY, 3000);
        assertTrue(link.connected, "STANDBY 保留連線（spec §4.1）");
        int ackedAfterDrain = link.acked.size();
        link.messages.add("late");
        try { Thread.sleep(200); } catch (InterruptedException e) { throw new RuntimeException(e); }
        assertEquals(ackedAfterDrain, link.acked.size(), "STANDBY 不得再 fetch/ack");
        assertFalse(s.snapshot().admissionAllowed());
    }

    @Test
    void shutdownDrainsClosesAndExitsRetainingDurable() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        s.deliver(new Event.Shutdown());
        Await.until(s::isTerminated, 3000);
        assertFalse(link.connected, "NATS connection closed on shutdown");
        // no deleteConsumer path exists at all — the durable is retained by construction
    }

    @Test
    void rejectsBlankIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new ListenerSession("  ", new FakeNatsLink(), 0));
        assertThrows(IllegalArgumentException.class, () -> new ListenerSession(null, new FakeNatsLink(), 0));
    }

    @Test
    void startIsIdempotent() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.start();   // second start must not spawn a second loop
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        assertEquals("t", s.name());
        assertEquals(1, link.connectCalls.get(), "one loop → exactly one connect");
    }

    @Test
    void explicitSpecInvalidThenRestoredActiveDiscardsStaleHandles() {
        var link = new FakeNatsLink();
        for (int i = 0; i < 10; i++) link.messages.add("stale" + i);
        var s = new ListenerSession("t", link, 100);   // slow batch: handles stay in-flight
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().admittedCount() >= 1, 3000);

        s.deliver(new Event.SpecInvalid("declaration missing"));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 3000);
        Await.until(() -> !link.connected, 2000);
        assertTrue(link.acked.size() < 10, "in-flight handles must be discarded for redelivery, not all acked");

        // restore with the original durable → same session instance recovers
        link.messages.add("fresh");
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events", "dur-t", null)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        Await.until(() -> link.acked.contains("fresh"), 3000);

        assertTrue(link.staleAcks.isEmpty(), "no handle from the previous connection may be acked after reconnect");
        assertEquals(link.acked.size(), Set.copyOf(link.acked).size(), "no handle acked twice");
        assertFalse(s.isTerminated(), "actor survives");
    }

    @Test
    void durableMutatingSpecChangeFailsInvalidThenOriginalDurableRestores() {
        var link = new FakeNatsLink();
        for (int i = 0; i < 10; i++) link.messages.add("stale" + i);
        var s = new ListenerSession("t", link, 100);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));   // durable dur-t
        Await.until(() -> s.snapshot().admittedCount() >= 1, 3000);

        int connectsBefore = link.connectCalls.get();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events", "dur-t-NEW", null)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 3000);
        assertTrue(s.snapshot().reason().startsWith("INVALID_SPEC"), s.snapshot().reason());
        Await.until(() -> !link.connected, 2000);
        assertEquals(connectsBefore, link.connectCalls.get(), "durable mutation must not reconnect / create a consumer");

        // restore the latched durable → same instance recovers, stale handles never acked
        link.messages.add("fresh");
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, "v3", "tool.t.events", "dur-t", null)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        Await.until(() -> link.acked.contains("fresh"), 3000);
        assertTrue(link.staleAcks.isEmpty(), "no stale handle from before the failure may be acked");
        assertEquals("v3", s.snapshot().appliedConfigVersion());
        assertFalse(s.isTerminated());
    }

    @Test
    void drainOutageReconnectsAppliedThenAppliesPending() {
        var link = new FakeNatsLink();
        link.failAcksWhenDisconnected = true;
        for (int i = 0; i < 30; i++) link.messages.add("m" + i);
        var s = new ListenerSession("t", link, 60);   // slow enough to stay draining while the outage lands
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, "v1", "tool.t.events", "dur-t",
                Duration.ofMillis(100))));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        Await.until(() -> s.snapshot().admittedCount() >= 1, 3000);

        // pending config change (same durable) → DRAINING
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events.v2", "dur-t",
                Duration.ofMillis(100))));
        Await.until(() -> s.snapshot().observedState() == ObservedState.DRAINING, 3000);

        // outage DURING draining → ack fails → DEGRADED
        link.connected = false;
        Await.until(() -> s.snapshot().observedState() == ObservedState.DEGRADED, 3000);

        // reconnect uses the APPLIED config (v1), returns to DRAINING, then completes the pending v2 transition
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE
                && "v2".equals(s.snapshot().appliedConfigVersion()), 6000);
        assertEquals(List.of("v1", "v1", "v2"), link.connectedVersions,
                "reconnect after the drain outage must use the applied config, not the pending one");
    }

    @Test
    void invalidSpecFailsAndClosesConnection() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        s.deliver(new Event.SpecInvalid("missing subject"));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 2000);
        Await.until(() -> !link.connected, 2000);
        assertFalse(s.snapshot().configurationReady());
    }

    @Test
    void drainTimeoutDiscardsInFlightAndFails() {
        var link = new FakeNatsLink();
        for (int i = 0; i < 10; i++) link.messages.add("m" + i);
        var s = new ListenerSession("t", link, 150);
        s.start();
        s.deliver(new Event.SpecChanged(new SessionSpec("t", DesiredState.RUNNING, "v1",
                "tool.t.events", "dur-t", null, 10, Duration.ofMillis(300))));
        Await.until(() -> s.snapshot().admittedCount() >= 1, 3000);
        s.deliver(new Event.SpecChanged(new SessionSpec("t", DesiredState.STANDBY, "v1",
                "tool.t.events", "dur-t", null, 10, Duration.ofMillis(300))));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 5000);
        assertEquals("DRAIN_TIMEOUT", s.snapshot().reason());
        assertTrue(link.acked.size() < 10, "剩餘 in-flight 不得 ack（將由 redelivery 補）");
    }

    @Test
    void exponentialBackoffPathEscalates() {
        var link = new FakeNatsLink();
        link.connectFailures = 99;
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(new SessionSpec("t", DesiredState.RUNNING, "v1",
                "tool.t.events", "dur-t", null, 2, Duration.ofSeconds(5))));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 8000);
        assertEquals("RETRY_EXHAUSTED", s.snapshot().reason());
        assertEquals(3, link.connectCalls.get());
    }

    @Test
    void disconnectedFetchDegradesThenReconnects() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, Duration.ofMillis(200), 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);

        link.connected = false;

        Await.until(() -> s.snapshot().observedState() == ObservedState.DEGRADED, 2000);
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE
                && link.connectCalls.get() >= 2, 3000);
    }

    @Test
    void disconnectedAckDoesNotKillSessionThread() {
        var link = new FakeNatsLink();
        link.failAcksWhenDisconnected = true;
        link.messages.add("m1");
        var s = new ListenerSession("t", link, 500);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, Duration.ofMillis(200), 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        Await.until(link.messages::isEmpty, 2000);

        link.connected = false;

        Await.until(() -> s.snapshot().observedState() == ObservedState.DEGRADED, 2000);
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE
                && link.connectCalls.get() >= 2, 3000);
        link.messages.add("m2");
        Await.until(() -> s.snapshot().admittedCount() >= 2, 2000);
        assertTrue(link.acked.contains("m2"), "actor thread must continue after reconnect");
    }
}
