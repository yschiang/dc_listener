package dc.listener.session;

import dc.listener.Await;
import dc.listener.spec.DesiredState;
import dc.listener.spec.SessionSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ListenerSessionTest {

    static SessionSpec spec(DesiredState d, Duration retryInterval, int maxAttempts) {
        return new SessionSpec("t", d, "v1", "tool.t.events", "dur-t",
                retryInterval, maxAttempts, Duration.ofSeconds(5));
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
    void terminateDeletesConsumerAndExits() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        s.deliver(new Event.Terminate());
        Await.until(s::isTerminated, 3000);
        assertTrue(link.consumerDeleted);
        assertFalse(link.connected);
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
