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
}
