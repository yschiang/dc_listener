package dc.listener.session;

import dc.listener.spec.DesiredState;
import dc.listener.spec.SessionSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static dc.listener.session.ObservedState.*;
import static org.junit.jupiter.api.Assertions.*;

class SessionStateMachineTest {

    static SessionSpec spec(DesiredState d, String ver, String subject, int maxAttempts) {
        return new SessionSpec("t", d, ver, subject, "dur-t", null, maxAttempts, Duration.ofSeconds(30));
    }

    static SessionSpec running(String ver) { return spec(DesiredState.RUNNING, ver, "tool.t.events", 10); }

    static SessionStateMachine activeMachine() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectOk());
        return m;
    }

    @Test void startsInStandby() {
        assertEquals(STANDBY, new SessionStateMachine("t").state());
    }

    @Test void runningSpecConnects() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        assertEquals(CONNECTING, m.state());
        assertEquals("v1", m.appliedConfigVersion());
    }

    @Test void standbySpecStaysStandby() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "s", 10)));
        assertEquals(STANDBY, m.state());
    }

    @Test void connectOkActivates() {
        var m = activeMachine();
        assertEquals(ACTIVE, m.state());
        assertEquals(0, m.retryAttempt());
        assertEquals("", m.reason());
    }

    @Test void connectFailedDegrades() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("MESSAGING_ENDPOINT_UNREACHABLE"));
        assertEquals(DEGRADED, m.state());
        assertEquals(1, m.retryAttempt());
        assertEquals("MESSAGING_ENDPOINT_UNREACHABLE", m.reason());
    }

    @Test void retryTickReconnects() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));
        m.onEvent(new Event.RetryTick());
        assertEquals(CONNECTING, m.state());
    }

    @Test void retriesEscalateToFailed() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v1", "s", 2)));
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 初次失敗
        m.onEvent(new Event.RetryTick());
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 重試 1
        m.onEvent(new Event.RetryTick());
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 重試 2 → 用盡
        assertEquals(FAILED, m.state());
        assertEquals("RETRY_EXHAUSTED", m.reason());
    }

    @Test void specChangeInterruptsDegraded() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("MESSAGING_ENDPOINT_UNREACHABLE"));
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
        assertEquals(0, m.retryAttempt());
        assertEquals("v2", m.appliedConfigVersion());
    }

    @Test void activeConfigChangeDrains() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events.v2", 10)));
        assertEquals(DRAINING, m.state());
        assertEquals("v2", m.declaredConfigVersion());
        assertEquals("v1", m.appliedConfigVersion());
    }

    @Test void drainCompleteAppliesPending() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events.v2", 10)));
        m.onEvent(new Event.DrainComplete());
        assertEquals(CONNECTING, m.state());
        assertEquals("v2", m.appliedConfigVersion());
    }

    @Test void activeToStandbyViaDrain() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "tool.t.events", 10)));
        assertEquals(DRAINING, m.state());
        m.onEvent(new Event.DrainComplete());
        assertEquals(STANDBY, m.state());
    }

    @Test void hotFieldChangeStaysActive() {
        var m = activeMachine();
        var hot = new SessionSpec("t", DesiredState.RUNNING, "v1", "tool.t.events", "dur-t",
                Duration.ofSeconds(5), 99, Duration.ofSeconds(30));
        m.onEvent(new Event.SpecChanged(hot));
        assertEquals(ACTIVE, m.state());
        assertEquals(99, m.spec().maxAttempts());
    }

    @Test void drainTimeoutFails() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "tool.t.events", 10)));
        m.onEvent(new Event.DrainTimeout());
        assertEquals(FAILED, m.state());
        assertEquals("DRAIN_TIMEOUT", m.reason());
    }

    @Test void specChangeDuringDrainingWaits() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "s2", 10)));
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v3", "s3", 10)));
        assertEquals(DRAINING, m.state());
        assertEquals("v3", m.declaredConfigVersion());
        m.onEvent(new Event.DrainComplete());
        assertEquals("v3", m.appliedConfigVersion());
    }

    @Test void invalidSpecFailsFromAnyState() {
        var m = activeMachine();
        m.onEvent(new Event.SpecInvalid("missing subject"));
        assertEquals(FAILED, m.state());
        assertTrue(m.reason().startsWith("INVALID_SPEC"));
    }

    @Test void failedRecoversOnSpecChange() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecInvalid("missing subject"));
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
        assertEquals("", m.reason());
    }

    @Test void fetchErrorDegrades() {
        var m = activeMachine();
        m.onEvent(new Event.FetchError("MESSAGING_ENDPOINT_UNREACHABLE"));
        assertEquals(DEGRADED, m.state());
        assertEquals(1, m.retryAttempt());
    }

    @Test void terminateFromActiveDrainsThenStops() {
        var m = activeMachine();
        m.onEvent(new Event.Terminate());
        assertEquals(DRAINING, m.state());
        assertTrue(m.terminating());
        m.onEvent(new Event.DrainComplete());
        assertEquals(STOPPED, m.state());
    }

    @Test void terminateFromStandbyStopsImmediately() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.Terminate());
        assertEquals(STOPPED, m.state());
        assertTrue(m.terminating());
    }

    @Test void stoppedRestartsOnRunningSpec() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STOPPED, "v1", "s", 10)));
        assertEquals(STOPPED, m.state());
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
    }
}
