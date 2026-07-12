package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Instant;

/** 唯一權威的 lifecycle 狀態（guidance 8.1）。純轉移邏輯、零 I/O；動作由 ListenerSession loop 執行。 */
public final class SessionStateMachine {

    private final String name;
    private ObservedState state = ObservedState.STANDBY;
    private SessionSpec spec;        // 已生效（applied）
    private SessionSpec pending;     // 等 drain 完成才套用
    private int failures;            // 連續 connect 失敗次數（含初次）
    private String reason = "";
    private Instant lastTransition = Instant.now();
    private boolean terminating;     // YAML entry 已刪除 → STOPPED 後刪 consumer、結束 thread

    public SessionStateMachine(String name) { this.name = name; }

    public synchronized void onEvent(Event e) {
        switch (e) {
            case Event.SpecChanged sc -> onSpec(sc.spec());
            case Event.SpecInvalid si -> {
                pending = null;
                reason = "INVALID_SPEC: " + si.error();
                moveTo(ObservedState.FAILED);
            }
            case Event.ConnectOk ok -> {
                if (state == ObservedState.CONNECTING) {
                    failures = 0;
                    reason = "";
                    moveTo(ObservedState.ACTIVE);
                }
            }
            case Event.ConnectFailed f -> onConnectFailed(f.reason());
            case Event.FetchError fe -> {
                if (state == ObservedState.ACTIVE) {
                    failures = 1;
                    reason = fe.reason();
                    moveTo(ObservedState.DEGRADED);
                }
            }
            case Event.RetryTick rt -> {
                if (state == ObservedState.DEGRADED) moveTo(ObservedState.CONNECTING);
            }
            case Event.DrainComplete dc -> onDrainComplete();
            case Event.DrainTimeout dt -> onDrainTimeout();
            case Event.Terminate x -> onTerminate();
        }
    }

    private void onSpec(SessionSpec next) {
        switch (state) {
            case ACTIVE -> {
                if (hotOnly(spec, next)) spec = next;              // retry/drainTimeout 熱生效（spec §5.1）
                else { pending = next; moveTo(ObservedState.DRAINING); }
            }
            case DRAINING -> pending = next;                       // 完成當前轉移再收斂（spec §4.2）
            default -> { adopt(next); converge(); }                // FAILED 的重置、DEGRADED 的立即中斷都走這裡
        }
    }

    private void adopt(SessionSpec next) {
        spec = next;
        pending = null;
        failures = 0;
        reason = "";
    }

    private void converge() {
        switch (spec.desiredState()) {
            case RUNNING -> moveTo(ObservedState.CONNECTING);
            case STANDBY -> moveTo(ObservedState.STANDBY);
            case STOPPED -> moveTo(ObservedState.STOPPED);
        }
    }

    private void onConnectFailed(String r) {
        if (state != ObservedState.CONNECTING) return;
        failures++;
        if (failures > spec.maxAttempts()) {                       // 初次 + maxAttempts 次重試全失敗
            reason = "RETRY_EXHAUSTED";
            moveTo(ObservedState.FAILED);
        } else {
            reason = r;
            moveTo(ObservedState.DEGRADED);
        }
    }

    private void onDrainComplete() {
        if (state != ObservedState.DRAINING) return;
        if (terminating) { moveTo(ObservedState.STOPPED); return; }
        if (pending != null) adopt(pending);
        converge();
    }

    private void onDrainTimeout() {
        if (state != ObservedState.DRAINING) return;
        reason = "DRAIN_TIMEOUT";
        if (terminating) moveTo(ObservedState.STOPPED);
        else { pending = null; moveTo(ObservedState.FAILED); }
    }

    private void onTerminate() {
        terminating = true;
        switch (state) {
            case ACTIVE -> moveTo(ObservedState.DRAINING);
            case DRAINING -> { }
            default -> moveTo(ObservedState.STOPPED);
        }
    }

    private boolean hotOnly(SessionSpec a, SessionSpec b) {
        return a.desiredState() == b.desiredState()
                && a.subject().equals(b.subject())
                && a.durable().equals(b.durable())
                && a.configVersion().equals(b.configVersion());
    }

    private void moveTo(ObservedState next) {
        if (state != next) {
            state = next;
            lastTransition = Instant.now();
            System.out.println("[" + name + "] -> " + next + (reason.isEmpty() ? "" : " (" + reason + ")"));
        }
    }

    public synchronized ObservedState state() { return state; }
    public synchronized SessionSpec spec() { return spec; }
    public synchronized String reason() { return reason; }
    public synchronized int retryAttempt() { return failures; }
    public synchronized boolean terminating() { return terminating; }
    public synchronized Instant lastTransitionTime() { return lastTransition; }
    public synchronized String declaredConfigVersion() {
        var d = pending != null ? pending : spec;
        return d == null ? null : d.configVersion();
    }
    public synchronized String appliedConfigVersion() { return spec == null ? null : spec.configVersion(); }
    public synchronized dc.listener.spec.DesiredState declaredDesired() {
        var d = pending != null ? pending : spec;
        return d == null ? null : d.desiredState();
    }
}
