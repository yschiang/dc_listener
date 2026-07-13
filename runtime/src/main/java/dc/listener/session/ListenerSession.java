package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 一個 session = 專屬 NatsLink + 專屬 virtual thread + mailbox；跨 session 零共享可變狀態（P3）。 */
public final class ListenerSession {
    private static final int FETCH_BATCH = 10;

    private final String name;
    private final NatsLink link;
    private final PipelineStub pipeline;
    private final SessionStateMachine machine;
    private final AdmissionGate gate;
    private final BlockingQueue<Event> mailbox = new LinkedBlockingQueue<>();
    private final Deque<InFlightMsg> inFlight = new ArrayDeque<>();
    private volatile long pendingCount = -1;
    private volatile boolean terminated;
    private final AtomicBoolean started = new AtomicBoolean();
    private Instant drainDeadline;

    public ListenerSession(String name, NatsLink link, long processDelayMs) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("session name must be non-blank");
        this.name = name;
        this.link = link;
        this.pipeline = new PipelineStub(processDelayMs);
        this.machine = new SessionStateMachine(name);
        this.gate = new AdmissionGate(machine);
    }

    /** 冪等：重複呼叫只會有一條 actor loop（ownership 的一 process 一 loop）。 */
    public void start() {
        if (started.compareAndSet(false, true)) {
            Thread.ofVirtual().name("session-" + name).start(this::run);
        }
    }

    public void deliver(Event e) { mailbox.add(e); }

    /** 不可變身分，供 ownership 驗證。 */
    public String name() { return name; }

    public boolean isTerminated() { return terminated; }

    private void run() {
        try {
            while (true) {
                ObservedState s = machine.state();
                switch (s) {
                    case STANDBY, STOPPED, FAILED -> {
                        if (s == ObservedState.STOPPED && machine.shuttingDown()) {
                            link.close();          // durable 保留（ADR-0001：無 finalizer 前不刪 consumer）
                            terminated = true;
                            return;
                        }
                        if ((s == ObservedState.STOPPED || s == ObservedState.FAILED) && link.isConnected()) {
                            link.close();
                        }
                        Event e = mailbox.poll(2, TimeUnit.SECONDS);
                        if (e != null) handle(e); else refreshPending();
                    }
                    case CONNECTING -> {
                        link.close();   // 重連 = 與初次完全相同的路徑（spec §4.2）
                        try {
                            link.connect(machine.spec());
                            handle(new Event.ConnectOk());
                        } catch (LinkException ex) {
                            handle(new Event.ConnectFailed(ex.reasonCode()));
                        }
                    }
                    case ACTIVE -> {
                        Event e = mailbox.poll();
                        if (e != null) { handle(e); continue; }
                        if (inFlight.isEmpty()) {
                            try {
                                inFlight.addAll(link.fetch(FETCH_BATCH, Duration.ofSeconds(1)));
                                refreshPending();
                            } catch (LinkException ex) {
                                handle(new Event.FetchError(ex.reasonCode()));
                                continue;
                            }
                        }
                        if (!inFlight.isEmpty() && gate.admits()) {
                            try {
                                processOne();
                            } catch (LinkException ex) {
                                inFlight.clear();
                                handle(new Event.FetchError(ex.reasonCode()));
                            }
                        }
                    }
                    case DRAINING -> {
                        if (drainDeadline == null) {
                            drainDeadline = Instant.now().plus(machine.spec().drainTimeout());
                        }
                        Event e = mailbox.poll();
                        if (e != null) { handle(e); continue; }
                        if (inFlight.isEmpty()) {
                            handle(new Event.DrainComplete());
                        } else if (Instant.now().isAfter(drainDeadline)) {
                            inFlight.clear();   // 未 ack → 之後 redelivery（at-least-once）
                            handle(new Event.DrainTimeout());
                        } else {
                            try {
                                processOne();
                            } catch (LinkException ex) {
                                inFlight.clear();
                                handle(new Event.FetchError(ex.reasonCode()));
                            }
                        }
                    }
                    case DEGRADED -> {
                        Event e = mailbox.poll(retryDelay().toMillis(), TimeUnit.MILLISECONDS);
                        handle(e != null ? e : new Event.RetryTick());
                    }
                }
                if (machine.state() != ObservedState.DRAINING) drainDeadline = null;
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** 唯一的事件入口：任何轉移到 FAILED 都在 link 關閉前清空 in-flight handle，
     *  避免恢復後對「上一條連線」的 handle 做 ack（含 durable-mutating SpecChanged 的內部拒絕）。 */
    private void handle(Event e) {
        machine.onEvent(e);
        if (machine.state() == ObservedState.FAILED) inFlight.clear();
    }

    private void processOne() throws LinkException {
        InFlightMsg m = inFlight.removeFirst();
        pipeline.process(m);
        link.ack(m);
    }

    private Duration retryDelay() {
        SessionSpec sp = machine.spec();
        if (sp.retryInterval() != null) return sp.retryInterval();
        int f = Math.max(machine.retryAttempt(), 1);
        return Duration.ofSeconds(Math.min(1L << (f - 1), 30));   // 指數退避 1s→30s（spec §4.2）
    }

    private void refreshPending() {
        try { pendingCount = link.pending(); } catch (Exception e) { /* 保留舊值 */ }
    }

    public SessionStatus snapshot() {
        var v = machine.view();
        return new SessionStatus(
                name,
                v.spec() == null ? null : v.spec().subject(),
                v.declaredDesired(),
                v.state(),
                v.declaredConfigVersion(),
                v.appliedConfigVersion(),
                v.spec() != null && !v.reason().startsWith("INVALID_SPEC"),
                link.isConnected(),
                v.state() == ObservedState.ACTIVE || v.state() == ObservedState.DRAINING,
                v.state() == ObservedState.ACTIVE,
                v.reason(),
                v.lastTransitionTime(),
                pipeline.admitted(),
                pendingCount,
                v.retryAttempt());
    }
}
