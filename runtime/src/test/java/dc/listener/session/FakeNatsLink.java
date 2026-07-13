package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 測試替身：可注入 connect 失敗次數、可餵訊息、記錄 ack。
 *  每次成功 connect 遞增 generation：fetch 出的 handle 帶當時 generation，ack 一個 generation 不符的 handle
 *  代表在對「上一條連線」的 handle 做 ack（真實 NATS 會拒絕）→ 記到 staleAcks，證明重連後不得 ack 舊 handle。
 *  欄位 public：ReconcilerTest（dc.listener.reconcile package）也要直接讀。 */
public final class FakeNatsLink implements NatsLink {
    public final ConcurrentLinkedDeque<String> messages = new ConcurrentLinkedDeque<>();
    public final List<String> acked = new CopyOnWriteArrayList<>();
    public final List<String> staleAcks = new CopyOnWriteArrayList<>();
    public final List<String> connectedVersions = new CopyOnWriteArrayList<>();
    public final AtomicInteger connectCalls = new AtomicInteger();
    public volatile int connectFailures;
    public volatile String failReason = "MESSAGING_ENDPOINT_UNREACHABLE";
    public volatile boolean connected;
    public volatile boolean failAcksWhenDisconnected;
    private final AtomicInteger generation = new AtomicInteger();

    @Override public void connect(SessionSpec spec) throws LinkException {
        connectCalls.incrementAndGet();
        connectedVersions.add(spec.configVersion());
        if (connectFailures > 0) {
            connectFailures--;
            throw new LinkException(failReason, null);
        }
        generation.incrementAndGet();
        connected = true;
    }

    @Override public List<InFlightMsg> fetch(int max, Duration wait) throws LinkException {
        if (!connected) throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", null);
        var out = new ArrayList<InFlightMsg>();
        int gen = generation.get();
        String d;
        while (out.size() < max && (d = messages.poll()) != null) out.add(new InFlightMsg(d, gen));
        if (out.isEmpty()) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return out;
    }

    @Override public void ack(InFlightMsg m) throws LinkException {
        if (failAcksWhenDisconnected && !connected) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", null);
        }
        if (!(m.handle() instanceof Integer g) || g != generation.get()) {
            staleAcks.add(m.data());   // handle from a previous connection → real NATS would reject
            return;
        }
        acked.add(m.data());
    }
    @Override public long pending() { return messages.size(); }
    @Override public boolean isConnected() { return connected; }
    @Override public void close() { connected = false; }
}
