package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 測試替身：可注入 connect 失敗次數、可餵訊息、記錄 ack 與 deleteConsumer。
 *  欄位 public：ReconcilerTest（dc.listener.reconcile package）也要直接讀。 */
public final class FakeNatsLink implements NatsLink {
    public final ConcurrentLinkedDeque<String> messages = new ConcurrentLinkedDeque<>();
    public final List<String> acked = new CopyOnWriteArrayList<>();
    public final AtomicInteger connectCalls = new AtomicInteger();
    public volatile int connectFailures;
    public volatile String failReason = "MESSAGING_ENDPOINT_UNREACHABLE";
    public volatile boolean connected;
    public volatile boolean consumerDeleted;
    public volatile boolean failAcksWhenDisconnected;

    @Override public void connect(SessionSpec spec) throws LinkException {
        connectCalls.incrementAndGet();
        if (connectFailures > 0) {
            connectFailures--;
            throw new LinkException(failReason, null);
        }
        connected = true;
    }

    @Override public List<InFlightMsg> fetch(int max, Duration wait) throws LinkException {
        if (!connected) throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", null);
        var out = new ArrayList<InFlightMsg>();
        String d;
        while (out.size() < max && (d = messages.poll()) != null) out.add(new InFlightMsg(d, null));
        if (out.isEmpty()) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return out;
    }

    @Override public void ack(InFlightMsg m) throws LinkException {
        if (failAcksWhenDisconnected && !connected) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", null);
        }
        acked.add(m.data());
    }
    @Override public long pending() { return messages.size(); }
    @Override public boolean isConnected() { return connected; }
    @Override public void deleteConsumer(SessionSpec spec) { consumerDeleted = true; }
    @Override public void close() { connected = false; }
}
