package dc.listener.session;

import java.util.concurrent.atomic.AtomicLong;

/** Data Pipeline 替身：人工延遲 + 冪等計數（redelivery 只表現為重複計數，spec §4.5）。 */
public final class PipelineStub {
    private final long delayMs;
    private final AtomicLong admitted = new AtomicLong();

    public PipelineStub(long delayMs) { this.delayMs = delayMs; }

    public void process(InFlightMsg m) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        admitted.incrementAndGet();
    }

    public long admitted() { return admitted.get(); }
}
