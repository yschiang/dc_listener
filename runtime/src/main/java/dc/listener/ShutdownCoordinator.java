package dc.listener;

import dc.listener.session.Event;
import dc.listener.session.ListenerSession;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 程序收尾協調器（ADR-0001 §2/§5）：送出非破壞性 {@link Event.Shutdown}（drain → close NATS → 結束 loop，
 * durable 保留、無 finalizer 前不刪 consumer），最多等 {@code timeout} 讓那唯一的 actor loop 結束，再停 status server。
 *
 * <p>逾時即返回（不無限等待）。第二次呼叫是 no-op：不會再送一次 Shutdown、也不會再起任何 loop。
 */
final class ShutdownCoordinator {

    private final ListenerSession session;
    private final Runnable stopStatus;
    private final Duration timeout;
    private final AtomicBoolean started = new AtomicBoolean();

    ShutdownCoordinator(ListenerSession session, Runnable stopStatus, Duration timeout) {
        this.session = session;
        this.stopStatus = stopStatus;
        this.timeout = timeout;
    }

    void shutdown() {
        if (!started.compareAndSet(false, true)) return;   // 第二次 shutdown = 完全 no-op
        session.deliver(new Event.Shutdown());
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!session.isTerminated() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        stopStatus.run();
    }
}
