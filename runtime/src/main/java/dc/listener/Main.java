package dc.listener;

import dc.listener.reconcile.FileWatcher;
import dc.listener.reconcile.SingleSessionReconciler;
import dc.listener.session.JnatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.status.StatusServer;

/**
 * 組裝殼：一個 process = 一個 ListenerSession（ADR-0001）。env 只讀靜態設定；邏輯全在 spec/session/reconcile。
 * SESSION_NAME 選出 sessions.yaml 裡唯一的一條宣告，作為不可變 process 身分。
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        StartupConfig cfg = StartupConfig.fromEnv(System.getenv());

        var session = new ListenerSession(cfg.sessionName(), new JnatsLink(cfg.natsUrl()), cfg.processDelayMs());
        var reconciler = new SingleSessionReconciler(cfg.sessionsFile(), session);
        reconciler.reload();
        new FileWatcher(cfg.sessionsFile(), reconciler::applySnapshot).start();
        var status = new StatusServer(cfg.statusPort(), reconciler);
        status.start();

        // SIGTERM/JVM 關閉：非破壞性 drain → close NATS → 結束 loop（durable 保留），再停 status server。
        // 不依賴設定消失，也不會再起第二條 loop。
        var coordinator = new ShutdownCoordinator(session, status::stop, cfg.shutdownTimeout());
        Runtime.getRuntime().addShutdownHook(new Thread(coordinator::shutdown, "shutdown"));

        System.out.println("listener-runtime up | session=" + cfg.sessionName()
                + " | sessions=" + cfg.sessionsFile() + " | nats=" + cfg.natsUrl()
                + " | status=:" + cfg.statusPort() + "/status");
        Thread.currentThread().join();
    }
}
