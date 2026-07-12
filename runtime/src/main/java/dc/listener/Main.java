package dc.listener;

import dc.listener.reconcile.FileWatcher;
import dc.listener.reconcile.Reconciler;
import dc.listener.session.JnatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.status.StatusServer;

import java.nio.file.Path;

/** 組裝殼：env 讀靜態設定（spec §3），邏輯全在 spec/session/reconcile。 */
public final class Main {
    public static void main(String[] args) throws Exception {
        String natsUrl = env("NATS_URL", "nats://localhost:4222");
        Path sessionsFile = Path.of(env("SESSIONS_FILE", "config/sessions.yaml"));
        long processDelayMs = Long.parseLong(env("PROCESS_DELAY_MS", "200"));

        var reconciler = new Reconciler(sessionsFile,
                name -> new ListenerSession(name, new JnatsLink(natsUrl), processDelayMs));
        reconciler.reload();
        new FileWatcher(sessionsFile, reconciler::applySnapshot).start();
        new StatusServer(8080, reconciler).start();
        System.out.println("listener-runtime up | sessions=" + sessionsFile
                + " | nats=" + natsUrl + " | status=:8080/status");
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? def : value;
    }
}
