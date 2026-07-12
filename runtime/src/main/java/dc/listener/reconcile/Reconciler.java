package dc.listener.reconcile;

import dc.listener.session.Event;
import dc.listener.session.ListenerSession;
import dc.listener.spec.SessionSpec;
import dc.listener.spec.SpecParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 宣告 → 現實的單向收斂（spec §5.2）：parse → diff → 投遞事件到 per-session mailbox。
 * 不直接碰 connection；一個 session 卡住不阻塞其他 session 的 reconcile（P3）。
 */
public final class Reconciler {
    private final Path file;
    private final Function<String, ListenerSession> factory;
    private final Map<String, ListenerSession> sessions = new ConcurrentHashMap<>();
    private final Set<String> retiring = ConcurrentHashMap.newKeySet();
    private Map<String, SessionSpec> lastValid = Map.of();
    private Map<String, String> lastInvalid = Map.of();
    private final Map<String, SessionSpec> deliveredValid = new HashMap<>();
    private final Map<String, String> deliveredInvalid = new HashMap<>();
    private volatile String specError;

    public Reconciler(Path file, Function<String, ListenerSession> factory) {
        this.file = file;
        this.factory = factory;
    }

    public synchronized void reload() {
        String text;
        try {
            text = Files.readString(file);
        } catch (IOException e) {
            specError = "cannot read " + file + ": " + e.getMessage();
            return;
        }
        apply(text);
    }

    /** FileWatcher 已確認穩定的同一份內容；不得重新讀檔，以免產生 TOCTOU。 */
    public synchronized void applySnapshot(String text) {
        if (text == null) {
            specError = "cannot read " + file;
            return;
        }
        apply(text);
    }

    synchronized void apply(String text) {
        SpecParser.Parsed parsed;
        try {
            parsed = SpecParser.parse(text);
        } catch (SpecParser.SpecParseException e) {
            specError = "spec parse error: " + e.getMessage();
            return;
        }
        specError = null;

        lastValid = Map.copyOf(parsed.valid());
        lastInvalid = Map.copyOf(parsed.invalid());

        Set<String> restarted = new HashSet<>();
        for (var entry : List.copyOf(sessions.entrySet())) {
            if (entry.getValue().isTerminated() && finishRetirement(entry.getKey(), entry.getValue())) {
                restarted.add(entry.getKey());
            }
        }

        for (String name : List.copyOf(sessions.keySet())) {
            if (!parsed.valid().containsKey(name) && !parsed.invalid().containsKey(name)) {
                retire(name, sessions.get(name));
            }
        }
        parsed.valid().forEach((name, spec) -> {
            if (!restarted.contains(name) && !retiring.contains(name)
                    && (!sessions.containsKey(name) || !spec.equals(deliveredValid.get(name)))) {
                sessionFor(name).deliver(new Event.SpecChanged(spec));
                deliveredValid.put(name, spec);
                deliveredInvalid.remove(name);
            }
        });
        parsed.invalid().forEach((name, err) -> {
            if (!restarted.contains(name) && !retiring.contains(name)
                    && (!sessions.containsKey(name) || !err.equals(deliveredInvalid.get(name)))) {
                sessionFor(name).deliver(new Event.SpecInvalid(err));
                deliveredInvalid.put(name, err);
                deliveredValid.remove(name);
            }
        });
    }

    private void retire(String name, ListenerSession session) {
        if (!retiring.add(name)) return;
        session.deliver(new Event.Terminate());
        Thread.ofVirtual().name("session-retire-" + name).start(() -> {
            try {
                while (!session.isTerminated()) Thread.sleep(10);
                while (true) {
                    synchronized (this) {
                        if (finishRetirement(name, session)) return;
                    }
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /** 移除已退場 instance；若同名宣告已恢復，立即建立 replacement，不必等下一次檔案變更。 */
    private boolean finishRetirement(String name, ListenerSession session) {
        sessions.remove(name, session);
        if (sessions.containsKey(name)) {
            retiring.remove(name);
            return true;
        }
        SessionSpec desired = lastValid.get(name);
        String error = lastInvalid.get(name);
        try {
            if (desired != null) {
                sessionFor(name).deliver(new Event.SpecChanged(desired));
                deliveredValid.put(name, desired);
                deliveredInvalid.remove(name);
                retiring.remove(name);
                return true;
            }
            if (error != null) {
                sessionFor(name).deliver(new Event.SpecInvalid(error));
                deliveredInvalid.put(name, error);
                deliveredValid.remove(name);
                retiring.remove(name);
                return true;
            }
        } catch (RuntimeException e) {
            System.err.println("[" + name + "] replacement failed, retrying: " + e.getMessage());
            return false;
        }
        deliveredValid.remove(name);
        deliveredInvalid.remove(name);
        retiring.remove(name);
        return true;
    }

    private ListenerSession sessionFor(String name) {
        return sessions.computeIfAbsent(name, n -> {
            var s = factory.apply(n);
            s.start();
            return s;
        });
    }

    public Map<String, ListenerSession> sessions() { return Map.copyOf(sessions); }

    public String specError() { return specError; }
}
