package dc.listener.reconcile;

import dc.listener.session.Event;
import dc.listener.session.ListenerSession;
import dc.listener.spec.SessionSpec;
import dc.listener.spec.SpecParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 宣告 → 現實的單向收斂（spec §5.2）：parse → diff → 投遞事件到 per-session mailbox。
 * 不直接碰 connection；一個 session 卡住不阻塞其他 session 的 reconcile（P3）。
 *
 * <p>ADR-0001：宣告消失不是 offboarding。此處只投遞 {@code SpecInvalid("declaration missing")} 給既有
 * instance（fail closed、保留 durable），絕不退場/替換/刪 consumer。Task 4 會以 SingleSessionReconciler
 * 取代這個相容層。
 */
public final class Reconciler {
    static final String DECLARATION_MISSING = "declaration missing";

    private final Path file;
    private final Function<String, ListenerSession> factory;
    private final Map<String, ListenerSession> sessions = new ConcurrentHashMap<>();
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

        // 宣告消失 → 對既有 instance fail closed（保留 durable），不退場、不刪 consumer（ADR-0001）。
        for (String name : List.copyOf(sessions.keySet())) {
            if (!parsed.valid().containsKey(name) && !parsed.invalid().containsKey(name)) {
                deliverInvalid(name, DECLARATION_MISSING);
            }
        }
        parsed.valid().forEach((name, spec) -> {
            if (!sessions.containsKey(name) || !spec.equals(deliveredValid.get(name))) {
                sessionFor(name).deliver(new Event.SpecChanged(spec));
                deliveredValid.put(name, spec);
                deliveredInvalid.remove(name);
            }
        });
        parsed.invalid().forEach(this::deliverInvalid);
    }

    private void deliverInvalid(String name, String error) {
        if (!sessions.containsKey(name) || !error.equals(deliveredInvalid.get(name))) {
            sessionFor(name).deliver(new Event.SpecInvalid(error));
            deliveredInvalid.put(name, error);
            deliveredValid.remove(name);
        }
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
