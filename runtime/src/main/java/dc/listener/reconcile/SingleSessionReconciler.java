package dc.listener.reconcile;

import dc.listener.session.Event;
import dc.listener.session.ListenerSession;
import dc.listener.spec.SessionSpec;
import dc.listener.spec.SpecParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 單一 owner 的 reconciler（Task 3）：只投遞事件給建構時傳入的、身分不可變的那個 ListenerSession，
 * 從共用的 sessions.yaml 相容 schema 裡只投影出該 session 名下的那一條宣告；其餘條目（含其 durable
 * 變更）一律忽略，絕不影響本 session（single-owner 規劃 §Scope and invariants）。
 *
 * <p>宣告消失比照 Task 2 的 Reconciler：投遞 {@code SpecInvalid("declaration missing")}
 * 給既有 instance（fail closed、保留 durable），絕不退場/替換/刪 consumer。
 */
public final class SingleSessionReconciler {
    static final String DECLARATION_MISSING = "declaration missing";

    private final Path file;
    private final ListenerSession session;
    private SessionSpec deliveredValid;
    private String deliveredInvalid;
    private volatile String specError;

    public SingleSessionReconciler(Path file, ListenerSession session) {
        this.file = file;
        this.session = session;
        session.start();
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

    private void apply(String text) {
        SpecParser.Parsed parsed;
        try {
            parsed = SpecParser.parse(text);
        } catch (SpecParser.SpecParseException e) {
            specError = "spec parse error: " + e.getMessage();
            return;
        }
        specError = null;

        String name = session.name();
        SessionSpec spec = parsed.valid().get(name);
        if (spec != null) {
            if (!spec.equals(deliveredValid)) {
                session.deliver(new Event.SpecChanged(spec));
                deliveredValid = spec;
                deliveredInvalid = null;
            }
            return;
        }
        String error = parsed.invalid().getOrDefault(name, DECLARATION_MISSING);
        if (!error.equals(deliveredInvalid)) {
            session.deliver(new Event.SpecInvalid(error));
            deliveredInvalid = error;
            deliveredValid = null;
        }
    }

    public String sessionName() { return session.name(); }

    public ListenerSession session() { return session; }

    public String specError() { return specError; }
}
