package dc.listener.spec;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** 解析 + 驗證 sessions.yaml。整份壞 → SpecParseException；單 entry 壞 → 進 invalid map（spec §4.2 / §5.2）。 */
public final class SpecParser {

    public record Parsed(Map<String, SessionSpec> valid, Map<String, String> invalid) {}

    public static final class SpecParseException extends Exception {
        public SpecParseException(String msg) { super(msg); }
    }

    private static final Pattern DURATION = Pattern.compile("(\\d+)(ms|s|m)");

    private SpecParser() {}

    @SuppressWarnings("unchecked")
    public static Parsed parse(String text) throws SpecParseException {
        Object root;
        try {
            root = new Yaml(new SafeConstructor(new LoaderOptions())).load(text);
        } catch (RuntimeException e) {
            throw new SpecParseException(String.valueOf(e.getMessage()));
        }
        if (!(root instanceof Map<?, ?> rootMap)) throw new SpecParseException("root must be a map");
        Object sessions = ((Map<String, Object>) rootMap).get("sessions");
        if (!(sessions instanceof Map<?, ?> sessionMap)) throw new SpecParseException("missing 'sessions' map");

        Map<String, SessionSpec> valid = new LinkedHashMap<>();
        Map<String, String> invalid = new LinkedHashMap<>();
        ((Map<String, Object>) sessionMap).forEach((name, body) -> {
            try {
                valid.put(name, one(name, body));
            } catch (RuntimeException e) {
                invalid.put(name, String.valueOf(e.getMessage()));
            }
        });
        return new Parsed(valid, invalid);
    }

    private static SessionSpec one(String name, Object body) {
        Map<String, Object> m = asMap(body, "session body");
        DesiredState desired = DesiredState.valueOf(req(m, "desiredState"));
        String version = req(m, "configVersion");
        Map<String, Object> cfg = asMap(m.get("config"), "config");
        String subject = req(cfg, "subject");
        String durable = req(cfg, "durable");

        Duration interval = null;
        int maxAttempts = 10;
        if (cfg.get("retry") != null) {
            Map<String, Object> r = asMap(cfg.get("retry"), "retry");
            if (r.get("interval") != null) interval = duration(r.get("interval").toString());
            if (r.get("maxAttempts") != null) maxAttempts = Integer.parseInt(r.get("maxAttempts").toString());
        }
        Duration drain = cfg.get("drainTimeout") != null
                ? duration(cfg.get("drainTimeout").toString()) : Duration.ofSeconds(30);
        return new SessionSpec(name, desired, version, subject, durable, interval, maxAttempts, drain);
    }

    static Duration duration(String s) {
        var m = DURATION.matcher(s.trim());
        if (!m.matches()) throw new IllegalArgumentException("bad duration: " + s);
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            default -> Duration.ofMinutes(n);
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o, String what) {
        if (!(o instanceof Map<?, ?> m)) throw new IllegalArgumentException("missing/invalid " + what);
        return (Map<String, Object>) m;
    }

    private static String req(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null || v.toString().isBlank()) throw new IllegalArgumentException("missing " + key);
        return v.toString();
    }
}
