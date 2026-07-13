package dc.listener;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * 啟動時的靜態設定（ADR-0001 §3：NATS endpoint、tool 身分、設定檔、status port、shutdown 逾時皆為 startup-only）。
 * 從環境變數（或測試傳入的 Map）解析。{@code SESSION_NAME} 是不可變的 process 身分，缺少/空白即拒絕啟動；
 * 數值型欄位若無法解析則以明確錯誤拒絕啟動。
 */
record StartupConfig(
        String sessionName,
        String natsUrl,
        Path sessionsFile,
        long processDelayMs,
        int statusPort,
        Duration shutdownTimeout) {

    static StartupConfig fromEnv(Map<String, String> env) {
        String sessionName = env.get("SESSION_NAME");
        if (sessionName == null || sessionName.isBlank()) {
            throw new IllegalArgumentException("SESSION_NAME is required and must be non-blank");
        }
        long statusPort = parseLong(env, "STATUS_PORT", 8080);
        if (statusPort < 0 || statusPort > 65535) {
            throw new IllegalArgumentException("STATUS_PORT must be 0..65535, got: " + statusPort);
        }
        return new StartupConfig(
                sessionName,
                orDefault(env, "NATS_URL", "nats://localhost:4222"),
                Path.of(orDefault(env, "SESSIONS_FILE", "config/sessions.yaml")),
                parseLong(env, "PROCESS_DELAY_MS", 200),
                (int) statusPort,
                Duration.ofMillis(parseLong(env, "SHUTDOWN_TIMEOUT_MS", 30_000)));
    }

    private static String orDefault(Map<String, String> env, String key, String def) {
        String v = env.get(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static long parseLong(Map<String, String> env, String key, long def) {
        String v = env.get(key);
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, got: " + v);
        }
    }
}
