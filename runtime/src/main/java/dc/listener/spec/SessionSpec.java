package dc.listener.spec;

import java.time.Duration;

/** 一個 session 的 desired 宣告（來自 sessions.yaml，經驗證）。retryInterval == null 代表指數退避 1s→30s。 */
public record SessionSpec(
        String name,
        DesiredState desiredState,
        String configVersion,
        String subject,
        String durable,
        Duration retryInterval,
        int maxAttempts,
        Duration drainTimeout) {
}
