package dc.listener.session;

import dc.listener.spec.DesiredState;

import java.time.Instant;

/** /status 用的唯讀快照（spec §6）。 */
public record SessionStatus(
        String name,
        String subject,
        DesiredState desiredState,
        ObservedState observedState,
        String declaredConfigVersion,
        String appliedConfigVersion,
        boolean configurationReady,
        boolean connectionReady,
        boolean consumerReady,
        boolean admissionAllowed,
        String reason,
        Instant lastTransitionTime,
        long admittedCount,
        long pendingCount,
        int retryAttempt) {
}
