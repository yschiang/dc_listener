package dc.listener.session;

/** guidance 8.1：admission 由 lifecycle state 推導，不是獨立狀態機。 */
public final class AdmissionGate {
    private final SessionStateMachine machine;

    public AdmissionGate(SessionStateMachine machine) { this.machine = machine; }

    public boolean admits() { return machine.state() == ObservedState.ACTIVE; }
}
