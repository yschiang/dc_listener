package dc.listener.session;

/** guidance 8.3 的七個 lifecycle state；Runtime 唯一對外回報的 observed 值。 */
public enum ObservedState { STANDBY, CONNECTING, ACTIVE, DRAINING, DEGRADED, FAILED, STOPPED }
