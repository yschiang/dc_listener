package dc.listener.session;

/** NatsLink 錯誤，一律帶 reason code；分類規則見 spec §4.2（連線類與資源不存在都走 DEGRADED）。 */
public class LinkException extends Exception {
    private final String reasonCode;

    public LinkException(String reasonCode, Throwable cause) {
        super(reasonCode, cause);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() { return reasonCode; }
}
