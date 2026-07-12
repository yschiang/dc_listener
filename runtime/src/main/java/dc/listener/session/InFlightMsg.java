package dc.listener.session;

/** 已 fetch 未 ack 的訊息（spec §4.4 的 in-flight 定義）。handle 由實作自用（JnatsLink 放 io.nats Message）。 */
public record InFlightMsg(String data, Object handle) {}
