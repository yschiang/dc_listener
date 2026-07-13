package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.util.List;

/** session 專屬的 NATS 通道：一條 Connection + 一個 durable pull consumer。 */
public interface NatsLink {
    /** 連線 + createOrUpdate durable consumer；重連與初次走同一路徑。 */
    void connect(SessionSpec spec) throws LinkException;

    List<InFlightMsg> fetch(int max, Duration wait) throws LinkException;

    void ack(InFlightMsg m) throws LinkException;

    /** consumer 的 server 端 backlog（numPending）；不可用時丟例外，呼叫端保留舊值。 */
    long pending() throws Exception;

    boolean isConnected();

    /** offboarding 專用：刪 server 端 durable consumer（best-effort）。 */
    void deleteConsumer(SessionSpec spec);

    /** 冪等、不丟例外。 */
    void close();
}
