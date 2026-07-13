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

    /** 冪等、不丟例外。程序關閉與宣告消失都只 close，durable 保留（ADR-0001：無 finalizer 前不得刪 consumer）。 */
    void close();
}
