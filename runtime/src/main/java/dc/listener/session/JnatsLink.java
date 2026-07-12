package dc.listener.session;

import dc.listener.spec.SessionSpec;
import io.nats.client.Connection;
import io.nats.client.ConsumerContext;
import io.nats.client.FetchConsumeOptions;
import io.nats.client.FetchConsumer;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** 真實 NATS 通道。auto-reconnect 關閉（maxReconnects(0)）：重連主導權在狀態機（spec §4.2）。 */
public final class JnatsLink implements NatsLink {
    static final String STREAM = "TOOL_EVENTS";   // 上游資產；runtime 只讀 stream、只管 consumer（spec §4.3）

    private final String url;
    private Connection nc;
    private ConsumerContext consumer;

    public JnatsLink(String url) { this.url = url; }

    @Override
    public void connect(SessionSpec spec) throws LinkException {
        close();
        try {
            nc = Nats.connect(options());
        } catch (IOException | InterruptedException e) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", e);
        }
        try {
            var cc = ConsumerConfiguration.builder()
                    .durable(spec.durable())
                    .filterSubject(spec.subject())
                    .ackPolicy(AckPolicy.Explicit)   // DeliverPolicy 用預設 All（spec §4.3）
                    .build();
            var jsm = nc.jetStreamManagement();
            try {
                jsm.addOrUpdateConsumer(STREAM, cc);
            } catch (JetStreamApiException e) {
                // subject 換版時 server 拒絕 filter 更新 → delete + recreate（spec §4.3 fallback）
                jsm.deleteConsumer(STREAM, spec.durable());
                jsm.addOrUpdateConsumer(STREAM, cc);
            }
            consumer = nc.getStreamContext(STREAM).getConsumerContext(spec.durable());
        } catch (JetStreamApiException e) {
            close();
            throw new LinkException("RESOURCE_NOT_FOUND", e);
        } catch (IOException e) {
            close();
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", e);
        }
    }

    @Override
    public List<InFlightMsg> fetch(int max, Duration wait) throws LinkException {
        try {
            var out = new ArrayList<InFlightMsg>();
            var opts = FetchConsumeOptions.builder()
                    .maxMessages(max)
                    .expiresIn(Math.max(wait.toMillis(), 1000))
                    .build();
            try (FetchConsumer fc = consumer.fetch(opts)) {
                Message m;
                while ((m = fc.nextMessage()) != null) {
                    out.add(new InFlightMsg(new String(m.getData(), StandardCharsets.UTF_8), m));
                }
            }
            return out;
        } catch (Exception e) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", e);
        }
    }

    @Override
    public void ack(InFlightMsg m) { ((Message) m.handle()).ack(); }

    @Override
    public long pending() throws Exception { return consumer.getConsumerInfo().getNumPending(); }

    @Override
    public boolean isConnected() { return nc != null && nc.getStatus() == Connection.Status.CONNECTED; }

    @Override
    public void deleteConsumer(SessionSpec spec) {
        // 用短命連線執行，不依賴主連線是否存活（offboarding 時 NATS 可能剛好不在）
        try (Connection c = Nats.connect(options())) {
            c.jetStreamManagement().deleteConsumer(STREAM, spec.durable());
        } catch (Exception e) {
            // ponytail: best-effort 清理；正式版需 retry 或後台 GC 無主 consumer
            System.err.println("[" + spec.name() + "] deleteConsumer failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (nc != null) {
            try { nc.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            nc = null;
            consumer = null;
        }
    }

    private Options options() {
        return Options.builder()
                .server(url)
                .maxReconnects(0)
                .connectionTimeout(Duration.ofSeconds(2))
                .build();
    }
}
