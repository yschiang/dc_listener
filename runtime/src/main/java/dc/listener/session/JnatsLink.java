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
            // in-place createOrUpdate；durable/filter 更新一律原地生效，永不 delete+recreate（ADR-0001）。
            // server 拒絕（含 filter 更新失敗）→ JetStreamApiException → RESOURCE_NOT_FOUND → DEGRADED 有界重試。
            nc.jetStreamManagement().addOrUpdateConsumer(STREAM, cc);
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
        if (!isConnected()) throw disconnected();
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
            if (!isConnected()) throw disconnected();
            return out;
        } catch (LinkException e) {
            throw e;
        } catch (Exception e) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", e);
        }
    }

    @Override
    public void ack(InFlightMsg m) throws LinkException {
        try {
            ((Message) m.handle()).ack();
        } catch (RuntimeException e) {
            throw new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", e);
        }
    }

    @Override
    public long pending() throws Exception { return consumer.getConsumerInfo().getNumPending(); }

    @Override
    public boolean isConnected() { return nc != null && nc.getStatus() == Connection.Status.CONNECTED; }

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

    private LinkException disconnected() {
        return new LinkException("MESSAGING_ENDPOINT_UNREACHABLE", null);
    }
}
