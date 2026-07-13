package dc.listener.status;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dc.listener.reconcile.SingleSessionReconciler;
import dc.listener.session.ListenerSession;
import dc.listener.session.SessionStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/** 唯讀觀察介面（spec §6）：只提供精確的 GET /status。單一 owner：最多回報那一個被選中的 session。 */
public final class StatusServer {
    private final HttpServer http;
    private final SingleSessionReconciler reconciler;

    public StatusServer(int port, SingleSessionReconciler reconciler) throws IOException {
        this.reconciler = reconciler;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/status", this::handleStatus);
    }

    public void start() { http.start(); }

    public void stop() { http.stop(0); }

    int port() { return http.getAddress().getPort(); }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"/status".equals(exchange.getRequestURI().getPath())) {
            sendEmpty(exchange, 404);
            return;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "GET");
            sendEmpty(exchange, 405);
            return;
        }

        byte[] body = json().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (var os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private String json() {
        var sb = new StringBuilder();
        // cellId 欄位名保留（demo 相容）；值改為 process/runtime 身分（= SESSION_NAME），不再假裝有 cell 聚合。
        sb.append("{\"cell\":{\"cellId\":")
          .append(q(reconciler.sessionName()))
          .append(",\"specError\":")
          .append(q(reconciler.specError()))
          .append("},\"sessions\":{");
        ListenerSession session = reconciler.session();
        if (!session.isTerminated()) {
            SessionStatus s = session.snapshot();
            sb.append(q(reconciler.sessionName())).append(":{")
              .append("\"subject\":").append(q(s.subject())).append(',')
              .append("\"desiredState\":").append(q(s.desiredState() == null ? null : s.desiredState().name())).append(',')
              .append("\"observedState\":").append(q(s.observedState().name())).append(',')
              .append("\"declaredConfigVersion\":").append(q(s.declaredConfigVersion())).append(',')
              .append("\"appliedConfigVersion\":").append(q(s.appliedConfigVersion())).append(',')
              .append("\"conditions\":{")
              .append("\"configurationReady\":").append(s.configurationReady()).append(',')
              .append("\"connectionReady\":").append(s.connectionReady()).append(',')
              .append("\"consumerReady\":").append(s.consumerReady()).append(',')
              .append("\"admissionAllowed\":").append(s.admissionAllowed())
              .append("},")
              .append("\"reason\":").append(q(s.reason())).append(',')
              .append("\"lastTransitionTime\":").append(q(s.lastTransitionTime().toString())).append(',')
              .append("\"admittedCount\":").append(s.admittedCount()).append(',')
              .append("\"pendingCount\":").append(s.pendingCount()).append(',')
              .append("\"retryAttempt\":").append(s.retryAttempt())
              .append('}');
        }
        sb.append("}}");
        return sb.toString();
    }

    private static String q(String value) {
        if (value == null) return "null";
        var out = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.append('"').toString();
    }
}
