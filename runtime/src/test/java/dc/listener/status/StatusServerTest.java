package dc.listener.status;

import dc.listener.Await;
import dc.listener.reconcile.SingleSessionReconciler;
import dc.listener.session.FakeNatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.session.ObservedState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StatusServerTest {
    private final HttpClient client = HttpClient.newHttpClient();
    private StatusServer server;

    @AfterEach
    void stopServer() {
        if (server != null) server.stop();
    }

    @Test
    void getStatusReturnsValidJsonSnapshot() throws Exception {
        SingleSessionReconciler rec = activeReconciler();
        server = new StatusServer(0, rec);
        server.start();

        var response = client.send(HttpRequest.newBuilder(uri("/status")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("")
                .startsWith("application/json"));
        Map<?, ?> root = json(response.body());
        Map<?, ?> cell = (Map<?, ?>) root.get("cell");
        Map<?, ?> sessions = (Map<?, ?>) root.get("sessions");
        Map<?, ?> tool = (Map<?, ?>) sessions.get("tool-a");
        Map<?, ?> conditions = (Map<?, ?>) tool.get("conditions");
        assertEquals("tool-a", cell.get("cellId"));   // process/runtime identity == SESSION_NAME
        assertNull(cell.get("specError"));
        assertEquals("tool.a.events", tool.get("subject"));
        assertEquals("RUNNING", tool.get("desiredState"));
        assertEquals("ACTIVE", tool.get("observedState"));
        assertEquals(Boolean.TRUE, conditions.get("admissionAllowed"));
    }

    @Test
    void statusContainsExactlyTheSelectedToolEvenWithSeveralDeclarations() throws Exception {
        var session = new ListenerSession("tool-a", new FakeNatsLink(), 0);
        var rec = new SingleSessionReconciler(Path.of("/nonexistent"), session);
        rec.applySnapshot("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-tool-a
              tool-b:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.b.events
                  durable: dur-tool-b
              tool-c:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.c.events
                  durable: dur-tool-c
            """);
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        server = new StatusServer(0, rec);
        server.start();

        var response = client.send(HttpRequest.newBuilder(uri("/status")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        Map<?, ?> sessions = (Map<?, ?>) json(response.body()).get("sessions");
        assertEquals(Set.of("tool-a"), sessions.keySet(),
                "single-session status must expose only the process-selected tool");
    }

    @Test
    void malformedSpecErrorIsStillValidJson() throws Exception {
        var rec = new SingleSessionReconciler(Path.of("/nonexistent"),
                new ListenerSession("tool-a", new FakeNatsLink(), 0));
        rec.applySnapshot("sessions: [broken");
        server = new StatusServer(0, rec);
        server.start();

        var response = client.send(HttpRequest.newBuilder(uri("/status")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        Map<?, ?> cell = (Map<?, ?>) json(response.body()).get("cell");
        assertTrue(String.valueOf(cell.get("specError")).startsWith("spec parse error:"));
    }

    @Test
    void onlyExactGetStatusIsServed() throws Exception {
        server = new StatusServer(0, activeReconciler());
        server.start();

        var post = client.send(HttpRequest.newBuilder(uri("/status"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.discarding());
        var nested = client.send(HttpRequest.newBuilder(uri("/status/extra")).GET().build(),
                HttpResponse.BodyHandlers.discarding());

        assertEquals(405, post.statusCode());
        assertEquals(404, nested.statusCode());
    }

    private SingleSessionReconciler activeReconciler() {
        var session = new ListenerSession("tool-a", new FakeNatsLink(), 0);
        var rec = new SingleSessionReconciler(Path.of("/nonexistent"), session);
        rec.applySnapshot("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-tool-a
            """);
        Await.until(() -> session.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        return rec;
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.port() + path);
    }

    @SuppressWarnings("unchecked")
    private static Map<?, ?> json(String text) {
        Object value = new Yaml(new SafeConstructor(new LoaderOptions())).load(text);
        assertInstanceOf(Map.class, value);
        return (Map<?, ?>) value;
    }
}
