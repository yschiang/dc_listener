package dc.listener.reconcile;

import dc.listener.Await;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileWatcherTest {

    @TempDir
    Path dir;

    @Test
    void notifiesInitialStableContent() throws Exception {
        Path file = dir.resolve("sessions.yaml");
        Files.writeString(file, "initial-content");
        var received = new AtomicReference<String>();

        new FileWatcher(file, received::set).start();

        Await.until(() -> "initial-content".equals(received.get()), 2000);
    }

    @Test
    void waitsForStableContentBeforeNotifying() throws Exception {
        Path file = dir.resolve("sessions.yaml");
        Files.writeString(file, "initial");
        List<String> observed = new CopyOnWriteArrayList<>();
        new FileWatcher(file, observed::add).start();

        Thread.sleep(600);
        Files.writeString(file, "baseline");
        Await.until(() -> observed.contains("baseline"), 2000);
        observed.clear();

        Files.writeString(file, "partial-valid-content");
        Thread.sleep(600);
        Files.writeString(file, "complete-content");
        Await.until(() -> observed.contains("complete-content"), 2500);

        assertEquals(List.of("complete-content"), observed);
    }

    @Test
    void callbackFailureDoesNotStopWatching() throws Exception {
        Path file = dir.resolve("sessions.yaml");
        Files.writeString(file, "initial");
        var attempts = new AtomicInteger();
        new FileWatcher(file, content -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("boom");
        }).start();

        Thread.sleep(600);
        Files.writeString(file, "first-change");
        Await.until(() -> attempts.get() == 1, 2000);
        Await.until(() -> attempts.get() == 2, 2500);
    }

    @Test
    void passesTheVerifiedSnapshotToCallback() throws Exception {
        Path file = dir.resolve("sessions.yaml");
        Files.writeString(file, "initial");
        var received = new AtomicReference<String>();
        new FileWatcher(file, content -> {
            if (received.compareAndSet(null, content)) {
                try {
                    Files.writeString(file, "changed-during-callback");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        Thread.sleep(600);
        Files.writeString(file, "verified-stable-content");
        Await.until(() -> received.get() != null, 2500);

        assertEquals("verified-stable-content", received.get());
    }
}
