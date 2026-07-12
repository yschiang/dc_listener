package dc.listener.reconcile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 平台中立的 500ms 內容輪詢。內容連續兩次相同才通知，避免讀到 truncate/rewrite 中間態。
 */
public final class FileWatcher {
    private final Path file;
    private final Consumer<String> onChange;

    public FileWatcher(Path file, Consumer<String> onChange) {
        this.file = file;
        this.onChange = onChange;
    }

    public void start() {
        Thread.ofVirtual().name("file-watcher").start(() -> {
            String applied = null;
            boolean hasApplied = false;
            String candidate = null;
            boolean hasCandidate = false;
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                String current = content();
                if (hasApplied && Objects.equals(current, applied)) {
                    candidate = null;
                    hasCandidate = false;
                } else if (hasCandidate && Objects.equals(current, candidate)) {
                    if (notifySafely(current)) {
                        applied = current;
                        hasApplied = true;
                    }
                    candidate = null;
                    hasCandidate = false;
                } else {
                    candidate = current;
                    hasCandidate = true;
                }
            }
        });
    }

    private String content() {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            return null;
        }
    }

    private boolean notifySafely(String content) {
        try {
            onChange.accept(content);
            return true;
        } catch (RuntimeException e) {
            System.err.println("[file-watcher] reload failed: " + e.getMessage());
            return false;
        }
    }
}
