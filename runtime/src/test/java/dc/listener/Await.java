package dc.listener;

import java.util.function.BooleanSupplier;

public final class Await {
    private Await() {}

    public static void until(BooleanSupplier cond, long timeoutMs) {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return;
            try { Thread.sleep(10); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }
}
