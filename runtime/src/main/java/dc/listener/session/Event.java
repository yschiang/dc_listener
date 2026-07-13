package dc.listener.session;

import dc.listener.spec.SessionSpec;

/** 所有情境一律通過狀態機（spec §4.2）：錯誤、宣告變更、退場全部轉譯成事件。 */
public sealed interface Event {
    record SpecChanged(SessionSpec spec) implements Event {}
    record SpecInvalid(String error) implements Event {}
    record ConnectOk() implements Event {}
    record ConnectFailed(String reason) implements Event {}
    record FetchError(String reason) implements Event {}
    record RetryTick() implements Event {}
    record DrainComplete() implements Event {}
    record DrainTimeout() implements Event {}
    /** 程序生命週期收尾：drain → close NATS → 結束 loop，durable 保留（不刪 consumer）。 */
    record Shutdown() implements Event {}
}
