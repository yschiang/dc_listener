# ListenerSession NATS Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 依 `docs/specs/2026-07-13-listener-session-nats-prototype-design.md` 建出可跑的 docker prototype：一個 Java 21 ListenerCell runtime 管多個 ListenerSession，desired 宣告走 `config/sessions.yaml`，observed 走 `GET :8080/status`，配五個 demo 場景。

**Architecture:** 每個 session 一條獨立 NATS Connection + 一個 virtual thread + mailbox（`BlockingQueue<Event>`）；`SessionStateMachine` 是唯一權威 lifecycle（純轉移邏輯、無 I/O，7 個 observedState）；`Reconciler` 輪詢 YAML → diff → 投遞事件；jnats auto-reconnect 關閉（`maxReconnects(0)`），重連全由狀態機主導。

**Tech Stack:** Java 21（純 JDK，無 framework）、jnats 2.20.2、snakeyaml 2.2、JUnit 5、Gradle 8.7（docker image 跑，本機無 gradle/JDK21）、docker compose、NATS 2.10 + JetStream、natsio/nats-box（publisher）。

## Global Constraints

- 語言/版本：Java 21（用到 virtual threads、record patterns）；依賴僅 `io.nats:jnats:2.20.2`、`org.yaml:snakeyaml:2.2`、JUnit 5 —— 不新增其他依賴。
- **本機無 gradle 也無 JDK 21**：所有 build/test 一律用（在 repo 根目錄執行）：
  `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon <task>`
  第一次跑會拉 image + 下載依賴（約 2–5 分鐘），之後有 named volume 快取。
- 命名照 spec §11：類名 `ListenerSession`；對外欄位 `desiredState` / `observedState` 全名；observedState 七值 `STANDBY/CONNECTING/ACTIVE/DRAINING/DEGRADED/FAILED/STOPPED`；desiredState 三值 `RUNNING/STANDBY/STOPPED`。
- Runtime 絕不因 session 層級錯誤 exit（spec §4.2）；所有錯誤轉譯成狀態機事件。
- Stream `TOOL_EVENTS`（subjects `tool.>`）＝上游資產，由 publisher 冪等建立；runtime 只建/更新 durable consumer。
- Commit 規則（CLAUDE.md）：訊息精簡具體、**不加 Co-Authored-By trailer**、只 commit 本地、**不 push**。
- reason codes 固定字串：`MESSAGING_ENDPOINT_UNREACHABLE`、`RESOURCE_NOT_FOUND`、`INVALID_SPEC: <detail>`、`RETRY_EXHAUSTED`、`DRAIN_TIMEOUT`。
- 檔案變更偵測採平台中立的 **500ms 穩定內容輪詢**：連續兩次相同才把該份
  snapshot 直接交給 Reconciler，穩定後 ≤1s reconcile；不依賴特定 OS 的檔案事件語意。

## 檔案結構總覽

```
dc_listener/
├── docker-compose.yml                # Task 7
├── config/sessions.yaml              # Task 7
├── demo/
│   ├── smoke-test.sh                 # Task 7
│   ├── watch-status.sh               # Task 8
│   └── 01..05-*.sh                   # Task 8
└── runtime/
    ├── settings.gradle  build.gradle  Dockerfile      # Task 1 / 7
    └── src/main/java/dc/listener/
        ├── Main.java                                  # Task 6
        ├── spec/ DesiredState SessionSpec SpecParser  # Task 1
        ├── session/ ObservedState Event SessionStateMachine SessionStatus   # Task 2
        │            NatsLink InFlightMsg LinkException AdmissionGate
        │            PipelineStub ListenerSession      # Task 3
        │            JnatsLink                         # Task 4
        ├── reconcile/ Reconciler FileWatcher          # Task 5
        └── status/ StatusServer                       # Task 6
    └── src/test/java/dc/listener/
        ├── Await.java                                 # Task 3
        ├── spec/SpecParserTest.java                   # Task 1
        ├── session/ SessionStateMachineTest FakeNatsLink ListenerSessionTest  # Task 2/3
        └── reconcile/ReconcilerTest.java              # Task 5
```

---

### Task 1: Gradle 專案 + SessionSpec + SpecParser

**Files:**
- Create: `runtime/settings.gradle`
- Create: `runtime/build.gradle`
- Create: `runtime/src/main/java/dc/listener/spec/DesiredState.java`
- Create: `runtime/src/main/java/dc/listener/spec/SessionSpec.java`
- Create: `runtime/src/main/java/dc/listener/spec/SpecParser.java`
- Test: `runtime/src/test/java/dc/listener/spec/SpecParserTest.java`
- Modify: `.gitignore`（追加 gradle 產物）

**Interfaces:**
- Produces: `record SessionSpec(String name, DesiredState desiredState, String configVersion, String subject, String durable, Duration retryInterval, int maxAttempts, Duration drainTimeout)`（`retryInterval` 可為 null＝指數退避；預設 `maxAttempts=10`、`drainTimeout=30s`）
- Produces: `SpecParser.parse(String) → SpecParser.Parsed(Map<String,SessionSpec> valid, Map<String,String> invalid)`；整份 YAML 壞掉丟 `SpecParser.SpecParseException`（checked）；單一 entry 缺欄位/值不合法 → 進 `invalid`（name → 錯誤訊息），不影響其他 entry。

- [ ] **Step 1: 建 Gradle 骨架**

`runtime/settings.gradle`：

```gradle
rootProject.name = 'runtime'
```

`runtime/build.gradle`：

```gradle
plugins {
    id 'application'
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}

repositories { mavenCentral() }

dependencies {
    implementation 'io.nats:jnats:2.20.2'
    implementation 'org.yaml:snakeyaml:2.2'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.2'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

application { mainClass = 'dc.listener.Main' }

test {
    useJUnitPlatform()
    testLogging { events 'passed', 'failed', 'skipped' }
}
```

`.gitignore` 追加三行：

```
runtime/.gradle/
runtime/build/
runtime/bin/
```

- [ ] **Step 2: 寫 failing test**

`runtime/src/test/java/dc/listener/spec/SpecParserTest.java`：

```java
package dc.listener.spec;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SpecParserTest {

    static final String FULL = """
        sessions:
          tool-a:
            desiredState: RUNNING
            configVersion: v1
            config:
              subject: tool.a.events
              durable: listener-tool-a
              retry:
                interval: 5s
                maxAttempts: 3
              drainTimeout: 10s
        """;

    @Test
    void parsesFullSpec() throws Exception {
        var p = SpecParser.parse(FULL);
        assertTrue(p.invalid().isEmpty());
        SessionSpec s = p.valid().get("tool-a");
        assertEquals(DesiredState.RUNNING, s.desiredState());
        assertEquals("v1", s.configVersion());
        assertEquals("tool.a.events", s.subject());
        assertEquals("listener-tool-a", s.durable());
        assertEquals(Duration.ofSeconds(5), s.retryInterval());
        assertEquals(3, s.maxAttempts());
        assertEquals(Duration.ofSeconds(10), s.drainTimeout());
    }

    @Test
    void appliesDefaults() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: STANDBY
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
            """);
        SessionSpec s = p.valid().get("tool-a");
        assertNull(s.retryInterval());
        assertEquals(10, s.maxAttempts());
        assertEquals(Duration.ofSeconds(30), s.drainTimeout());
    }

    @Test
    void invalidEntryDoesNotPoisonOthers() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
              tool-x:
                desiredState: RUNNING
                configVersion: v1
                config:
                  durable: listener-tool-x
            """);
        assertTrue(p.valid().containsKey("tool-a"));
        assertTrue(p.invalid().get("tool-x").contains("subject"));
    }

    @Test
    void badDesiredStateIsInvalidEntry() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: BANANAS
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
            """);
        assertTrue(p.valid().isEmpty());
        assertFalse(p.invalid().get("tool-a").isEmpty());
    }

    @Test
    void brokenYamlThrows() {
        assertThrows(SpecParser.SpecParseException.class,
                () -> SpecParser.parse("sessions: [oops"));
    }

    @Test
    void missingSessionsKeyThrows() {
        assertThrows(SpecParser.SpecParseException.class,
                () -> SpecParser.parse("hello: world"));
    }

    @Test
    void badDurationIsInvalidEntry() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
                  drainTimeout: soon
            """);
        assertTrue(p.invalid().get("tool-a").contains("soon"));
    }
}
```

- [ ] **Step 3: 跑測試確認 fail**

Run（repo 根目錄）：
`docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon test`
Expected: FAIL — compile error（`DesiredState`、`SessionSpec`、`SpecParser` 不存在）。

- [ ] **Step 4: 實作 spec/ 三個檔**

`runtime/src/main/java/dc/listener/spec/DesiredState.java`：

```java
package dc.listener.spec;

public enum DesiredState { RUNNING, STANDBY, STOPPED }
```

`runtime/src/main/java/dc/listener/spec/SessionSpec.java`：

```java
package dc.listener.spec;

import java.time.Duration;

/** 一個 session 的 desired 宣告（來自 sessions.yaml，經驗證）。retryInterval == null 代表指數退避 1s→30s。 */
public record SessionSpec(
        String name,
        DesiredState desiredState,
        String configVersion,
        String subject,
        String durable,
        Duration retryInterval,
        int maxAttempts,
        Duration drainTimeout) {
}
```

`runtime/src/main/java/dc/listener/spec/SpecParser.java`：

```java
package dc.listener.spec;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** 解析 + 驗證 sessions.yaml。整份壞 → SpecParseException；單 entry 壞 → 進 invalid map（spec §4.2 / §5.2）。 */
public final class SpecParser {

    public record Parsed(Map<String, SessionSpec> valid, Map<String, String> invalid) {}

    public static final class SpecParseException extends Exception {
        public SpecParseException(String msg) { super(msg); }
    }

    private static final Pattern DURATION = Pattern.compile("(\\d+)(ms|s|m)");

    private SpecParser() {}

    @SuppressWarnings("unchecked")
    public static Parsed parse(String text) throws SpecParseException {
        Object root;
        try {
            root = new Yaml(new SafeConstructor(new LoaderOptions())).load(text);
        } catch (RuntimeException e) {
            throw new SpecParseException(String.valueOf(e.getMessage()));
        }
        if (!(root instanceof Map<?, ?> rootMap)) throw new SpecParseException("root must be a map");
        Object sessions = ((Map<String, Object>) rootMap).get("sessions");
        if (!(sessions instanceof Map<?, ?> sessionMap)) throw new SpecParseException("missing 'sessions' map");

        Map<String, SessionSpec> valid = new LinkedHashMap<>();
        Map<String, String> invalid = new LinkedHashMap<>();
        ((Map<String, Object>) sessionMap).forEach((name, body) -> {
            try {
                valid.put(name, one(name, body));
            } catch (RuntimeException e) {
                invalid.put(name, String.valueOf(e.getMessage()));
            }
        });
        return new Parsed(valid, invalid);
    }

    private static SessionSpec one(String name, Object body) {
        Map<String, Object> m = asMap(body, "session body");
        DesiredState desired = DesiredState.valueOf(req(m, "desiredState"));
        String version = req(m, "configVersion");
        Map<String, Object> cfg = asMap(m.get("config"), "config");
        String subject = req(cfg, "subject");
        String durable = req(cfg, "durable");

        Duration interval = null;
        int maxAttempts = 10;
        if (cfg.get("retry") != null) {
            Map<String, Object> r = asMap(cfg.get("retry"), "retry");
            if (r.get("interval") != null) interval = duration(r.get("interval").toString());
            if (r.get("maxAttempts") != null) maxAttempts = Integer.parseInt(r.get("maxAttempts").toString());
        }
        Duration drain = cfg.get("drainTimeout") != null
                ? duration(cfg.get("drainTimeout").toString()) : Duration.ofSeconds(30);
        return new SessionSpec(name, desired, version, subject, durable, interval, maxAttempts, drain);
    }

    static Duration duration(String s) {
        var m = DURATION.matcher(s.trim());
        if (!m.matches()) throw new IllegalArgumentException("bad duration: " + s);
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            default -> Duration.ofMinutes(n);
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o, String what) {
        if (!(o instanceof Map<?, ?> m)) throw new IllegalArgumentException("missing/invalid " + what);
        return (Map<String, Object>) m;
    }

    private static String req(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null || v.toString().isBlank()) throw new IllegalArgumentException("missing " + key);
        return v.toString();
    }
}
```

- [ ] **Step 5: 跑測試確認 pass**

Run: 同 Step 3 指令。
Expected: `SpecParserTest` 7 tests PASS，`BUILD SUCCESSFUL`。

- [ ] **Step 6: Commit**

```bash
git add runtime .gitignore
git commit -m "runtime: Gradle scaffold + SessionSpec/SpecParser with per-entry validation"
```

---

### Task 2: SessionStateMachine（唯一權威 lifecycle）

**Files:**
- Create: `runtime/src/main/java/dc/listener/session/ObservedState.java`
- Create: `runtime/src/main/java/dc/listener/session/Event.java`
- Create: `runtime/src/main/java/dc/listener/session/SessionStateMachine.java`
- Test: `runtime/src/test/java/dc/listener/session/SessionStateMachineTest.java`

**Interfaces:**
- Consumes: Task 1 的 `SessionSpec`、`DesiredState`。
- Produces: `enum ObservedState { STANDBY, CONNECTING, ACTIVE, DRAINING, DEGRADED, FAILED, STOPPED }`
- Produces: `sealed interface Event`，成員：`SpecChanged(SessionSpec)`、`SpecInvalid(String error)`、`ConnectOk()`、`ConnectFailed(String reason)`、`FetchError(String reason)`、`RetryTick()`、`DrainComplete()`、`DrainTimeout()`、`Terminate()`。
- Produces: `SessionStateMachine`，synchronized 方法：`onEvent(Event)`、getters `state()`、`spec()`、`reason()`、`retryAttempt()`、`terminating()`、`declaredConfigVersion()`、`appliedConfigVersion()`、`declaredDesired()`、`lastTransitionTime()`。純轉移邏輯、零 I/O —— Task 3 的 loop 負責執行動作。

轉移表（spec §4.1/§4.2 的實作化，測試逐條驗證）：

| 現態 | 事件 | 次態 / 效果 |
|---|---|---|
| any | `SpecInvalid` | `FAILED`，reason=`INVALID_SPEC: <err>`，清 pending |
| `STANDBY`/`STOPPED`/`FAILED`/`DEGRADED`/`CONNECTING` | `SpecChanged` | adopt（spec=新、failures=0、reason 清空）→ 依 desired 收斂：RUNNING→`CONNECTING`、STANDBY→`STANDBY`、STOPPED→`STOPPED`（FAILED 的「重置回 STANDBY」與 DEGRADED 的「立即中斷等待」都由此路徑實現） |
| `ACTIVE` | `SpecChanged`（僅 retry/drainTimeout 變） | 熱生效：spec=新，留在 `ACTIVE` |
| `ACTIVE` | `SpecChanged`（desired/subject/durable/configVersion 變） | pending=新 → `DRAINING` |
| `DRAINING` | `SpecChanged` | pending=新，留在 `DRAINING`（先完成當前轉移） |
| `CONNECTING` | `ConnectOk` | `ACTIVE`，failures=0，reason 清空 |
| `CONNECTING` | `ConnectFailed(r)` | failures++；failures > maxAttempts → `FAILED`(RETRY_EXHAUSTED)，否則 `DEGRADED`(r)。語意：maxAttempts=重試上限（不含初次），初次+maxAttempts 次重試全失敗才升級 |
| `DEGRADED` | `RetryTick` | `CONNECTING` |
| `ACTIVE` | `FetchError(r)` | failures=1 → `DEGRADED`(r) |
| `DRAINING` | `DrainComplete` | terminating → `STOPPED`；否則 adopt(pending) → 依 desired 收斂 |
| `DRAINING` | `DrainTimeout` | reason=DRAIN_TIMEOUT；terminating → `STOPPED`，否則 `FAILED` |
| `ACTIVE` | `Terminate` | terminating=true → `DRAINING` |
| `DRAINING` | `Terminate` | terminating=true，留在 `DRAINING` |
| 其他 | `Terminate` | terminating=true → `STOPPED` |

- [ ] **Step 1: 寫 failing test**

`runtime/src/test/java/dc/listener/session/SessionStateMachineTest.java`：

```java
package dc.listener.session;

import dc.listener.spec.DesiredState;
import dc.listener.spec.SessionSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static dc.listener.session.ObservedState.*;
import static org.junit.jupiter.api.Assertions.*;

class SessionStateMachineTest {

    static SessionSpec spec(DesiredState d, String ver, String subject, int maxAttempts) {
        return new SessionSpec("t", d, ver, subject, "dur-t", null, maxAttempts, Duration.ofSeconds(30));
    }

    static SessionSpec running(String ver) { return spec(DesiredState.RUNNING, ver, "tool.t.events", 10); }

    static SessionStateMachine activeMachine() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectOk());
        return m;
    }

    @Test void startsInStandby() {
        assertEquals(STANDBY, new SessionStateMachine("t").state());
    }

    @Test void runningSpecConnects() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        assertEquals(CONNECTING, m.state());
        assertEquals("v1", m.appliedConfigVersion());
    }

    @Test void standbySpecStaysStandby() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "s", 10)));
        assertEquals(STANDBY, m.state());
    }

    @Test void connectOkActivates() {
        var m = activeMachine();
        assertEquals(ACTIVE, m.state());
        assertEquals(0, m.retryAttempt());
        assertEquals("", m.reason());
    }

    @Test void connectFailedDegrades() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("MESSAGING_ENDPOINT_UNREACHABLE"));
        assertEquals(DEGRADED, m.state());
        assertEquals(1, m.retryAttempt());
        assertEquals("MESSAGING_ENDPOINT_UNREACHABLE", m.reason());
    }

    @Test void retryTickReconnects() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));
        m.onEvent(new Event.RetryTick());
        assertEquals(CONNECTING, m.state());
    }

    @Test void retriesEscalateToFailed() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v1", "s", 2)));
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 初次失敗
        m.onEvent(new Event.RetryTick());
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 重試 1
        m.onEvent(new Event.RetryTick());
        m.onEvent(new Event.ConnectFailed("RESOURCE_NOT_FOUND"));   // 重試 2 → 用盡
        assertEquals(FAILED, m.state());
        assertEquals("RETRY_EXHAUSTED", m.reason());
    }

    @Test void specChangeInterruptsDegraded() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(running("v1")));
        m.onEvent(new Event.ConnectFailed("MESSAGING_ENDPOINT_UNREACHABLE"));
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
        assertEquals(0, m.retryAttempt());
        assertEquals("v2", m.appliedConfigVersion());
    }

    @Test void activeConfigChangeDrains() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events.v2", 10)));
        assertEquals(DRAINING, m.state());
        assertEquals("v2", m.declaredConfigVersion());
        assertEquals("v1", m.appliedConfigVersion());
    }

    @Test void drainCompleteAppliesPending() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "tool.t.events.v2", 10)));
        m.onEvent(new Event.DrainComplete());
        assertEquals(CONNECTING, m.state());
        assertEquals("v2", m.appliedConfigVersion());
    }

    @Test void activeToStandbyViaDrain() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "tool.t.events", 10)));
        assertEquals(DRAINING, m.state());
        m.onEvent(new Event.DrainComplete());
        assertEquals(STANDBY, m.state());
    }

    @Test void hotFieldChangeStaysActive() {
        var m = activeMachine();
        var hot = new SessionSpec("t", DesiredState.RUNNING, "v1", "tool.t.events", "dur-t",
                Duration.ofSeconds(5), 99, Duration.ofSeconds(30));
        m.onEvent(new Event.SpecChanged(hot));
        assertEquals(ACTIVE, m.state());
        assertEquals(99, m.spec().maxAttempts());
    }

    @Test void drainTimeoutFails() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STANDBY, "v1", "tool.t.events", 10)));
        m.onEvent(new Event.DrainTimeout());
        assertEquals(FAILED, m.state());
        assertEquals("DRAIN_TIMEOUT", m.reason());
    }

    @Test void specChangeDuringDrainingWaits() {
        var m = activeMachine();
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v2", "s2", 10)));
        m.onEvent(new Event.SpecChanged(spec(DesiredState.RUNNING, "v3", "s3", 10)));
        assertEquals(DRAINING, m.state());
        assertEquals("v3", m.declaredConfigVersion());
        m.onEvent(new Event.DrainComplete());
        assertEquals("v3", m.appliedConfigVersion());
    }

    @Test void invalidSpecFailsFromAnyState() {
        var m = activeMachine();
        m.onEvent(new Event.SpecInvalid("missing subject"));
        assertEquals(FAILED, m.state());
        assertTrue(m.reason().startsWith("INVALID_SPEC"));
    }

    @Test void failedRecoversOnSpecChange() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecInvalid("missing subject"));
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
        assertEquals("", m.reason());
    }

    @Test void fetchErrorDegrades() {
        var m = activeMachine();
        m.onEvent(new Event.FetchError("MESSAGING_ENDPOINT_UNREACHABLE"));
        assertEquals(DEGRADED, m.state());
        assertEquals(1, m.retryAttempt());
    }

    @Test void terminateFromActiveDrainsThenStops() {
        var m = activeMachine();
        m.onEvent(new Event.Terminate());
        assertEquals(DRAINING, m.state());
        assertTrue(m.terminating());
        m.onEvent(new Event.DrainComplete());
        assertEquals(STOPPED, m.state());
    }

    @Test void terminateFromStandbyStopsImmediately() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.Terminate());
        assertEquals(STOPPED, m.state());
        assertTrue(m.terminating());
    }

    @Test void stoppedRestartsOnRunningSpec() {
        var m = new SessionStateMachine("t");
        m.onEvent(new Event.SpecChanged(spec(DesiredState.STOPPED, "v1", "s", 10)));
        assertEquals(STOPPED, m.state());
        m.onEvent(new Event.SpecChanged(running("v2")));
        assertEquals(CONNECTING, m.state());
    }
}
```

- [ ] **Step 2: 跑測試確認 fail**

Run: `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon test`
Expected: FAIL — compile error（session 套件不存在）。

- [ ] **Step 3: 實作三個檔**

`runtime/src/main/java/dc/listener/session/ObservedState.java`：

```java
package dc.listener.session;

/** guidance 8.3 的七個 lifecycle state；Runtime 唯一對外回報的 observed 值。 */
public enum ObservedState { STANDBY, CONNECTING, ACTIVE, DRAINING, DEGRADED, FAILED, STOPPED }
```

`runtime/src/main/java/dc/listener/session/Event.java`：

```java
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
    record Terminate() implements Event {}
}
```

`runtime/src/main/java/dc/listener/session/SessionStateMachine.java`：

```java
package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Instant;

/** 唯一權威的 lifecycle 狀態（guidance 8.1）。純轉移邏輯、零 I/O；動作由 ListenerSession loop 執行。 */
public final class SessionStateMachine {

    private final String name;
    private ObservedState state = ObservedState.STANDBY;
    private SessionSpec spec;        // 已生效（applied）
    private SessionSpec pending;     // 等 drain 完成才套用
    private int failures;            // 連續 connect 失敗次數（含初次）
    private String reason = "";
    private Instant lastTransition = Instant.now();
    private boolean terminating;     // YAML entry 已刪除 → STOPPED 後刪 consumer、結束 thread

    public SessionStateMachine(String name) { this.name = name; }

    public synchronized void onEvent(Event e) {
        switch (e) {
            case Event.SpecChanged sc -> onSpec(sc.spec());
            case Event.SpecInvalid si -> {
                pending = null;
                reason = "INVALID_SPEC: " + si.error();
                moveTo(ObservedState.FAILED);
            }
            case Event.ConnectOk ok -> {
                if (state == ObservedState.CONNECTING) {
                    failures = 0;
                    reason = "";
                    moveTo(ObservedState.ACTIVE);
                }
            }
            case Event.ConnectFailed f -> onConnectFailed(f.reason());
            case Event.FetchError fe -> {
                if (state == ObservedState.ACTIVE) {
                    failures = 1;
                    reason = fe.reason();
                    moveTo(ObservedState.DEGRADED);
                }
            }
            case Event.RetryTick rt -> {
                if (state == ObservedState.DEGRADED) moveTo(ObservedState.CONNECTING);
            }
            case Event.DrainComplete dc -> onDrainComplete();
            case Event.DrainTimeout dt -> onDrainTimeout();
            case Event.Terminate x -> onTerminate();
        }
    }

    private void onSpec(SessionSpec next) {
        switch (state) {
            case ACTIVE -> {
                if (hotOnly(spec, next)) spec = next;              // retry/drainTimeout 熱生效（spec §5.1）
                else { pending = next; moveTo(ObservedState.DRAINING); }
            }
            case DRAINING -> pending = next;                       // 完成當前轉移再收斂（spec §4.2）
            default -> { adopt(next); converge(); }                // FAILED 的重置、DEGRADED 的立即中斷都走這裡
        }
    }

    private void adopt(SessionSpec next) {
        spec = next;
        pending = null;
        failures = 0;
        reason = "";
    }

    private void converge() {
        switch (spec.desiredState()) {
            case RUNNING -> moveTo(ObservedState.CONNECTING);
            case STANDBY -> moveTo(ObservedState.STANDBY);
            case STOPPED -> moveTo(ObservedState.STOPPED);
        }
    }

    private void onConnectFailed(String r) {
        if (state != ObservedState.CONNECTING) return;
        failures++;
        if (failures > spec.maxAttempts()) {                       // 初次 + maxAttempts 次重試全失敗
            reason = "RETRY_EXHAUSTED";
            moveTo(ObservedState.FAILED);
        } else {
            reason = r;
            moveTo(ObservedState.DEGRADED);
        }
    }

    private void onDrainComplete() {
        if (state != ObservedState.DRAINING) return;
        if (terminating) { moveTo(ObservedState.STOPPED); return; }
        if (pending != null) adopt(pending);
        converge();
    }

    private void onDrainTimeout() {
        if (state != ObservedState.DRAINING) return;
        reason = "DRAIN_TIMEOUT";
        if (terminating) moveTo(ObservedState.STOPPED);
        else { pending = null; moveTo(ObservedState.FAILED); }
    }

    private void onTerminate() {
        terminating = true;
        switch (state) {
            case ACTIVE -> moveTo(ObservedState.DRAINING);
            case DRAINING -> { }
            default -> moveTo(ObservedState.STOPPED);
        }
    }

    private boolean hotOnly(SessionSpec a, SessionSpec b) {
        return a.desiredState() == b.desiredState()
                && a.subject().equals(b.subject())
                && a.durable().equals(b.durable())
                && a.configVersion().equals(b.configVersion());
    }

    private void moveTo(ObservedState next) {
        if (state != next) {
            state = next;
            lastTransition = Instant.now();
            System.out.println("[" + name + "] -> " + next + (reason.isEmpty() ? "" : " (" + reason + ")"));
        }
    }

    public synchronized ObservedState state() { return state; }
    public synchronized SessionSpec spec() { return spec; }
    public synchronized String reason() { return reason; }
    public synchronized int retryAttempt() { return failures; }
    public synchronized boolean terminating() { return terminating; }
    public synchronized Instant lastTransitionTime() { return lastTransition; }
    public synchronized String declaredConfigVersion() {
        var d = pending != null ? pending : spec;
        return d == null ? null : d.configVersion();
    }
    public synchronized String appliedConfigVersion() { return spec == null ? null : spec.configVersion(); }
    public synchronized dc.listener.spec.DesiredState declaredDesired() {
        var d = pending != null ? pending : spec;
        return d == null ? null : d.desiredState();
    }
}
```

- [ ] **Step 4: 跑測試確認 pass**

Run: 同 Step 2 指令。
Expected: `SessionStateMachineTest` 20 tests PASS + Task 1 的 7 tests 仍 PASS。

- [ ] **Step 5: Commit**

```bash
git add runtime/src
git commit -m "runtime: SessionStateMachine — 7-state lifecycle, error classification, drain/retry semantics"
```

---

### Task 3: ListenerSession loop + NatsLink 介面 + AdmissionGate + PipelineStub

**Files:**
- Create: `runtime/src/main/java/dc/listener/session/InFlightMsg.java`
- Create: `runtime/src/main/java/dc/listener/session/LinkException.java`
- Create: `runtime/src/main/java/dc/listener/session/NatsLink.java`
- Create: `runtime/src/main/java/dc/listener/session/AdmissionGate.java`
- Create: `runtime/src/main/java/dc/listener/session/PipelineStub.java`
- Create: `runtime/src/main/java/dc/listener/session/SessionStatus.java`
- Create: `runtime/src/main/java/dc/listener/session/ListenerSession.java`
- Test: `runtime/src/test/java/dc/listener/Await.java`
- Test: `runtime/src/test/java/dc/listener/session/FakeNatsLink.java`
- Test: `runtime/src/test/java/dc/listener/session/ListenerSessionTest.java`

**Interfaces:**
- Consumes: Task 2 的 `SessionStateMachine`、`Event`、`ObservedState`；Task 1 的 `SessionSpec`。
- Produces: `interface NatsLink`：`void connect(SessionSpec) throws LinkException`、`List<InFlightMsg> fetch(int max, Duration wait) throws LinkException`、`void ack(InFlightMsg)`、`long pending() throws Exception`、`boolean isConnected()`、`void deleteConsumer(SessionSpec)`、`void close()`。
- Produces: `record InFlightMsg(String data, Object handle)`；`class LinkException extends Exception` 帶 `String reasonCode()`。
- Produces: `class ListenerSession`：`ListenerSession(String name, NatsLink link, long processDelayMs)`、`void start()`（virtual thread）、`void deliver(Event)`（mailbox）、`SessionStatus snapshot()`、`boolean isTerminated()`。Task 5 的 Reconciler 只用 `start/deliver/snapshot/isTerminated` 四個方法。
- Produces: `record SessionStatus(String name, String subject, DesiredState desiredState, ObservedState observedState, String declaredConfigVersion, String appliedConfigVersion, boolean configurationReady, boolean connectionReady, boolean consumerReady, boolean admissionAllowed, String reason, Instant lastTransitionTime, long admittedCount, long pendingCount, int retryAttempt)`。

Loop 設計要點（實作時照抄下方程式碼即可）：
- **STANDBY/STOPPED/FAILED**：阻塞等 mailbox（2s timeout 順便刷新 pendingCount）。STOPPED+terminating → 刪 consumer、關線、結束 thread（offboarding，spec §4.3）。FAILED/STOPPED 進入時關閉連線；STANDBY **保留**連線（spec §4.1，場景 4 靠它在 STANDBY 讀 numPending）。
- **CONNECTING**：先 `close()` 再 `connect()` —— 重連與初次走完全相同路徑（spec §4.2）。
- **ACTIVE**：先非阻塞收 mailbox；in-flight 空了才 fetch（batch 10、等 1s）；一次處理**一則**（process→ack），訊息之間都會再看 mailbox —— 這讓 DRAINING 有可觀察窗口（batch 10 × 200ms ≈ 2s，spec §4.4）。
- **DRAINING**：不再 fetch，把 in-flight（已 fetch 未 ack）逐則 process→ack；清空 → `DrainComplete`；超過 drainTimeout → 丟棄剩餘（不 ack，之後 redelivery，at-least-once）→ `DrainTimeout`。
- **DEGRADED**：`mailbox.poll(retryDelay)` —— 有 spec 變更立即中斷等待，timeout 才 `RetryTick`。退避 = `retryInterval` 固定值，否則 `min(2^(failures-1), 30)` 秒。

- [ ] **Step 1: 寫 failing test（含 Await helper 與 FakeNatsLink）**

`runtime/src/test/java/dc/listener/Await.java`：

```java
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
```

`runtime/src/test/java/dc/listener/session/FakeNatsLink.java`：

```java
package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 測試替身：可注入 connect 失敗次數、可餵訊息、記錄 ack 與 deleteConsumer。
 *  欄位 public：ReconcilerTest（dc.listener.reconcile package）也要直接讀。 */
public final class FakeNatsLink implements NatsLink {
    public final ConcurrentLinkedDeque<String> messages = new ConcurrentLinkedDeque<>();
    public final List<String> acked = new CopyOnWriteArrayList<>();
    public final AtomicInteger connectCalls = new AtomicInteger();
    public volatile int connectFailures;
    public volatile String failReason = "MESSAGING_ENDPOINT_UNREACHABLE";
    public volatile boolean connected;
    public volatile boolean consumerDeleted;

    @Override public void connect(SessionSpec spec) throws LinkException {
        connectCalls.incrementAndGet();
        if (connectFailures > 0) {
            connectFailures--;
            throw new LinkException(failReason, null);
        }
        connected = true;
    }

    @Override public List<InFlightMsg> fetch(int max, Duration wait) {
        var out = new ArrayList<InFlightMsg>();
        String d;
        while (out.size() < max && (d = messages.poll()) != null) out.add(new InFlightMsg(d, null));
        if (out.isEmpty()) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return out;
    }

    @Override public void ack(InFlightMsg m) { acked.add(m.data()); }
    @Override public long pending() { return messages.size(); }
    @Override public boolean isConnected() { return connected; }
    @Override public void deleteConsumer(SessionSpec spec) { consumerDeleted = true; }
    @Override public void close() { connected = false; }
}
```

`runtime/src/test/java/dc/listener/session/ListenerSessionTest.java`：

```java
package dc.listener.session;

import dc.listener.Await;
import dc.listener.spec.DesiredState;
import dc.listener.spec.SessionSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ListenerSessionTest {

    static SessionSpec spec(DesiredState d, Duration retryInterval, int maxAttempts) {
        return new SessionSpec("t", d, "v1", "tool.t.events", "dur-t",
                retryInterval, maxAttempts, Duration.ofSeconds(5));
    }

    @Test
    void connectsConsumesAndAcks() {
        var link = new FakeNatsLink();
        link.messages.addAll(List.of("m1", "m2", "m3"));
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        Await.until(() -> s.snapshot().admittedCount() == 3, 2000);
        assertEquals(List.of("m1", "m2", "m3"), link.acked);
        var st = s.snapshot();
        assertTrue(st.admissionAllowed());
        assertTrue(st.connectionReady());
        assertTrue(st.consumerReady());
    }

    @Test
    void degradedRetriesThenEscalatesToFailed() {
        var link = new FakeNatsLink();
        link.connectFailures = 99;
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, Duration.ofMillis(20), 2)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 3000);
        assertEquals("RETRY_EXHAUSTED", s.snapshot().reason());
        assertEquals(3, link.connectCalls.get());   // 初次 + 2 次重試
    }

    @Test
    void drainsToStandbyAndStopsConsuming() {
        var link = new FakeNatsLink();
        for (int i = 0; i < 20; i++) link.messages.add("m" + i);
        var s = new ListenerSession("t", link, 20);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().admittedCount() >= 2, 3000);
        s.deliver(new Event.SpecChanged(spec(DesiredState.STANDBY, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.STANDBY, 3000);
        assertTrue(link.connected, "STANDBY 保留連線（spec §4.1）");
        int ackedAfterDrain = link.acked.size();
        link.messages.add("late");
        try { Thread.sleep(200); } catch (InterruptedException e) { throw new RuntimeException(e); }
        assertEquals(ackedAfterDrain, link.acked.size(), "STANDBY 不得再 fetch/ack");
        assertFalse(s.snapshot().admissionAllowed());
    }

    @Test
    void terminateDeletesConsumerAndExits() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        s.deliver(new Event.Terminate());
        Await.until(s::isTerminated, 3000);
        assertTrue(link.consumerDeleted);
        assertFalse(link.connected);
    }

    @Test
    void invalidSpecFailsAndClosesConnection() {
        var link = new FakeNatsLink();
        var s = new ListenerSession("t", link, 0);
        s.start();
        s.deliver(new Event.SpecChanged(spec(DesiredState.RUNNING, null, 10)));
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        s.deliver(new Event.SpecInvalid("missing subject"));
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 2000);
        Await.until(() -> !link.connected, 2000);
        assertFalse(s.snapshot().configurationReady());
    }
}
```

- [ ] **Step 2: 跑測試確認 fail**

Run: `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon test`
Expected: FAIL — compile error（`NatsLink` 等類別不存在）。

- [ ] **Step 3: 實作六個 main 檔**

`runtime/src/main/java/dc/listener/session/InFlightMsg.java`：

```java
package dc.listener.session;

/** 已 fetch 未 ack 的訊息（spec §4.4 的 in-flight 定義）。handle 由實作自用（JnatsLink 放 io.nats Message）。 */
public record InFlightMsg(String data, Object handle) {}
```

`runtime/src/main/java/dc/listener/session/LinkException.java`：

```java
package dc.listener.session;

/** NatsLink 錯誤，一律帶 reason code；分類規則見 spec §4.2（連線類與資源不存在都走 DEGRADED）。 */
public class LinkException extends Exception {
    private final String reasonCode;

    public LinkException(String reasonCode, Throwable cause) {
        super(reasonCode, cause);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() { return reasonCode; }
}
```

`runtime/src/main/java/dc/listener/session/NatsLink.java`：

```java
package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.util.List;

/** session 專屬的 NATS 通道：一條 Connection + 一個 durable pull consumer。 */
public interface NatsLink {
    /** 連線 + createOrUpdate durable consumer；重連與初次走同一路徑。 */
    void connect(SessionSpec spec) throws LinkException;

    List<InFlightMsg> fetch(int max, Duration wait) throws LinkException;

    void ack(InFlightMsg m);

    /** consumer 的 server 端 backlog（numPending）；不可用時丟例外，呼叫端保留舊值。 */
    long pending() throws Exception;

    boolean isConnected();

    /** offboarding 專用：刪 server 端 durable consumer（best-effort）。 */
    void deleteConsumer(SessionSpec spec);

    /** 冪等、不丟例外。 */
    void close();
}
```

`runtime/src/main/java/dc/listener/session/AdmissionGate.java`：

```java
package dc.listener.session;

/** guidance 8.1：admission 由 lifecycle state 推導，不是獨立狀態機。 */
public final class AdmissionGate {
    private final SessionStateMachine machine;

    public AdmissionGate(SessionStateMachine machine) { this.machine = machine; }

    public boolean admits() { return machine.state() == ObservedState.ACTIVE; }
}
```

`runtime/src/main/java/dc/listener/session/PipelineStub.java`：

```java
package dc.listener.session;

import java.util.concurrent.atomic.AtomicLong;

/** Data Pipeline 替身：人工延遲 + 冪等計數（redelivery 只表現為重複計數，spec §4.5）。 */
public final class PipelineStub {
    private final long delayMs;
    private final AtomicLong admitted = new AtomicLong();

    public PipelineStub(long delayMs) { this.delayMs = delayMs; }

    public void process(InFlightMsg m) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        admitted.incrementAndGet();
    }

    public long admitted() { return admitted.get(); }
}
```

`runtime/src/main/java/dc/listener/session/SessionStatus.java`：

```java
package dc.listener.session;

import dc.listener.spec.DesiredState;

import java.time.Instant;

/** /status 用的唯讀快照（spec §6）。 */
public record SessionStatus(
        String name,
        String subject,
        DesiredState desiredState,
        ObservedState observedState,
        String declaredConfigVersion,
        String appliedConfigVersion,
        boolean configurationReady,
        boolean connectionReady,
        boolean consumerReady,
        boolean admissionAllowed,
        String reason,
        Instant lastTransitionTime,
        long admittedCount,
        long pendingCount,
        int retryAttempt) {
}
```

`runtime/src/main/java/dc/listener/session/ListenerSession.java`：

```java
package dc.listener.session;

import dc.listener.spec.SessionSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** 一個 session = 專屬 NatsLink + 專屬 virtual thread + mailbox；跨 session 零共享可變狀態（P3）。 */
public final class ListenerSession {
    private static final int FETCH_BATCH = 10;

    private final String name;
    private final NatsLink link;
    private final PipelineStub pipeline;
    private final SessionStateMachine machine;
    private final AdmissionGate gate;
    private final BlockingQueue<Event> mailbox = new LinkedBlockingQueue<>();
    private final Deque<InFlightMsg> inFlight = new ArrayDeque<>();
    private volatile long pendingCount = -1;
    private volatile boolean terminated;
    private Instant drainDeadline;

    public ListenerSession(String name, NatsLink link, long processDelayMs) {
        this.name = name;
        this.link = link;
        this.pipeline = new PipelineStub(processDelayMs);
        this.machine = new SessionStateMachine(name);
        this.gate = new AdmissionGate(machine);
    }

    public void start() { Thread.ofVirtual().name("session-" + name).start(this::run); }

    public void deliver(Event e) { mailbox.add(e); }

    public boolean isTerminated() { return terminated; }

    private void run() {
        try {
            while (true) {
                ObservedState s = machine.state();
                switch (s) {
                    case STANDBY, STOPPED, FAILED -> {
                        if (s == ObservedState.STOPPED && machine.terminating()) {
                            if (machine.spec() != null) link.deleteConsumer(machine.spec());
                            link.close();
                            terminated = true;
                            return;
                        }
                        if ((s == ObservedState.STOPPED || s == ObservedState.FAILED) && link.isConnected()) {
                            link.close();
                        }
                        Event e = mailbox.poll(2, TimeUnit.SECONDS);
                        if (e != null) machine.onEvent(e); else refreshPending();
                    }
                    case CONNECTING -> {
                        link.close();   // 重連 = 與初次完全相同的路徑（spec §4.2）
                        try {
                            link.connect(machine.spec());
                            machine.onEvent(new Event.ConnectOk());
                        } catch (LinkException ex) {
                            machine.onEvent(new Event.ConnectFailed(ex.reasonCode()));
                        }
                    }
                    case ACTIVE -> {
                        Event e = mailbox.poll();
                        if (e != null) { machine.onEvent(e); continue; }
                        if (inFlight.isEmpty()) {
                            try {
                                inFlight.addAll(link.fetch(FETCH_BATCH, Duration.ofSeconds(1)));
                                refreshPending();
                            } catch (LinkException ex) {
                                machine.onEvent(new Event.FetchError(ex.reasonCode()));
                                continue;
                            }
                        }
                        if (!inFlight.isEmpty() && gate.admits()) processOne();
                    }
                    case DRAINING -> {
                        if (drainDeadline == null) {
                            drainDeadline = Instant.now().plus(machine.spec().drainTimeout());
                        }
                        Event e = mailbox.poll();
                        if (e != null) { machine.onEvent(e); continue; }
                        if (inFlight.isEmpty()) {
                            machine.onEvent(new Event.DrainComplete());
                        } else if (Instant.now().isAfter(drainDeadline)) {
                            inFlight.clear();   // 未 ack → 之後 redelivery（at-least-once）
                            machine.onEvent(new Event.DrainTimeout());
                        } else {
                            processOne();
                        }
                    }
                    case DEGRADED -> {
                        Event e = mailbox.poll(retryDelay().toMillis(), TimeUnit.MILLISECONDS);
                        machine.onEvent(e != null ? e : new Event.RetryTick());
                    }
                }
                if (machine.state() != ObservedState.DRAINING) drainDeadline = null;
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void processOne() {
        InFlightMsg m = inFlight.removeFirst();
        pipeline.process(m);
        link.ack(m);
    }

    private Duration retryDelay() {
        SessionSpec sp = machine.spec();
        if (sp.retryInterval() != null) return sp.retryInterval();
        int f = Math.max(machine.retryAttempt(), 1);
        return Duration.ofSeconds(Math.min(1L << (f - 1), 30));   // 指數退避 1s→30s（spec §4.2）
    }

    private void refreshPending() {
        try { pendingCount = link.pending(); } catch (Exception e) { /* 保留舊值 */ }
    }

    public SessionStatus snapshot() {
        ObservedState st = machine.state();
        SessionSpec sp = machine.spec();
        return new SessionStatus(
                name,
                sp == null ? null : sp.subject(),
                machine.declaredDesired(),
                st,
                machine.declaredConfigVersion(),
                machine.appliedConfigVersion(),
                sp != null && !machine.reason().startsWith("INVALID_SPEC"),
                link.isConnected(),
                st == ObservedState.ACTIVE || st == ObservedState.DRAINING,
                gate.admits(),
                machine.reason(),
                machine.lastTransitionTime(),
                pipeline.admitted(),
                pendingCount,
                machine.retryAttempt());
    }
}
```

- [ ] **Step 4: 跑測試確認 pass**

Run: 同 Step 2 指令。
Expected: `ListenerSessionTest` 5 tests PASS；累計 32 tests PASS。

- [ ] **Step 5: Commit**

```bash
git add runtime/src
git commit -m "runtime: ListenerSession actor loop, NatsLink interface, admission gate, pipeline stub"
```

---

### Task 4: JnatsLink（真實 NATS 實作）

**Files:**
- Create: `runtime/src/main/java/dc/listener/session/JnatsLink.java`

**Interfaces:**
- Consumes: Task 3 的 `NatsLink`、`InFlightMsg`、`LinkException`；Task 1 的 `SessionSpec`。
- Produces: `class JnatsLink implements NatsLink`，建構子 `JnatsLink(String natsUrl)`；stream 名固定 `TOOL_EVENTS`。

無單元測試（需要真 NATS server）—— 行為由 Task 7 的 smoke test 驗證。本 task 只要求編譯通過。

錯誤分類對照（spec §4.2）：連線層 `IOException`/`InterruptedException` → `MESSAGING_ENDPOINT_UNREACHABLE`；JetStream API 層 `JetStreamApiException`（stream 不存在、filter 不合法等）→ `RESOURCE_NOT_FOUND`。兩者都由狀態機收斂成 DEGRADED。

- [ ] **Step 1: 實作**

`runtime/src/main/java/dc/listener/session/JnatsLink.java`：

```java
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
```

- [ ] **Step 2: 編譯（含既有測試）確認通過**

Run: `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon build`
Expected: `BUILD SUCCESSFUL`（32 tests 仍 PASS）。若 jnats 2.20.2 的 API 名稱有出入（如 `FetchConsumeOptions` builder 方法名），以編譯錯誤訊息為準對照 javadoc 修正 —— 允許小幅調整呼叫方式，但 `maxReconnects(0)`、durable pull consumer、delete+recreate fallback 三個語意不可變。

- [ ] **Step 3: Commit**

```bash
git add runtime/src
git commit -m "runtime: JnatsLink — durable pull consumer, maxReconnects(0), delete+recreate fallback"
```

---

### Task 5: Reconciler + FileWatcher

**Files:**
- Create: `runtime/src/main/java/dc/listener/reconcile/Reconciler.java`
- Create: `runtime/src/main/java/dc/listener/reconcile/FileWatcher.java`
- Test: `runtime/src/test/java/dc/listener/reconcile/ReconcilerTest.java`
- Modify: `docs/specs/2026-07-13-listener-session-nats-prototype-design.md`（§5.2 偵測機制修訂）

**Interfaces:**
- Consumes: Task 1 `SpecParser`；Task 3 `ListenerSession`（`start/deliver/snapshot/isTerminated`）、`Event`。
- Produces: `class Reconciler`：`Reconciler(Path file, Function<String, ListenerSession> factory)`、`void reload()`（startup 讀檔 + apply）、`void applySnapshot(String yamlText)`（套用 watcher 已驗證的同一份內容）、`void apply(String yamlText)`（package-private，測試用）、`Map<String, ListenerSession> sessions()`、`String specError()`。
- Produces: `class FileWatcher`：`FileWatcher(Path file, Consumer<String> onChange)`、`void start()`；callback 直接收到已連續兩次驗證相同的內容，失敗時自動以同內容重試。

**Code-review amendments:** desired declaration 與已成功投遞的 diff baseline 分開；快速
remove/re-add 會在舊 instance 完成退場後自動建立 replacement；parser 將非字串 session key
正規化為 `SpecParseException`。下方初始 TDD sketch 由實際 source 與 regression tests 取代。

- [ ] **Step 1: 寫 failing test**

`runtime/src/test/java/dc/listener/reconcile/ReconcilerTest.java`：

```java
package dc.listener.reconcile;

import dc.listener.Await;
import dc.listener.session.FakeNatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.session.ObservedState;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class ReconcilerTest {

    final Map<String, FakeNatsLink> links = new ConcurrentHashMap<>();
    final Reconciler rec = new Reconciler(Path.of("/nonexistent"),
            name -> new ListenerSession(name, links.computeIfAbsent(name, n -> new FakeNatsLink()), 0));

    static String yaml(String name, String desired, String subject, String ver) {
        return """
            sessions:
              %s:
                desiredState: %s
                configVersion: %s
                config:
                  subject: %s
                  durable: dur-%s
            """.formatted(name, desired, ver, subject, name);
    }

    @Test
    void addStartsSessionAndConverges() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        assertNotNull(s);
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
    }

    @Test
    void unchangedSpecIsNotRedelivered() {
        String y = yaml("tool-a", "RUNNING", "tool.a.events", "v1");
        rec.apply(y);
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply(y);
        rec.apply(y);
        assertEquals(1, links.get("tool-a").connectCalls.get());
    }

    @Test
    void configChangeTriggersReconnectWithNewVersion() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events.v2", "v2"));
        Await.until(() -> "v2".equals(s.snapshot().appliedConfigVersion())
                && s.snapshot().observedState() == ObservedState.ACTIVE, 3000);
        assertEquals(2, links.get("tool-a").connectCalls.get());
    }

    @Test
    void removedEntryTerminatesSessionAndDeletesConsumer() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply("sessions: {}");
        Await.until(s::isTerminated, 3000);
        assertTrue(links.get("tool-a").consumerDeleted);
        rec.apply("sessions: {}");   // 下一次 reload 清掉已終止 session
        assertFalse(rec.sessions().containsKey("tool-a"));
    }

    @Test
    void brokenYamlKeepsLastGoodDeclaration() {
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        var s = rec.sessions().get("tool-a");
        Await.until(() -> s.snapshot().observedState() == ObservedState.ACTIVE, 2000);
        rec.apply("sessions: [broken");
        assertNotNull(rec.specError());
        assertTrue(rec.sessions().containsKey("tool-a"));
        assertEquals(ObservedState.ACTIVE, s.snapshot().observedState());
        rec.apply(yaml("tool-a", "RUNNING", "tool.a.events", "v1"));
        assertNull(rec.specError());
    }

    @Test
    void invalidEntryCreatesFailedSession() {
        rec.apply("""
            sessions:
              tool-x:
                desiredState: RUNNING
                configVersion: v1
                config:
                  durable: dur-x
            """);
        var s = rec.sessions().get("tool-x");
        assertNotNull(s);
        Await.until(() -> s.snapshot().observedState() == ObservedState.FAILED, 2000);
        assertTrue(s.snapshot().reason().startsWith("INVALID_SPEC"));
    }
}
```

- [ ] **Step 2: 跑測試確認 fail**

Run: `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon test`
Expected: FAIL — compile error（`Reconciler` 不存在）。

- [ ] **Step 3: 實作兩個檔**

`runtime/src/main/java/dc/listener/reconcile/Reconciler.java`：

```java
package dc.listener.reconcile;

import dc.listener.session.Event;
import dc.listener.session.ListenerSession;
import dc.listener.spec.SessionSpec;
import dc.listener.spec.SpecParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 宣告 → 現實的單向收斂（spec §5.2）：parse → diff → 投遞事件到 per-session mailbox。
 * 不直接碰 connection；一個 session 卡住不阻塞其他 session 的 reconcile（P3）。
 */
public final class Reconciler {
    private final Path file;
    private final Function<String, ListenerSession> factory;
    private final Map<String, ListenerSession> sessions = new ConcurrentHashMap<>();
    private Map<String, SessionSpec> lastValid = Map.of();
    private Map<String, String> lastInvalid = Map.of();
    private volatile String specError;

    public Reconciler(Path file, Function<String, ListenerSession> factory) {
        this.file = file;
        this.factory = factory;
    }

    public synchronized void reload() {
        String text;
        try {
            text = Files.readString(file);
        } catch (IOException e) {
            specError = "cannot read " + file + ": " + e.getMessage();
            return;
        }
        apply(text);
    }

    synchronized void apply(String text) {
        SpecParser.Parsed parsed;
        try {
            parsed = SpecParser.parse(text);
        } catch (SpecParser.SpecParseException e) {
            specError = "spec parse error: " + e.getMessage();   // 保留上一份有效宣告（spec §5.2）
            return;
        }
        specError = null;

        sessions.entrySet().removeIf(en -> en.getValue().isTerminated());

        for (String name : List.copyOf(sessions.keySet())) {
            if (!parsed.valid().containsKey(name) && !parsed.invalid().containsKey(name)) {
                sessions.get(name).deliver(new Event.Terminate());   // 刪 entry = 永久退場（spec §4.3）
            }
        }
        parsed.valid().forEach((name, spec) -> {
            if (!spec.equals(lastValid.get(name))) {
                sessionFor(name).deliver(new Event.SpecChanged(spec));
            }
        });
        parsed.invalid().forEach((name, err) -> {
            if (!err.equals(lastInvalid.get(name))) {
                sessionFor(name).deliver(new Event.SpecInvalid(err));
            }
        });
        lastValid = parsed.valid();
        lastInvalid = parsed.invalid();
    }

    private ListenerSession sessionFor(String name) {
        // ponytail: 同名 entry 在退場完成前重新加回會拿到將死的 session；下一次 reload 才重生。demo 不會這樣操作。
        return sessions.computeIfAbsent(name, n -> {
            var s = factory.apply(n);
            s.start();
            return s;
        });
    }

    public Map<String, ListenerSession> sessions() { return Map.copyOf(sessions); }

    public String specError() { return specError; }
}
```

`runtime/src/main/java/dc/listener/reconcile/FileWatcher.java`：

```java
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
```

- [ ] **Step 4: 跑測試確認 pass**

Run: 同 Step 2 指令。
Expected: Task 5 regression tests 與既有 suite 全部 PASS（目前累計 50 tests）。

- [ ] **Step 5: 同步修訂 spec §5.2 的偵測機制描述**

編輯 `docs/specs/2026-07-13-listener-session-nats-prototype-design.md`，把：

```
檔案變更（WatchService + 200ms debounce，容忍編輯器「先刪再寫」）
```

改成：

```
檔案變更（平台中立的 500ms 內容輪詢；連續兩次相同才把該份 snapshot
  直接交給 Reconciler，穩定後 ≤1s 觸發 reconcile）
```

- [ ] **Step 6: Commit**

```bash
git add runtime/src docs/specs
git commit -m "runtime: Reconciler diff + terminate flow, FileWatcher content-hash polling; spec §5.2 updated to match"
```

---

### Task 6: StatusServer + Main（組裝）

**Files:**
- Create: `runtime/src/main/java/dc/listener/status/StatusServer.java`
- Create: `runtime/src/main/java/dc/listener/Main.java`
- Test: `runtime/src/test/java/dc/listener/status/StatusServerTest.java`

**Interfaces:**
- Consumes: Task 5 `Reconciler`（`sessions()`、`specError()`）；Task 3 `SessionStatus`。
- Produces: `class StatusServer`：`StatusServer(int port, Reconciler rec) throws IOException`、`void start()`；唯一精確 route `GET /status` 回 spec §6 的 JSON（多一個 `subject` 欄位，watch-status.sh 的表格要用）。package-private `port()` / `stop()` 供 HTTP contract tests 使用。
- Produces: `Main.main()`：env `NATS_URL`（預設 `nats://localhost:4222`）、`SESSIONS_FILE`（預設 `config/sessions.yaml`）、`PROCESS_DELAY_MS`（預設 `200`）。

三個 HTTP contract tests 驗證：有效 JSON/status snapshot、multiline `specError` escaping，
以及只有精確 `GET /status` 可服務。Task 7 smoke test 會再以真實 runtime/NATS 驗證。

**Code-review amendments:** 最終 reviewed source 對所有 JSON C0 control characters 做 escaping，
拒絕非 GET 與 `/status/*`，並以 ephemeral port/stop hooks 隔離測試；下方 initial sketch
由實際 source 與 `StatusServerTest` 取代。

- [ ] **Step 1: 實作兩個檔**

`runtime/src/main/java/dc/listener/status/StatusServer.java`：

```java
package dc.listener.status;

import com.sun.net.httpserver.HttpServer;
import dc.listener.reconcile.Reconciler;
import dc.listener.session.SessionStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

/** 唯讀觀察介面（spec §6）：GET /status，無其他 route。 */
public final class StatusServer {
    private final HttpServer http;
    private final Reconciler reconciler;

    public StatusServer(int port, Reconciler reconciler) throws IOException {
        this.reconciler = reconciler;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/status", exchange -> {
            byte[] body = json().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) { os.write(body); }
        });
    }

    public void start() { http.start(); }

    private String json() {
        var sb = new StringBuilder();
        sb.append("{\"cell\":{\"cellId\":\"cell-1\",\"specError\":")
          .append(q(reconciler.specError()))
          .append("},\"sessions\":{");
        boolean first = true;
        for (var en : new TreeMap<>(reconciler.sessions()).entrySet()) {
            if (en.getValue().isTerminated()) continue;   // 已退場的不再回報
            SessionStatus s = en.getValue().snapshot();
            if (!first) sb.append(',');
            first = false;
            sb.append(q(en.getKey())).append(":{")
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

    private static String q(String s) {
        if (s == null) return "null";
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + '"';
    }
}
```

`runtime/src/main/java/dc/listener/Main.java`：

```java
package dc.listener;

import dc.listener.reconcile.FileWatcher;
import dc.listener.reconcile.Reconciler;
import dc.listener.session.JnatsLink;
import dc.listener.session.ListenerSession;
import dc.listener.status.StatusServer;

import java.nio.file.Path;

/** 組裝殼：env 讀靜態設定（spec §3），邏輯全在 spec/session/reconcile。 */
public final class Main {
    public static void main(String[] args) throws Exception {
        String natsUrl = env("NATS_URL", "nats://localhost:4222");
        Path sessionsFile = Path.of(env("SESSIONS_FILE", "config/sessions.yaml"));
        long processDelayMs = Long.parseLong(env("PROCESS_DELAY_MS", "200"));

        var reconciler = new Reconciler(sessionsFile,
                name -> new ListenerSession(name, new JnatsLink(natsUrl), processDelayMs));
        reconciler.reload();
        new FileWatcher(sessionsFile, reconciler::applySnapshot).start();
        new StatusServer(8080, reconciler).start();
        System.out.println("listener-runtime up | sessions=" + sessionsFile
                + " | nats=" + natsUrl + " | status=:8080/status");
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
```

- [ ] **Step 2: 編譯確認通過**

Run: `docker run --rm -v "$(pwd)/runtime":/work -w /work -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 gradle --no-daemon build`
Expected: `BUILD SUCCESSFUL`，53 tests PASS。

- [ ] **Step 3: Commit**

```bash
git add runtime/src
git commit -m "runtime: StatusServer /status JSON + Main assembly"
```

---

### Task 7: Docker 化 + compose + smoke test

**Files:**
- Create: `runtime/Dockerfile`
- Create: `docker-compose.yml`
- Create: `config/sessions.yaml`
- Create: `demo/smoke-test.sh`

**Interfaces:**
- Consumes: Task 6 的可執行 runtime（`gradle installDist` 產出 `build/install/runtime/bin/runtime`）。
- Produces: `docker compose up -d --build` 起三個服務；`demo/smoke-test.sh` 端到端自檢（CI 可跑），成功輸出 `SMOKE TEST PASS`、exit 0。

**Code-review amendments:** publisher 使用已 smoke-tested 的 nats CLI 0.4.0 image digest；
smoke cleanup 是單一冪等 EXIT handler，INT/TERM 分別保留 130/143，避免 signal 後繼續執行
或重複 cleanup。下方指令與實際 source 已同步。

- [ ] **Step 1: 寫 Dockerfile**

`runtime/Dockerfile`：

```dockerfile
FROM gradle:8.7-jdk21 AS build
WORKDIR /app
COPY settings.gradle build.gradle ./
COPY src ./src
RUN gradle --no-daemon installDist

FROM eclipse-temurin:21-jre
COPY --from=build /app/build/install/runtime /opt/runtime
ENTRYPOINT ["/opt/runtime/bin/runtime"]
```

- [ ] **Step 2: 寫 docker-compose.yml 與初始 sessions.yaml**

`docker-compose.yml`（repo 根目錄）：

```yaml
services:
  upstream-nats:                      # 上游擁有（spec §3）
    image: nats:2.10-alpine
    command: ["-js", "-sd", "/data"]
    ports:
      - "4222:4222"

  upstream-publisher:                 # 上游擁有：冪等建 stream + 每秒發訊（spec §4.3）
    # Smoke-tested nats CLI 0.4.0；digest pin 保持 Task 8 command semantics 可重現。
    image: natsio/nats-box@sha256:ffce8bd103383f179f8c7f11cf645726acf5d17280706c530c3b342dbe16334c
    environment:
      NATS_URL: nats://upstream-nats:4222
      SUBJECTS: ${PUBLISHER_SUBJECTS:-tool.a.events tool.b.events tool.c.events tool.d.events}
    command:
      - sh
      - -c
      - |
        until nats -s "$$NATS_URL" stream info TOOL_EVENTS >/dev/null 2>&1; do
          nats -s "$$NATS_URL" stream add TOOL_EVENTS --subjects 'tool.>' --defaults >/dev/null 2>&1 || sleep 1
        done
        echo "stream TOOL_EVENTS ready; publishing to: $$SUBJECTS"
        i=0
        while true; do
          i=$$((i+1))
          for s in $$SUBJECTS; do
            nats -s "$$NATS_URL" pub "$$s" "event-$$i" >/dev/null 2>&1 || true
          done
          sleep 1
        done

  listener-runtime:                   # 我們的 ListenerCell
    build: ./runtime
    ports:
      - "8080:8080"
    volumes:
      - ./config:/config
    environment:
      NATS_URL: nats://upstream-nats:4222
      SESSIONS_FILE: /config/sessions.yaml
      PROCESS_DELAY_MS: "200"
    # 刻意不設 depends_on：runtime 先起 → 短暫 DEGRADED → 自動收斂 ACTIVE（spec §4.3）
```

`config/sessions.yaml`（初始宣告；tool-d 留給場景 5 onboarding）：

```yaml
sessions:
  tool-a:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.a.events
      durable: listener-tool-a
  tool-b:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.b.events
      durable: listener-tool-b
      retry:                # 固定間隔，場景 3 的 retry 計數肉眼可讀
        interval: 5s
        maxAttempts: 10
  tool-c:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.c.events
      durable: listener-tool-c
```

- [ ] **Step 3: 手動起 compose 驗證收斂**

```bash
docker compose up -d --build
sleep 20
curl -s localhost:8080/status | jq .
```

Expected: 三個 session 都 `observedState: "ACTIVE"`、`admittedCount` > 0、conditions 四個全 true。若 runtime 比 NATS 先起，中間會短暫看到 `DEGRADED`（這正是 spec §4.3 的預期行為）。

- [ ] **Step 4: 寫 smoke test**

`demo/smoke-test.sh`：

```sh
#!/bin/sh
# 端到端自檢（spec §9.3）：up → ACTIVE → 計數增加 → STANDBY → 計數停 → down
set -eu
cd "$(dirname "$0")/.."

command -v docker >/dev/null 2>&1 || { echo "FAIL: docker is required"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is required"; exit 1; }
[ -f config/sessions.yaml ] || { echo "FAIL: config/sessions.yaml is missing"; exit 1; }

BACKUP=$(mktemp)
cp config/sessions.yaml "$BACKUP"
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ -f "$BACKUP" ]; then
    cp "$BACKUP" config/sessions.yaml || status=1
    rm -f "$BACKUP"
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

state_of() {
  curl -sf http://localhost:8080/status | jq -r ".sessions[\"$1\"].observedState"
}

admitted_of() {
  curl -sf http://localhost:8080/status | jq -r ".sessions[\"$1\"].admittedCount"
}

wait_state() {
  session=$1
  expected=$2
  timeout=$3
  i=0
  while [ "$i" -lt "$timeout" ]; do
    [ "$(state_of "$session" 2>/dev/null || true)" = "$expected" ] && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $session -> $expected (now: $(state_of "$session" 2>/dev/null || echo unavailable))"
  exit 1
}

cat > config/sessions.yaml <<'EOF'
sessions:
  tool-a:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.a.events
      durable: smoke-tool-a
EOF

docker compose up -d --build
wait_state tool-a ACTIVE 90

c1=$(admitted_of tool-a)
sleep 5
c2=$(admitted_of tool-a)
[ "$c2" -gt "$c1" ] || { echo "FAIL: admittedCount not growing ($c1 -> $c2)"; exit 1; }
echo "OK: consuming ($c1 -> $c2)"

cat > config/sessions.yaml <<'EOF'
sessions:
  tool-a:
    desiredState: STANDBY
    configVersion: v1
    config:
      subject: tool.a.events
      durable: smoke-tool-a
EOF

wait_state tool-a STANDBY 30
sleep 2
c3=$(admitted_of tool-a)
sleep 4
c4=$(admitted_of tool-a)
[ "$c4" -eq "$c3" ] || { echo "FAIL: still consuming in STANDBY ($c3 -> $c4)"; exit 1; }
echo "OK: STANDBY stops consumption ($c3 = $c4)"

echo "SMOKE TEST PASS"
```

然後：`chmod +x demo/smoke-test.sh`

- [ ] **Step 5: 跑 smoke test 確認 pass**

Run: `demo/smoke-test.sh`
Expected: 依序印出 `OK: consuming (...)`、`OK: STANDBY stops consumption (...)`、`SMOKE TEST PASS`，exit 0，且結束後 `config/sessions.yaml` 內容復原、compose 已 down。

- [ ] **Step 6: Commit**

```bash
git add runtime/Dockerfile docker-compose.yml config demo/smoke-test.sh
git commit -m "compose: upstream-nats/publisher + listener-runtime, initial sessions.yaml, smoke test"
```

---

### Task 8: watch-status.sh + 五支 demo 腳本

**Files:**
- Create: `demo/watch-status.sh`
- Create: `demo/01-change-flow.sh`
- Create: `demo/02-degraded.sh`
- Create: `demo/03-isolation.sh`
- Create: `demo/04-replay.sh`
- Create: `demo/05-onboarding.sh`

**Interfaces:**
- Consumes: Task 6 的 `/status` JSON（含 `subject` 欄位）；Task 7 的 compose 服務名（`upstream-nats`、`upstream-publisher`）與 `PUBLISHER_SUBJECTS` 環境變數。
- Produces: 六支可執行 shell script。腳本原則（spec §8）：**只做上游側與觀察側動作；改 YAML 一律由演示者手動做**，腳本印出要改什麼並等 Enter。

- [ ] **Step 1: 寫 watch-status.sh**

`demo/watch-status.sh`（macOS 沒有 `watch`，內建 1s 自我輪詢；Ctrl-C 離開）：

```sh
#!/bin/sh
# /status → 人讀表格（spec §6）。單次模式：watch-status.sh -1
URL="${STATUS_URL:-http://localhost:8080/status}"

render() {
  curl -sf "$URL" | jq -r '
    ((.cell.specError // empty) | "SPEC ERROR: " + .),
    (["SESSION","SUBJECT","DESIRED","OBSERVED","VER(d/a)","CONN","ADMIT","ADMITTED","PENDING","RETRY","REASON"] | @tsv),
    (.sessions | to_entries[] | .value as $v | [
       .key,
       ($v.subject // "-"),
       ($v.desiredState // "-"),
       $v.observedState,
       "\($v.declaredConfigVersion // "-")/\($v.appliedConfigVersion // "-")",
       (if $v.conditions.connectionReady then "ok" else "x" end),
       (if $v.conditions.admissionAllowed then "ok" else "x" end),
       ($v.admittedCount | tostring),
       ($v.pendingCount | tostring),
       ($v.retryAttempt | tostring),
       (if $v.reason == "" then "-" else $v.reason end)
     ] | @tsv)' | column -t -s "$(printf '\t')" \
    || echo "runtime unreachable: $URL"
}

if [ "$1" = "-1" ]; then render; exit 0; fi
while true; do
  clear
  date "+%H:%M:%S  ($URL)"
  render
  sleep 1
done
```

- [ ] **Step 2: 寫五支場景腳本**

`demo/01-change-flow.sh`（場景 1：上游變更流程，guidance 6.3 / spec §8#1）：

```sh
#!/bin/sh
# 場景 1：上游 subject 換版，全程不重啟 runtime（P1）
cd "$(dirname "$0")/.."
pause() { printf '\n>>> %s\n(Enter 繼續) ' "$1"; read -r _; }

echo "前置：docker compose up -d --build 已跑、tool-a ACTIVE。"
echo "另開一個終端跑觀察視窗：demo/watch-status.sh"

pause "步驟 1（手動改 YAML）：config/sessions.yaml 把 tool-a 的 desiredState 改成 STANDBY。
    觀察：DRAINING（約 2 秒，in-flight 收尾）→ STANDBY，ADMIT 變 x，PENDING 開始累積"

pause "步驟 2（腳本做上游側）：把 publisher 的 tool-a 主題切到 tool.a.events.v2 並重建 publisher（stream tool.> 不動）"
PUBLISHER_SUBJECTS="tool.a.events.v2 tool.b.events tool.c.events tool.d.events" \
  docker compose up -d upstream-publisher
echo "publisher 已改發 tool.a.events.v2"

pause "步驟 3（手動改 YAML）：tool-a 的 subject 改 tool.a.events.v2、configVersion 改 v2、desiredState 改回 RUNNING。
    觀察：CONNECTING → ACTIVE，VER 變 v2/v2，ADMITTED 繼續增加。
    不漏訊論證（spec §4.3）：舊 subject 殘餘訊息先補完（durable 游標），新 subject 從頭收"

echo ""
echo "場景 1 完成：runtime 容器全程未重啟（docker compose ps 可驗證 listener-runtime 的 Up 時間）。"
```

`demo/02-degraded.sh`（場景 2：DEGRADED 不 crash，P2 / spec §8#2）：

```sh
#!/bin/sh
# 場景 2：NATS 整個消失 → DEGRADED 自動重試、process 存活 → 回來自動收斂
cd "$(dirname "$0")/.."
pause() { printf '\n>>> %s\n(Enter 繼續) ' "$1"; read -r _; }

pause "我將 docker stop upstream-nats。
    觀察：RUNNING 的 session 全進 DEGRADED（reason MESSAGING_ENDPOINT_UNREACHABLE）、RETRY 計數爬升；
    runtime 容器保持 Up —— 錯誤是狀態，不是 crash。
    注意：預設 maxAttempts=10 + 指數退避 ≈ 3 分鐘後升級 FAILED，所以本場景 3 分鐘內要復原（升級本身也是特性，場景 3 演）"
docker stop $(docker compose ps -q upstream-nats)
echo "upstream-nats 已停。看 watch-status 視窗與：docker compose ps listener-runtime"

pause "我將 docker start upstream-nats —— 觀察全部自動回 ACTIVE，無任何人工介入、無重啟"
docker start $(docker compose ps -aq upstream-nats)
echo ""
echo "場景 2 完成：連線故障全程狀態化（DEGRADED → ACTIVE），process 從未退出。"
```

`demo/03-isolation.sh`（場景 3：per-session 隔離 + retry 升級，P3 / spec §8#3）：

```sh
#!/bin/sh
# 場景 3：tool-b 指向壞 subject → DEGRADED 重試 → FAILED；tool-a/c 全程 ACTIVE
cd "$(dirname "$0")/.."
pause() { printf '\n>>> %s\n(Enter 繼續) ' "$1"; read -r _; }

echo "說明：壞 subject 要用 stream 範圍(tool.>)之外的，例如 bad.b.events ——"
echo "consumer filter 不在 stream subjects 內，NATS 拒絕建 consumer → RESOURCE_NOT_FOUND。"
echo "（tool.b.nothing 這種在 tool.> 內的 subject 是合法 filter，只會收不到訊息，不會錯）"

pause "步驟 1（手動改 YAML）：tool-b 的 subject 改成 bad.b.events、configVersion 改 v2。
    觀察：tool-b DRAINING → CONNECTING → DEGRADED（reason RESOURCE_NOT_FOUND），
    每 5s 重試（tool-b 配了 interval: 5s）、RETRY 1→10，約 50 秒後升級 FAILED；
    全程 tool-a / tool-c 保持 ACTIVE、ADMITTED 持續增加 —— 單一 Tool 故障不外溢"

pause "步驟 2（手動改 YAML）：把 tool-b 的 subject 改回 tool.b.events、configVersion 改 v3。
    觀察：FAILED 收到宣告變更 → 重置收斂 → ACTIVE（FAILED 不是死刑，是等待修正）"

echo ""
echo "場景 3 完成：錯誤分類（DEGRADED→FAILED 升級路徑）+ per-session 隔離，一場景三主張。"
```

`demo/04-replay.sh`（場景 4：STANDBY replay，guidance 9.3 Option A / spec §8#4）：

```sh
#!/bin/sh
# 場景 4：STANDBY 期間欠帳累積（server 端 pending），恢復 RUNNING 自動補齊
cd "$(dirname "$0")/.."
pause() { printf '\n>>> %s\n(Enter 繼續) ' "$1"; read -r _; }

pause "步驟 1（手動改 YAML）：tool-a 的 desiredState 改 STANDBY。
    觀察：PENDING 每秒 +1（publisher 沒停，訊息留在 stream；游標在 server 端）"

pause "步驟 2：等 PENDING 累積到 ~30，再手動把 desiredState 改回 RUNNING。
    觀察：ACTIVE 後 PENDING 快速歸零、ADMITTED 補齊 —— durable consumer 從上次 ack 位置續讀，
    replay 是免費的（不需要任何補償邏輯）"

echo ""
echo "場景 4 完成：STANDBY 擋流不丟訊（Option A Replay）。"
```

`demo/05-onboarding.sh`（場景 5：Tool onboarding / spec §8#5）：

```sh
#!/bin/sh
# 場景 5：新 Tool 上線 = 貼一段 config，別無其他
cd "$(dirname "$0")/.."
pause() { printf '\n>>> %s\n(Enter 繼續) ' "$1"; read -r _; }

echo "publisher 從一開始就在發 tool.d.events（歷史都在 stream 裡），只是還沒有人聽。"
echo ""
echo "把下面這段貼進 config/sessions.yaml 的 sessions: 底下："
echo ""
cat <<'YAML'
  tool-d:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.d.events
      durable: listener-tool-d
YAML

pause "貼好存檔後觀察：tool-d 出現 → CONNECTING → ACTIVE，
    且 ADMITTED 從 0 快速衝到全部歷史筆數（DeliverPolicy=All：新 consumer 吃整段 backlog）。
    onboarding = 加一段宣告；沒有部署、沒有重啟、沒有 topology 變更"

pause "（選演）offboarding 對照：把 tool-d 整段刪掉 → DRAINING → STOPPED → session 消失、
    server 端 durable consumer 一併刪除（永久退場，破壞性）；只想暫停該用 desiredState: STOPPED"

echo ""
echo "場景 5 完成：topology coupling 解除。"
```

- [ ] **Step 3: chmod + 手動走一遍場景 5（最快驗證端到端）**

```bash
chmod +x demo/*.sh
docker compose up -d --build
demo/watch-status.sh -1        # 應看到 tool-a/b/c 表格
demo/05-onboarding.sh          # 照提示貼 tool-d，確認 ACTIVE + backlog 補齊
```

Expected: `watch-status.sh -1` 印出對齊表格；tool-d 貼入後 ≤3 秒出現在表格並收斂 ACTIVE，ADMITTED 一次跳到歷史總量。結束後把 tool-d 從 YAML 移除（觀察 offboarding），`docker compose down`。

- [ ] **Step 4: Commit**

```bash
git add demo
git commit -m "demo: watch-status table + five scenario scripts (change-flow, degraded, isolation, replay, onboarding)"
```

---

## 完成定義

全部 task 打勾後：`demo/smoke-test.sh` 綠燈、五支 demo 腳本可照 spec §8 演完、`gradle test` 38 tests 全過、git log 乾淨（每 task 至少一個 commit、無 Co-Authored-By、未 push）。
