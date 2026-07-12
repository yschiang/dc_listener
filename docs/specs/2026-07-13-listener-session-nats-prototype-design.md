# ListenerSession NATS Prototype — Technical Design

**Status:** Approved draft（brainstorming 定案版）
**Date:** 2026-07-13
**基於:** [Listener Lifecycle and Message Admission — Architecture Guidance v1.1](../../listener-lifecycle-message-admission-architecture-guidance-v1.1.md)

---

## 1. 目標與範圍

用一個可跑的 docker prototype 驗證並 demo guidance 的核心主張：

- **P1**：K8S 管部署、Configuration 定義期望、Listener Runtime 管運作 —— Runtime 容器起來後全程不重啟，所有操作透過改 config 完成。
- **P2**：一般錯誤不以 process crash 處理 —— 連線失敗進 `DEGRADED` 自動重試，設定錯誤進 `FAILED` 停止無效重試。
- **P3**：per-session 隔離 —— 一個 Runtime（ListenerCell）管多個 ListenerSession，單一 Tool 故障不影響其他 Tool。

範圍是 guidance **Phase 2 + Phase 3 精簡版**：一份 code，先用單 session 演完整 lifecycle，再用多 session 演隔離與 onboarding。

Prototype 同時是 guidance Section 10 所要求 Technical Design 在此範圍內的具體回答（見 §12）。

### Out of scope

- K8S 部署（CRD、operator、ConfigMap 實體）—— 只寫 mapping（§10）
- 真實 Data Pipeline —— 用 stub 取代
- Kafka 相容、效能測試、安全性（TLS/credential）
- STANDBY backlog 的 Fence/Discard policy（guidance 9.3 Option B/C）—— prototype 只做 Option A（Replay）

---

## 2. 定案摘要

| 決策點 | 定案 |
|---|---|
| Demo 範圍 | Phase 2 + 3 精簡版：一個 Cell Runtime、3+1 個 session |
| Desired 宣告投遞 | 本地 `sessions.yaml` volume mount + 檔案 watch（production ↔ ConfigMap，見 §10） |
| 觀察介面 | 唯讀 HTTP `GET /status`（JSON） |
| 語言/stack | Java 21、純 JDK（無 framework）、jnats、snakeyaml、內建 HttpServer |
| Consumer 模型 | JetStream **durable pull consumer**，per-session 專屬 Connection |
| 部署拓撲 | 一份 docker-compose，服務命名標示擁有權（`upstream-*` vs `listener-runtime`） |
| Demo 場景 | 五個：上游變更流程、DEGRADED 不 crash、per-session 隔離、STANDBY replay、Tool onboarding |

---

## 3. 系統架構

```
┌─ docker-compose ────────────────────────────────────────────┐
│                                                              │
│  upstream-nats        NATS 2.x + JetStream（上游擁有）        │
│                       stream: TOOL_EVENTS                    │
│                       subjects: tool.<id>.events             │
│                                                              │
│  upstream-publisher   每秒對各 tool subject 發訊息（上游擁有） │
│                                                              │
│  listener-runtime     Java 21 process = 1 ListenerCell（我們）│
│    volume: ./config/sessions.yaml                            │
│    port:   8080 /status                                      │
│    ├─ ListenerSession tool-a                                 │
│    ├─ ListenerSession tool-b                                 │
│    └─ ListenerSession tool-c   （tool-d 由 demo 中途 onboard）│
└──────────────────────────────────────────────────────────────┘
```

操作者只有兩個把手：

- 改 `./config/sessions.yaml` —— 下 desired 宣告（**唯一**的控制方式）
- `curl :8080/status` —— 看 observed state / conditions

資料流與控制流：

```
資料流：publisher → NATS stream → session 的 durable pull consumer
        → AdmissionGate → pipeline stub（log + per-session 計數）

控制流：sessions.yaml（WatchService）→ Reconciler → per-session mailbox
        → SessionStateMachine → connect / fetch / drain / admission 動作
        → observedState + conditions → /status
```

隔離原則：**每個 session 一條獨立 NATS Connection + 一個 virtual thread**，跨 session 零共享可變狀態。隔離由 session 邊界提供，不是由容器提供 —— 這是 Phase 3 的核心論證。

NATS server URL 等 Runtime 自身設定屬靜態 config，走環境變數（startup 讀一次）；只有 per-session 宣告放 `sessions.yaml`。

---

## 4. ListenerSession 狀態機

七個 observed state 照 guidance 8.3 全實作，轉移關係照其 state diagram，不新增公開 state（drain 收尾等 transient 行為藏在實作內）。

每個 session 內部四個件：

```
SessionSpec         ← 來自 YAML：desiredState + configVersion + config
SessionStateMachine ← 唯一權威的 state；所有動作由它決定
NatsLink            ← 專屬 Connection + JetStream durable pull consumer
AdmissionGate       ← 由 state 推導（guidance 8.1 約束：不是獨立狀態機）
```

### 4.1 State × 動作對應

選 pull consumer 的理由：consumption control 零成本 ——「不消費」=「不呼叫 fetch」。

| State | Connection | Consumption（fetch loop） | Admission | 進入條件 |
|---|---|---|---|---|
| `STANDBY` | 保留（預設 policy） | 停 | 擋 | 初始 / drain 完成 / FAILED 修復後 reset |
| `CONNECTING` | 建線 + 驗證 stream/consumer | 停 | 擋 | desired=RUNNING 且 config ready |
| `ACTIVE` | 已連 | fetch → pipeline stub → ack | 允許 | connection + consumer ready |
| `DRAINING` | 保留 | 停新 fetch，等 in-flight ack（drainTimeout） | 只出不進 | ACTIVE 中收到 STANDBY/STOPPED 或 config 變更 |
| `DEGRADED` | 退避重連 | 停 | 擋 | 連線類錯誤 |
| `FAILED` | 關閉 | 停 | 擋 | 設定類錯誤 / retry 用盡 |
| `STOPPED` | 關閉 | 停 | 擋 | desired=STOPPED / YAML 刪除 entry |

### 4.2 錯誤分類（P2）

同一層 catch 分流，Runtime 絕不因 session 層級錯誤 exit：

```
連不上 / timeout / connection lost     → DEGRADED（自動重試）
stream/subject 不存在 / 該 session 的
  spec 無效（缺欄位、值不合法）         → FAILED（重試無意義，等宣告修正）
DEGRADED 重試超過 maxAttempts          → FAILED（升級，停止無效重試）
DRAINING 超過 drainTimeout             → FAILED（guidance 8.3：drain timeout）
FAILED 後宣告有任何變更                → 重置回 STANDBY 重新收斂
```

（整份 YAML 解析失敗屬 Reconciler 層級，不影響任何 session state，見 §5.2。）

Retry 預設指數退避 1s→2s→…→cap 30s；per-session 可用 `retry.interval` 覆寫為固定間隔（見 §5.1）。

### 4.3 訊息語意

At-least-once：訊息經 AdmissionGate 進 pipeline stub（計數）**之後才 ack**。STANDBY/DRAINING 期間不 fetch，訊息留在 stream；恢復 ACTIVE 後 durable consumer 從上次 ack 位置續讀 —— guidance 9.3 **Option A（Replay）** 由此免費取得。Pipeline stub 為冪等計數，redelivery 在 prototype 中僅表現為計數重複，可接受並在 demo 中說明。

### 4.4 Conditions

`configurationReady / connectionReady / consumerReady / admissionAllowed` 四布林 + `reason` + `lastTransitionTime`，隨轉移更新，格式對齊 guidance 8.4。

---

## 5. Desired 宣告與 Reconciler

### 5.1 sessions.yaml 格式

```yaml
sessions:
  tool-a:
    desiredState: RUNNING        # RUNNING | STANDBY | STOPPED
    configVersion: v1            # 操作者自訂字串，變更即代表 config 換版
    config:
      subject: tool.a.events
      durable: listener-tool-a
      retry:                     # 可選
        interval: 5s             #   固定重試間隔（不填 = 指數退避 1s→30s）
        maxAttempts: 10          #   超過升級 FAILED（預設 10）
      drainTimeout: 30s          # 可選（預設 30s）
```

`retry.*` 與 `drainTimeout` 變更屬「熱生效」類，不觸發 drain/重連；`subject/durable/configVersion` 變更觸發 `DRAINING → CONNECTING` 換版流程。

### 5.2 Reconciler

單一事件驅動 loop：

```
檔案變更（WatchService + 200ms debounce，容忍編輯器「先刪再寫」）
  → 解析 + 驗證整份 YAML
      ├─ 失敗 → 保留上一份有效宣告繼續運作；/status 顯示 specError
      │         （絕不 crash、絕不套用半份設定）
      └─ 成功 → diff 現有 sessions：
            新增 entry → 建 session，自 STANDBY 起步
            變更 entry → 投遞新 spec 至該 session mailbox
            刪除 entry → 該 session DRAINING → STOPPED → 移除
```

Reconciler 不直接操作 connection；spec 經 per-session mailbox 單向投遞，由狀態機在自己的 thread 收斂。一個 session 卡住不阻塞其他 session 的 reconcile（P3）。

### 5.3 版本可觀察性

`/status` 每個 session 回報 `declaredConfigVersion`（YAML 宣告）與 `appliedConfigVersion`（實際生效）；兩者不等即代表收斂中或收斂失敗 —— demo「宣告 vs 現實」的最直觀指標。

---

## 6. /status Endpoint

`GET :8080/status` 回 JSON（唯讀，無其他 route）：

```json
{
  "cell": { "cellId": "cell-1", "specError": null },
  "sessions": {
    "tool-a": {
      "desiredState": "RUNNING",
      "observedState": "ACTIVE",
      "declaredConfigVersion": "v2",
      "appliedConfigVersion": "v2",
      "conditions": {
        "configurationReady": true,
        "connectionReady": true,
        "consumerReady": true,
        "admissionAllowed": true
      },
      "reason": "",
      "lastTransitionTime": "2026-07-13T08:00:00Z",
      "admittedCount": 1234,
      "retryAttempt": 0
    }
  }
}
```

---

## 7. Repo 結構

```
dc_listener/
├── docker-compose.yml          # upstream-nats / upstream-publisher / listener-runtime
├── config/sessions.yaml        # desired 宣告（volume mount，demo 現場編輯）
├── demo/
│   ├── 01-change-flow.sh
│   ├── 02-degraded.sh
│   ├── 03-isolation.sh
│   ├── 04-replay.sh
│   ├── 05-onboarding.sh
│   └── smoke-test.sh           # 端到端自檢（CI 可跑）
└── runtime/                    # Java 21 Gradle 專案
    ├── build.gradle            # 依賴：jnats、snakeyaml、junit
    └── src/main/java/.../
        ├── Main.java           # 組裝：env config、Reconciler、HttpServer
        ├── spec/               # SessionSpec、YAML 解析驗證
        ├── session/            # ListenerSession、SessionStateMachine、NatsLink、AdmissionGate
        ├── reconcile/          # FileWatcher、Reconciler
        └── status/             # StatusServer
```

邏輯（spec/session/reconcile）與殼（Main、StatusServer）分離，日後可低成本包成 Spring Boot（jnats code 不變，只換 HTTP 層與組裝）。

---

## 8. Demo 場景

腳本原則：腳本只做上游側與觀察側動作（docker 操作、curl、印對照說明）；**改 YAML 由演示者手動做** —— 「改一個 config 檔完成變更」是主角，不藏進腳本。

| # | 場景 | 步驟 | 驗證的主張 |
|---|---|---|---|
| 1 | 上游變更流程（guidance 6.3） | tool-a 設 `STANDBY` → 觀察 drain + admission 停 → 改 config v2 → 設 `RUNNING` → 觀察重連至 `ACTIVE` | 全程不重啟容器完成變更（P1、S1-S3） |
| 2 | DEGRADED 不 crash | `docker stop upstream-nats` → 全部 session 進 `DEGRADED` 按 retry policy 重試、process 存活 → `docker start` → 自動回 `ACTIVE` | P2、S4 |
| 3 | Per-session 隔離 | 把 tool-b 的 subject 改成不存在的 → tool-b 進 `FAILED`；tool-a/c 保持 `ACTIVE` 計數持續增加 | P3、Bounded Cell 雛形 |
| 4 | STANDBY replay | tool-a `STANDBY` 期間 publisher 持續發訊 → 恢復 `RUNNING` → `admittedCount` 補齊 backlog | guidance 9.3 Option A |
| 5 | Tool onboarding | YAML 貼入 tool-d entry → 新 session 自動 `STANDBY→CONNECTING→ACTIVE` | Topology coupling 解除：onboarding = 加一段 config |

---

## 9. 測試策略

1. **狀態機單元測試**（核心）：NatsLink 換假件，驗證轉移表 —— 每個 state × 每種事件（spec 變更、連線錯誤、設定錯誤、drain 完成/timeout、retry 用盡）→ 正確下一 state。
2. **Reconciler 測試**：餵 YAML 字串驗證 diff 三動作；解析失敗保留舊宣告。
3. **端到端 smoke test**（`demo/smoke-test.sh`）：compose up → 等 ACTIVE → 發訊 → 計數增加 → STANDBY → 計數停 → down。

不做：testcontainers、覆蓋率目標、效能測試。

---

## 10. Production Mapping

| Prototype | Production（短期） | Production（長期候選） |
|---|---|---|
| `sessions.yaml` volume mount + 檔案 watch | ConfigMap volume mount + Runtime watch 檔案（kubelet 同步延遲 ≤ ~1min；不可用 subPath/env） | `ListenerSession` CRD：spec=宣告、status=observed，Runtime 以 Informer watch，status 回寫 CRD |
| `/status` endpoint | 同左 + 對接監控 | `kubectl get listenersession` 直接可讀 |
| 手改 YAML | change workflow 產生 ConfigMap | change workflow `kubectl patch` spec |
| pipeline stub | 真實 Data Pipeline；admission gate 語意不變 | 同左 |
| 一個 runtime 容器 = Cell | 短期一 Pod 一 session（One Tool/One Deployment 照舊） | Bounded Cell：一 Pod 多 session，Cell 切分依 guidance 7.3 |

CRD vs「管理服務 + DB」的取捨屬正式 Technical Design 的開放決策（guidance 10.1），prototype 不預作選擇；本設計的 Runtime 側模式（watch + reconcile + status 回報）對兩者皆成立。

---

## 11. Naming Decisions

| 決策 | 理由 |
|---|---|
| 物件名 `ListenerSession`（不縮 `Session`、不叫 `Listener`） | 「Listener」既有語意 = 整個 process；單獨 "session" 誤導為短命物件；與 guidance 7.2 逐字對齊 |
| `observedState`（原 `observedPhase`，guidance 已升 v1.1 同步更名） | 團隊偏好 state 一詞；code 跟文件走，詞彙 source of truth 是 guidance，故連文件一併改，不允許兩邊漂移 |
| 只有兩個 state 欄位：`desiredState`（操作者寫）/ `observedState`（Runtime 寫） | 對應網管模型 adminStatus/operStatus；「收斂中」由兩者不等表達 + conditions 承載細節，不設第三欄位 |
| 對外欄位用全名，code 內部可簡稱 `state` | 對外詞彙嚴格、對內簡潔 |

註：`objects_and_states.png` 仍為 v1.0 字樣（observedPhase），重匯出時更新。

---

## 12. 對 guidance Section 10 的回答（prototype 範圍）

| Guidance 問題 | 本設計的回答 |
|---|---|
| 10.1 Configuration 存哪／誰更新 desired state | `sessions.yaml`（production: ConfigMap/CRD）；操作者或 change workflow 更新 |
| 10.1 誰執行 reconciliation | Listener Runtime 內建 Reconciler（檔案 watch + diff + per-session 收斂） |
| 10.1 observed 如何回報 | `/status` JSON：observedState + conditions + applied/declared version |
| 10.2 STANDBY 如何觸發／desired 存哪 | 改 YAML 的 `desiredState`；desired 即存於 YAML |
| 10.2 STANDBY/DRAINING/STOPPED 差異 | STANDBY=長駐擋流；DRAINING=轉移過程（清 in-flight）；STOPPED=session 停止但宣告保留 |
| 10.3 config 變更是否需要 restart | 否；換版走 DRAINING→CONNECTING，retry/drainTimeout 熱生效 |
| 10.4 錯誤分類／retry 控制 | §4.2；per-session retry policy，reconnect storm 由 per-session 退避天然抑制 |
| 10.5 Consumer 模型／STANDBY 訊息／redelivery | durable pull consumer；Option A Replay；ack-after-admission，stub 冪等 |
| 10.6 process exit 時機 | 僅 Runtime 自身無法安全繼續（如 OOM、佈署錯誤）；session 層級錯誤一律狀態化 |
| 10.7 隔離 | per-session Connection/thread/mailbox/retry；零共享可變狀態 |

其餘問題（credential 管理、Cell 容量切分、遷移步驟…）超出 prototype 範圍，留給正式 Technical Design。

---

## 13. 實作順序（供 implementation plan 參考）

1. 狀態機 + 單元測試（不碰 NATS，最快建立核心正確性）
2. NatsLink（connect/consumer/fetch/ack）+ AdmissionGate + pipeline stub
3. Reconciler + FileWatcher + spec 解析
4. StatusServer
5. docker-compose + publisher + smoke test
6. 五支 demo 腳本 + 講稿
