# ListenerSession NATS Prototype — Technical Design

**Status:** Approved draft（brainstorming 定案版）
**Date:** 2026-07-13
**基於:** [Listener Lifecycle and Message Admission — Architecture Guidance v1.1](../../listener-lifecycle-message-admission-architecture-guidance-v1.1.md)

> **⚠️ SUPERSEDED IN PART by [ADR-0001](../adr/0001-single-session-pod-runtime.md) and the
> [single-session pod refactor plan](../plans/2026-07-13-single-session-pod-refactor.md) (2026-07-13).**
> 本文件描述的是**已執行完成的 prototype 建置階段**：一個 Java process 內以多個 `ListenerSession`
> + per-session virtual thread 模擬隔離（§3）、YAML 新增/刪除 entry 觸發 in-process
> onboard/offboard（§4.3、§5.2、§8 場景 5）、subject 換版在 filter 更新被拒時 fallback
> 為 consumer delete+recreate（§4.3）。這些是 ADR-0001 決定**不採用**的 initial production
> model：production 是「一個 process = 一個 `ListenerSession`」，由 Kubernetes（非 virtual
> thread）提供隔離邊界，且直到 finalizer 協定存在前，`runtime/src/main` 不含任何 consumer 刪除
> 路徑（refactor Task 2, commit `01d1619`）。逐節 superseded 標記見下方對應章節；未標記的章節
> （狀態機轉移表、錯誤分類、drain/admission 語意、Reconciler 事件驅動模型、`/status` 格式、
> 命名決策）在單 session 語意下仍然有效，已由 refactor T1–T6 落地。**§1.1** 記錄本 prototype
> 目前實際證明與刻意尚未證明的範圍。

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
- ListenerCell multi-replica coordination——prototype 明確採 single replica；consumer ownership、
  leader election、跨 Pod coordinated drain 與 status aggregation 留待 production design。

### 1.1 Prototype 驗收：現在證明了什麼／刻意尚未證明什麼

（呼應 ADR-0001 acceptance gates 與 refactor plan「Explicit follow-up project」。）

**已證明（refactor T1–T6，77+ Java tests + 五個真跑 demo 場景 + `demo/consumer-safety-test.sh`）：**

- 一個 process 只擁有一個 `ListenerSession`（`SESSION_NAME` 決定唯一選中宣告，其餘 entry 被忽略）；
- 改動 tool-a 的 dynamic 欄位（subject/filter、`configVersion`、retry、drainTimeout）不重啟 process、
  不影響 tool-b/c；
- subject/filter 換版是同一 durable 的 in-place 更新，creation timestamp 與游標不重置，全程無
  consumer 刪除／重建；
- 選中宣告的 durable 一經 latch 即不可變：之後的 durable 變更 fail closed 為 `FAILED/INVALID_SPEC`，
  不建立第二個 consumer；
- 選中宣告消失（或整份 YAML 損毀）不會刪除 consumer、不會終止 process；
- NATS 斷線／恢復不殺死 session 執行緒（DEGRADED 有界重試後自動回 ACTIVE）；
- 個別 Compose 服務（Pod 的類比）崩潰或重啟不影響其他服務的處理與計數；
- `runtime/src/main` 原始碼層級檢查證明不含任何 `deleteConsumer` API 或 NATS consumer delete 呼叫；
- 以 Compose profile（`--profile onboarding`）明確啟動 `listener-tool-d` 代表「新增一個
  controller-generated workload」，而非在既有 process 內動態生出新 session。

**刻意尚未證明（留給 ADR-0001「Explicit follow-up project」的獨立 controller 技術設計）：**

- 沒有 `ToolListener` CRD／controller／admission webhook；`sessions.yaml` 仍是暫時相容輸入，不是
  正式 API 資源；
- 沒有 Kubernetes Lease 或任何 fencing：沒有兩個 runtime 競爭同一 durable 的防護，靠操作紀律
  （不 force-delete Pod）代替；
- 沒有 finalizer-driven 退場 handshake：目前 config 消失＝fail closed 停止收訊息，**不是**
  offboarding；authorized 的 consumer 刪除路徑要等 finalizer 協定存在才重新開放；
- 沒有 UID 派生的持久身分：durable 名稱仍是操作者在 YAML 中配置、由 process 生命週期 latch 的
  相容輸入，不是由 controller 依 `ToolListener` UID 決定的不可變身分；
- 沒有 production 規模的 workload generation：Compose 的四個服務手寫定義同一份 template 差異，
  不是由 controller 從宣告動態產生 Deployment。

延伸細節與比較依據見並行進行中的 controller technical design 討論（`docs/notes/`、`docs/handoffs/`
的 2026-07-14 產出）；本 prototype 不預先假設其結論。

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

> **Superseded by ADR-0001：**「一個 Cell Runtime、3+1 個 session」是**已執行完成**的 prototype
> 建置目標，不是 production 目標。Production／目前已落地的 refactor 結果是「一個 process、一個
> `ListenerSession`」，用四個 Compose 服務（tool-a/b/c 預設 + tool-d 走 `onboarding` profile）
> 呈現多個 controller-generated workload，而不是一個 process 內的多個 session。

---

## 3. 系統架構

> **Superseded by ADR-0001 §1、§5：** 下方架構圖與隔離原則描述的是 prototype 建置階段的
> 「一個 `listener-runtime` process 內跑多個 `ListenerSession`」拓樸，僅供歷史對照。Production
> 拓樸是「一個 process = 一個 `ListenerSession`」，由 Kubernetes（Compose 中以獨立服務類比）在
> process/Pod 邊界提供隔離；virtual thread 僅是 blocking I/O 的實作手段，**不是**隔離邊界（見本節
> 「隔離原則」段落下方標記）。

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

控制流：sessions.yaml（穩定內容 polling）→ Reconciler → per-session mailbox
        → SessionStateMachine → connect / fetch / drain / admission 動作
        → observedState + conditions → /status
```

隔離原則：**每個 session 一條獨立 NATS Connection + 一個 virtual thread**，跨 session 零共享可變狀態。隔離由 session 邊界提供，不是由容器提供 —— 這是 Phase 3 的核心論證。

> **Superseded by ADR-0001：**「隔離由 session 邊界提供」是 prototype 階段刻意驗證、後來被推翻的
> 論證。ADR-0001 的決定是相反方向——**Kubernetes（process/Pod）才是隔離邊界**，virtual thread
> 只保留作為 blocking I/O 的實作便利，不再是容錯邊界或必要架構原語（ADR-0001 §1）。目前
> `ListenerSession` 的執行緒仍是 named virtual thread，但註解已不再宣稱它提供
> isolation（refactor Task 4）。

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

原則：**所有情境一律通過狀態機**——任何錯誤、任何操作都轉譯成狀態機事件，不存在繞過 lifecycle 的側路（guidance 8.1 的 single authoritative lifecycle）。Runtime 絕不因 session 層級錯誤 exit：

```
連不上 / timeout / connection lost     → DEGRADED（自動重試）
stream/subject/consumer 不存在         → DEGRADED（無法與「上游尚未 ready」區分，
                                          一律先假設可恢復，靠重試升級把關）
該 session 的 spec 無效（缺欄位、
  值不合法）                           → FAILED（重試無意義，等宣告修正）
DEGRADED 重試超過 maxAttempts          → FAILED（升級，停止無效重試）
DRAINING 超過 drainTimeout             → FAILED（guidance 8.3：drain timeout）
FAILED 後宣告有任何變更                → 重置回 STANDBY 重新收斂
```

「資源不存在」不直接 FAILED 的理由：NATS 的 stream not found 無法分辨是操作者打錯字（永久）還是上游尚未建好（暫時，guidance 3.3 列為 temporary dependency failure）。分類器不猜，統一走 DEGRADED → maxAttempts 用盡 → FAILED 的升級路徑。

（整份 YAML 解析失敗屬 Reconciler 層級，不影響任何 session state，見 §5.2。）

Retry 預設指數退避 1s→2s→…→cap 30s；per-session 可用 `retry.interval` 覆寫為固定間隔（見 §5.1）。

**重連主導權：狀態機全權主導（jnats auto-reconnect 關閉，`maxReconnects(0)`）。**

理由：重試有兩個域——(1) transport 斷線（client 內建重連只覆蓋這個）、(2) 資源/設定類失敗如 stream 不存在（client 重連不管，應用層必須自己 retry，即上方 DEGRADED loop）。既然域 2 的 loop 躲不掉，讓它同時吃下域 1，全系統只有一條重試路徑、一套參數（`retry.*`）、`/status` 完整可見每次嘗試。重連實作 = close + 重走與初次 CONNECTING 完全相同的路徑；durable pull consumer 的游標在 server 端，client 端無狀態要保護，故不損失 auto-reconnect 的可靠性（其強項——publish buffer、push 訂閱重建——本設計用不到；cluster failover 由 `connect()` 吃 server list 保留）。

此為刻意偏離 client 預設慣例。兩個註記：
- **Kafka 對照**：Kafka client 的重連關不掉，屆時域 1 執行者換成 client、狀態機從 `poll()` 例外推導 DEGRADED——架構不變。
- **Revisit point**：若 production 改用 push consumer 或 session 需要發佈訊息，重新評估交回 jnats auto-reconnect 的混合式。

**轉移中收到 spec 變更的規則**：完成當前轉移後再重新收斂（如 DRAINING 一半改 desiredState，先把 drain 走完）。唯一例外：DEGRADED 的重試等待被 spec 變更**立即中斷**，直接重新評估——改好設定不應乾等 interval 走完。

### 4.3 Stream / Consumer 擁有權（guidance 10.5）

照擁有權切分建立責任：

```
Stream（TOOL_EVENTS, subjects: tool.>）＝ 上游資產
  upstream-publisher 啟動時冪等建立（demo 中代表上游 provisioning）。
  Runtime 永不建 stream，只讀；CONNECTING 發現 stream 不在 → DEGRADED 等
  （即「上游尚未 ready」，用 §4.2 分類）。

Durable consumer（listener-tool-a…）＝ 我們的資產
  本質是「我們讀到哪」的游標，由讀的人擁有。
  Runtime 於 CONNECTING 以 createOrUpdateConsumer 冪等建立/更新。
  DeliverPolicy = All（預設）；subject 換版時若 server 拒絕 filter 更新，
  fallback = delete + recreate——stream 是 source of truth、游標只是讀位置，
  兩條路對新 subject 的訊息集合等價，切換窗口不漏訊。
```

> **Superseded by ADR-0001 + refactor Task 2（commit `01d1619`）：** 「fallback = delete +
> recreate」已被移除，不是保留手段。`JnatsLink.connect()` 不再有 `JetStreamApiException`
> 廣泛 delete-and-recreate fallback；subject/filter 換版一律是同一 durable 的 in-place 更新，
> 若 server 拒絕更新則分類為 `RESOURCE_NOT_FOUND` 進 `DEGRADED` 有界重試，重試用盡才
> `FAILED/RETRY_EXHAUSTED`——兩種結果都**不**刪除或重建 consumer。`demo/consumer-safety-test.sh`
> 對此有真 NATS acceptance 證據（creation timestamp、游標不重置）。

推論：compose up 不需要 `depends_on` 編排開機順序——runtime 先起就短暫 DEGRADED，stream 建好後自動收斂 ACTIVE，reconcile 本身處理啟動競態。

**退場時的 consumer 處置**（與 onboarding 對稱，prototype 階段設計，已被下方標記取代）：

```
desiredState: STOPPED → 保留 durable consumer（游標是承諾：enable 後從斷點續讀）
YAML 刪除 entry       → DRAINING → STOPPED → 刪除 durable consumer → 移除 session
                        （offboarding = server 端資產清乾淨，避免無人讀的 consumer
                          在 stream 上永久累積 pending）
```

> **Superseded by ADR-0001 + refactor Task 2：** 「YAML 刪除 entry → 刪除 durable consumer」是
> ADR-0001 明確禁止的路徑——config 消失不是 offboarding。目前選中宣告消失（或整份 YAML 失效）
> 使 session 進入 `FAILED/INVALID_SPEC`：停止收訊息、**保留** durable consumer、process 不終止；
> 恢復宣告即從同一 session instance 復原，無 stale in-flight handle 被 ack。只有 ADR-0001
> §2「Offboarding protocol」定義、尚未實作的 finalizer 驅動 termination handshake（controller
> 送出含 UID/operation ID 的 termination request、runtime drain 後刪除、controller 收到精確
> ack 才移除 workload/finalizer）未來才重新授權刪除，且直到那時 `runtime/src/main` 不含任何
> consumer 刪除路徑（refactor Task 2 已加入原始碼層級檢查）。

操作語意要教育：**暫停用 STOPPED；刪 entry = 永久退場（破壞性）**——游標刪除後同名 tool 重新 onboard 視同全新 session，從 DeliverPolicy 起點重讀。（此段「刪 entry = 永久退場」敘述同樣屬 prototype 階段設計，已被上方標記取代：目前刪 entry 只是 fail closed，不刪游標。）

### 4.4 Drain 語意

- **in-flight ≝ 已 fetch 未 ack 的訊息**。
- DRAINING = 停止新 fetch，把已 fetch 的處理完並 ack（不 nak——這些訊息已被 admission 放行，退回只會製造 redelivery 重複）。
- fetch batch = 10；pipeline stub 每則訊息帶人工處理延遲（env 可調，預設 200ms），讓 DRAINING 有 1–2 秒可觀察窗口，ACTIVE 計數增速肉眼可讀。

> **Drain 語意澄清（refactor Task 1 review）：** 上面「in-flight ≝ 已 fetch 未 ack 的訊息」是
> 精確定義，容易被誤讀為「切換前把舊 subject 的所有 server 端 backlog 處理完」，特此明文排除：
> DRAINING 是 best-effort 的**記憶體內批次 drain**——只處理已經 fetch 進記憶體、尚未 ack 的那一批
> （batch 上限 10），**絕不重新 fetch** server 端剩餘 backlog。DRAINING 完成的訊號是這一批清空或
> `drainTimeout` 到，不是 `numPending` 歸零。凡本文件其餘章節或 `docs/plans/2026-07-13-listener-session-nats-prototype.md`
> 中出現「先把舊 subject 的工作處理完再切換」之類措辭，均以本段落為準。

### 4.5 訊息語意

At-least-once：訊息經 AdmissionGate 進 pipeline stub（計數）**之後才 ack**。STANDBY/DRAINING 期間不 fetch，訊息留在 stream；恢復 ACTIVE 後 durable consumer 從上次 ack 位置續讀 —— guidance 9.3 **Option A（Replay）** 由此免費取得。Pipeline stub 為冪等計數，redelivery 在 prototype 中僅表現為計數重複，可接受並在 demo 中說明。

### 4.6 Conditions

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
檔案變更（平台中立的 500ms 內容輪詢；連續兩次讀到相同內容才觸發 reconcile，
  並把該份已驗證 snapshot 直接交給 Reconciler，穩定後 ≤1s 套用，避免把
  編輯器 truncate/rewrite 的中間態當成正式宣告，也避免 callback 二次讀檔的 TOCTOU；
  啟動時的第一份穩定內容同樣會投遞，不把它靜默當成 baseline）
  → 解析 + 驗證整份 YAML
      ├─ 失敗 → 保留上一份有效宣告繼續運作；/status 顯示 specError
      │         （絕不 crash、絕不套用半份設定）
      └─ 成功 → diff 現有 sessions：
            新增 entry → 建 session，自 STANDBY 起步
            變更 entry → 投遞新 spec 至該 session mailbox
            刪除 entry → 該 session DRAINING → STOPPED → 移除
```

> **Superseded by ADR-0001 + refactor Task 3/4（`SingleSessionReconciler`，commit `acbbc63`/`3ce7908`）：**
> 上面「新增 entry → 建 session」「刪除 entry → 移除」是多 session `Reconciler` 的 diff 語意，已被
> production 路徑取代。`SingleSessionReconciler` 只投影 `SESSION_NAME` 選中的那一個 entry：其他
> entry 的新增/變更/刪除完全被忽略（不建立新 session、不觸發任何動作）；選中 entry 消失或失效
> 一律變成 `FAILED/INVALID_SPEC`，不移除、不刪除 consumer；恢復宣告會復原**同一個**已存在的
> `ListenerSession` instance，不會建立新 instance。舊的多 session `Reconciler` 已於 refactor
> Task 4 刪除（`runtime/src/main/java/dc/listener/reconcile/Reconciler.java` 不存在於目前程式碼）。

Reconciler 不直接操作 connection；spec 經 per-session mailbox 單向投遞，由狀態機在自己的 thread 收斂。一個 session 卡住不阻塞其他 session 的 reconcile（P3）。（此段描述的多 session reconcile 模型同樣是 prototype 階段設計，見上方標記。）

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
      "pendingCount": 0,
      "retryAttempt": 0
    }
  }
}
```

`pendingCount` 取自 consumer info 的 `numPending`（server 端 backlog）——場景 4 靠它把「STANDBY 期間欠帳累積、恢復後歸零補齊」變成看得見的數字。

**Demo 觀察工具 `demo/watch-status.sh`**：curl `/status` + jq 重排成人讀表格，所有場景共用（搭 `watch -n1`）：

```
SESSION  SUBJECT           DESIRED  OBSERVED   CONN  ADMIT  ADMITTED  PENDING  REASON
tool-a   tool.a.events.v2  RUNNING  ACTIVE     ok    ok     1234      0
tool-b   tool.b.events     RUNNING  DEGRADED   x     x      567       89       MESSAGING_ENDPOINT_UNREACHABLE (retry 3/10)
tool-c   tool.c.events     STANDBY  STANDBY    ok    x      890       45
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
│   ├── watch-status.sh         # /status → 人讀表格（各場景共用）
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
| 1 | 上游變更流程（guidance 6.3） | tool-a 設 `STANDBY` → 上游 publisher 切至 `.v2` → 以舊 subject 暫時 `RUNNING` 清空凍結 backlog → 再次 `STANDBY` → YAML 換 subject + configVersion v2 並設 `RUNNING` → 補齊新 subject backlog；全程 stream `tool.>` 不動 | 全程不重啟 runtime，且不因 durable filter delete/recreate 遺留舊 subject 訊息（P1、S1-S3） |
| 2 | DEGRADED 不 crash | `docker stop upstream-nats` → 全部 session 進 `DEGRADED` 按 retry policy 重試、process 存活 → `docker start` → 自動回 `ACTIVE` | P2、S4 |
| 3 | Per-session 隔離 | 把 tool-b 的 subject 改成不存在的 → tool-b 進 `DEGRADED` 按 interval 重試、maxAttempts 用盡升級 `FAILED`；全程 tool-a/c 保持 `ACTIVE` 計數持續增加 | P3 隔離 + 錯誤分類 + retry 升級，一場景三主張 |
| 4 | STANDBY replay | tool-a `STANDBY` 期間 publisher 持續發訊 → 恢復 `RUNNING` → `admittedCount` 補齊 backlog | guidance 9.3 Option A |
| 5 | Tool onboarding | YAML 貼入 tool-d entry → 新 session 自動 `STANDBY→CONNECTING→ACTIVE` | Topology coupling 解除：onboarding = 加一段 config |

> **Superseded by ADR-0001 + refactor Task 2/5/6：**
>
> - **場景 1** 上方「delete/recreate 遺留舊 subject 訊息」的疑慮已隨 §4.3 標記的 fallback 移除
>   而不再適用——目前的 subject/filter 換版是純 in-place 更新，沒有 delete/recreate 路徑，也就
>   沒有因它產生的訊息遺留風險；不需要「先把舊 subject 手動清到 0 pending」的操作步驟。目前的
>   權威版本是 `demo/01-change-flow.sh`，以真跑 Compose 場景 1 驗證。
> - **場景 5** 「YAML 貼入 tool-d entry → 新 session 自動 onboard」是 prototype 階段驗證的
>   config-only in-process onboarding，ADR-0001 明確不採用（onboarding 必須是可觀測的
>   workload 操作，而不是隱藏在既有 process 內）。目前的 onboarding 場景是：把 tool-d 宣告加進
>   `sessions.yaml` 本身**不會**啟動 port 8084（因為沒有任何 process 選中它）；必須明確執行
>   `docker compose --profile onboarding up -d listener-tool-d`（模擬 controller 生成新
>   workload）才會出現 tool-d，且過程中 a/b/c 的身分不受影響。目前的權威版本是
>   `demo/05-onboarding.sh`（refactor Task 6，commit `5b3b9f8`），含明確 cleanup。

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

> **Update per ADR-0001：** 上一列寫作時「短期」帶有暫定語氣；ADR-0001 已把「一 Pod 一 session」
> 訂為 initial production model（不是過渡態），refactor T1–T6 已在 `runtime/`、`docker-compose.yml`
> 落地並通過獨立 review。「Bounded Cell：一 Pod 多 session」保留為 ADR-0001「Revisit criteria」
> 章節列出的**未來重新評估選項**（需先量測 Pod/JVM overhead 是否成為主要瓶頸），不是目前路線圖的
> 一部分。

CRD vs「管理服務 + DB」的取捨屬正式 Technical Design 的開放決策（guidance 10.1），prototype 不預作選擇；本設計的 Runtime 側模式（watch + reconcile + status 回報）對兩者皆成立。ADR-0001 已傾向
`ToolListener` CRD + controller 方向；並行進行中的 controller technical design（`docs/notes/`、
`docs/handoffs/` 的 2026-07-14 產出）進一步比較 GitOps+Template 與 CRD+Controller 兩案，本文件
不預先假設其結論。

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
| 10.7 隔離 | per-session Connection/thread/mailbox/retry；零共享可變狀態（prototype 階段答案，見下方標記） |

> **Superseded by ADR-0001 §5：** 10.7 的答案是 prototype 階段用 session 邊界模擬隔離時給的回答。
> Production 的隔離邊界是 Kubernetes（process/Pod），不是 session/thread；per-session
> Connection/mailbox/retry 仍然存在（單一 process 內仍只有一個 session，一樣零共享可變狀態），
> 但它們不再是回答「10.7 隔離」這個問題的機制——Compose 中的獨立服務／未來的獨立 Pod 才是。

其餘問題（credential 管理、Cell 容量切分、遷移步驟…）超出 prototype 範圍，留給正式 Technical Design。

---

## 13. 實作順序（供 implementation plan 參考）

1. 狀態機 + 單元測試（不碰 NATS，最快建立核心正確性）
2. NatsLink（connect/consumer/fetch/ack）+ AdmissionGate + pipeline stub
3. Reconciler + FileWatcher + spec 解析
4. StatusServer
5. docker-compose + publisher + smoke test
6. 五支 demo 腳本 + 講稿
