# Config Delivery 中繼版 + Demo Controller — Technical Design

**日期:** 2026-07-14
**狀態:** Draft（待 owner 審 + grill-me）
**前置文件:** ADR-0001（方向）、`docs/plans/2026-07-13-single-session-pod-refactor.md`
（parallel gate 授權 + controller 範圍清單）、`docs/notes/2026-07-14-gitops-vs-crd-controller.md`
（方案比較與決策記錄 §7/§8）、`docs/handoffs/2026-07-14-controller-design-handoff.md`
**Fixture:** `docs/specs/fixtures/toollistener-projection-v1.yaml`

---

## 0. 定位與範圍

依 note §7 決策（2026-07-14 與 owner 定案）：

- **中繼版（改良 GitOps，per-tool projection）＝實戰版**：production 實際運行模型，
  按 production 標準投資。
- **Controller ＝ demo/方向驗證版**：在 kind 走通 ADR-0001 終態，證明方向可行；
  不做 production 硬化。升級觸發條件：「dynamic 變更可歸類 routine change」假設獲
  變更管理方證實，或 offboard 頻率/人工風險升高到 runbook 不可承受。
- **Demo 驗證範圍**：ADR acceptance gate 4（finalizer 退場握手）+ gate 8（projection
  更新不動 Pod template hash）全做；gate 7 簡化（orphan 有記錄、GC 拒刪活宣告，
  無排程器）；gate 5（lease fencing）不做——設計存檔於 §4。
- **一份 spec、兩個 plan**：本文件涵蓋兩者共享的合約；實作拆「中繼版 plan」與
  「demo controller plan」各自推進（見 §4a）。
- **技術棧**：demo controller 用 Java 21 + java-operator-sdk（與 runtime 同語言、
  同 docker-gradle 建置）；寫入把關用 CEL ValidatingAdmissionPolicy（不架 webhook）。
- 本設計工作本身受 refactor plan parallel gate 約束：**只產出文件與 fixture**，
  不加 controller code、CRD manifest、K8s 資產到進行中的 refactor。

### 0a. 進化脈絡（詳見 note §8）

```text
最原本: 一大包 values.yaml render 出每 tool 的 deployment，config 內嵌
        —— 改任何東西 = re-render + deployment + 變更單
  (1) 已完成: config 外部化 + hot reload（本 repo refactor，A）
  (2) 本設計 phase 1: config 按 tool 分裝，還原 per-tool 交付粒度（中繼版，B）
  (3) 本設計 phase 2: desired state 升級為 CRD，controller 對帳（demo，C）
```

---

## 1. 共享合約（中繼版與 demo controller 共用）

### 1a. Per-tool projection 檔案

每 tool 一個 ConfigMap，data 內容 = 沿用現行 collection schema、**恰好一個 entry**，
外加現行 runtime 會忽略的 `projection:` metadata 塊（已讀碼證實：`SpecParser`
根層只讀 `sessions`，其他 key 忽略；中繼版 plan 需加釘住測試防回歸）：

```yaml
projection:
  schema: 1                    # 本 metadata 塊的版本
  owner: "<ToolListener UID | 中繼版 instanceId(ULID)>"
  generation: 4                # CR generation；中繼版可省略
  revision: "<git SHA | controller 寫入值>"
  operation: null              # 平時必為 null；退場時見 1d
sessions:
  tool-a:                      # 恰好一個 entry；schema 與現行 sessions.yaml 相同
    desiredState: RUNNING
    configVersion: "v42"
    config:
      subject: "tool.a.>"
      durable: "tool-a-durable"
      retry: { interval: 5s, maxAttempts: 10 }
      drainTimeout: 30s
```

**交付規則（三地雷的正面表述，兩版皆適用）：**

1. ConfigMap 名字穩定：`listener-<tool>`。禁止內容 hash 後綴
   （kustomize `configMapGenerator` 必須 `disableNameSuffixHash: true`）。
2. 整個 volume 掛載；**禁 subPath**（subPath 收不到更新）；禁 `immutable: true`。
3. Pod template 不得含 config checksum annotation；dynamic 變更不得以任何方式
   改動 Pod template hash。

同一份 schema 兩邊用：中繼版由 GitOps render 寫入，demo controller 由 reconciler
寫入；runtime 無感。

### 1b. 身分規則

| | 中繼版 | Demo controller |
|---|---|---|
| owner | `instanceId` = onboard 時產生的 ULID，CI 檢查不可變 | ToolListener UID（K8s 保證唯一/不可變） |
| durable | 沿用 operator 配置名；runtime latch 行為不變 | UID 派生：`tl-` + RFC 4648 base32（小寫、去 padding）(sha256(clusterId + "/" + stream + "/" + uid)) 取前 26 字元 |

UID 派生名固定長度 29、字元集 `[a-z2-7-]`（符合 NATS durable 限制）；cluster/stream
入 hash 防跨環境相撞。`clusterId` 為 controller 部署時注入的設定值（如 cluster 名；
demo 固定 `kind-demo`）。唯一性不單靠 hash：admission/controller 對帳為第二道（3e）。
同名重 onboard = 新 UID/ULID = 新 durable（ADR 規則）。

### 1c. `/status` 擴充（additive，不破壞現有 envelope）

session 物件新增：

```json
"observed": { "owner": "...", "generation": 4, "revision": "..." },
"cleanup":  { "operationId": "...", "state": "Complete|Blocked", "reason": "..." }
```

值 = 最後一次成功 parse 的 projection metadata / 退場處理結果。中繼版即可用
（dashboard 對 git SHA 驗收斂）；demo controller 靠它把觀測抄回
`ToolListener.status`。`cleanup` 僅在收過 termination operation 後出現。

### 1d. Termination request（runtime 重獲「被授權的」刪除路徑）

背景：refactor T2 已移除 runtime 一切 consumer 刪除路徑並加 source-level 檢查；
ADR 要求只有 finalizer 驅動的退場可刪。本合約定義那條唯一路徑：

- **投遞**：走同一條 projection 鏈。controller 把 `projection.operation` 從 `null`
  改為：

  ```yaml
  operation:
    id: "<terminationOperationId>"
    type: Terminate
    assets: { stream: TOOLS, durable: "tl-..." }
    policy: DeleteConsumer
  ```

- **Runtime 驗證後才動作**：`type == Terminate` 且 `assets.durable` == 當前 latch
  的 durable 且 `owner` == 當前 observed owner，三者齊備 → 停 fetch/admission →
  drain（沿用既有 drain 語意與 timeout）→ 刪 consumer → `/status` 回報
  `cleanup{operationId, Complete}`。驗證不符 → 不動作、回報 `cleanup{.., Blocked,
  reason}`。**`operation` 非 null 時優先於 `desiredState`**——退場流程一旦開始，
  desiredState 不再被解讀。
- **operationId**：controller 產生，對該 UID 唯一即可（實作建議 UID 前綴 + 遞增
  序號）；runtime 原樣回報，不解讀其結構。
- **安全邊界**：這是 runtime 唯一的刪除呼叫點。T2 的 source-level 檢查改寫為
  「NATS consumer 刪除呼叫僅存在於 termination operation 處理器一處」。
- **中繼版永不寫 `operation` 塊**——退場走 runbook（2d），此路徑在中繼版形同不存在。

---

## 2. 中繼版（實戰版）

### 2a. 資產與位置

新目錄 `deploy/interim/`（本 repo；refactor T7 落地後以獨立 plan 實作）：

- **Render：kustomize**（`kubectl apply -k` 內建、零新依賴）。base = 共用 Deployment
  template；每 tool 一個 overlay 只含身分差異（tool 名、durable、port）。
  `configMapGenerator` 設 `disableNameSuffixHash: true`。
- 每 tool 兩物件：`listener-<tool>` ConfigMap（§1a 格式）+ `listener-<tool>`
  Deployment。
- `kind-up.sh` / `kind-down.sh`：起 NATS + 三 tool，一人可跑。
- Pattern 設計成可直接對映回真實 GitOps repo 的 helm 結構（值與結構分離）。

### 2b. Workload 形狀

`replicas: 1`、`strategy: Recreate`、`SESSION_NAME=<tool>`、整 CM volume 掛載。
Pod template 零 dynamic 欄位。static 欄位（image、resources、`NATS_URL`）改動
= 該 tool rollout（預期行為）。

### 2c. 驗證資產（把保證變成測試）

1. **Gate 8 測試**：bump tool-a `configVersion` → 等 `/status` declared==applied →
   斷言 tool-a Pod UID 與 pod-template-hash 不變、b/c Pod UID 與 admitted 計數
   不受影響。
2. **爆炸半徑測試**：寫爛 tool-b 的 CM → 只有 tool-b 報 `specError`（沿用
   last-good 照跑），a/c 繼續前進；修好自動收斂。
3. **身分 CI 檢查**（可搬進真實 repo）：`instanceId` 不可變、`durable` 不可變、
   任兩 tool 不得同 durable。
4. **SpecParser 容忍性釘住測試**：帶 `projection:` 塊的檔案 parse 結果與不帶時
   相同。

### 2d. Offboard runbook（文件化指令序列）

1. `desiredState: STOPPED` → 等 `/status` 確認 drain 完成
2. `nats consumer info` 抓證據（名字、cursor、pending）記入退場記錄
3. `nats consumer rm`——**唯一破壞性步驟，人工執行、雙人確認**
4. 移除該 tool 的 overlay（CM + Deployment 一起消失）
5. `instanceId` 記入 tombstone 清單；同名重 onboard 必須配新 ULID

順序刻意「先停、再刪 consumer、最後撤宣告」——與「config 消失 ≠ 退場」一致。

### 2e. 已知限制（明文接受）

無 lease fencing（操作紀律：不 force-delete Pod、只用 Recreate）；無 finalizer
（runbook 替代）；status 斷頭（watch 腳本/dashboard 抓 `/status`）；變更照走變更單；
render 端（一大包 values）爆炸半徑仍在，由 CI 擋。每條的解法都在 §3/§4，
觸發條件見 note §7。

---

## 3. Demo controller

### 3a. `ToolListener` CRD（`v1alpha1`）

```yaml
apiVersion: listener.dc/v1alpha1
kind: ToolListener
metadata:
  name: tool-a                      # tool 身分 = CR 名（天然不可變）
  finalizers: [listener.dc/cleanup] # controller 掛上
spec:
  stream: TOOLS                     # durable hash 輸入之一 → CEL 不可變
  subject: "tool.a.>"               # dynamic
  desiredState: RUNNING             # dynamic; enum RUNNING|STANDBY|STOPPED
  configVersion: "v42"              # dynamic; 非空
  retry: { interval: 5s, maxAttempts: 10 }
  drainTimeout: 30s
  workload:                         # static → 改了 rollout 該 tool
    image: dc/listener:1.2
    resources: { ... }
status:                             # 只有 controller 寫
  conditions: [ Ready, CleanupBlocked, IdentityConflict, ... ]
  observed: { uid, generation, revision, configVersion, state, reason }
  cleanup: { operationId, state }
```

### 3b. Reconcile 迴路（java-operator-sdk）

- CR → 確保 CM projection（§1a，蓋 `owner=UID`、`generation`、`revision`、
  `operation: null`）+ Deployment（共用 template，2b 形狀）存在且對版。
- 子物件掛 ownerReference（finalizer 移除後 GC 免費）。
- dynamic 變更只改 CM data；static 變更改 template → 只滾該 tool。
- 觀測回流：週期性 GET Pod `/status`，把 `observed`/`cleanup` 抄進
  `ToolListener.status`——status 閉環在此發生。

### 3c. Finalizer 退場（gate 4）

```text
kubectl delete → deletionTimestamp（finalizer 擋住）
→ controller 產 terminationOperationId，patch CM projection.operation（1d）
→ runtime 驗證 → 停 fetch → drain → 刪 consumer → /status cleanup{opId, Complete}
→ controller 驗證精確 UID+opId（過期/異 ID ack 一律拒收）
→ 刪 Deployment+CM → 移除 finalizer → CR 消失
失敗分支: cleanup{Blocked, reason} → condition CleanupBlocked + requeue 重試
強制孤兒: operator 加 annotation → 先寫 orphan record（3d）才放行 finalizer
```

### 3d. Orphan record + GC 拒絕（gate 7 簡化）

Orphan record = controller namespace 的 `listener-orphans` ConfigMap，每筆
`{stream, durable, ownerUid, opId, time}`。GC 為**手動觸發的檢查命令**（無排程器）：
拒刪任何仍被活 CR 宣告的資產。Demo 斷言：同名重 onboard → 新 UID → 新 durable
→ 舊 orphan record 動不到新 durable。

### 3e. 寫入把關（CEL ValidatingAdmissionPolicy，無 webhook）

`spec.stream` 不可變、`desiredState` 枚舉、`configVersion` 非空、retry 界限。
跨物件唯一性 CEL 做不到 → demo 由 controller reconcile 檢查，衝突設
`IdentityConflict` condition 並拒投影；production 要寫入時就擋才需 webhook
（記為硬化項）。durable 由 UID 派生，撞名機率密碼學等級低——此層是防禦縱深。

### 3f. Runtime 側全部改動（小、additive，排進 demo plan）

1. 讀 `projection:` 塊 → `/status` 加 `observed`（1c）
2. termination operation 處理器 = 唯一 consumer 刪除路徑（1d）；同步改寫 T2
   source-level 檢查的斷言
3. 其餘（state machine、drain、latch、reconciler）零變更

### 3g. 驗證（kind + JUnit/fabric8 驅動）

- Gate 4：建 CR → ACTIVE → delete → 斷言 NATS consumer 確實消失、順序正確、
  stale/異 ID ack 被拒、CleanupBlocked 分支可重試。
- Gate 8：改 `spec.subject` → CM 更新 → Pod UID/template hash 不變 → applied 收斂。
- Gate 7 簡化：3d 的斷言。
- 中繼版的 2c 測試在同一 kind 環境先行存在，controller 版繼承。

### 3h. 明文不做（demo 邊界）

Lease 實作（§4d 存檔）、admission webhook、CRD conversion/多版本、controller HA、
metrics、production RBAC 硬化。

---

## 4. 遷移路徑與 ADR 處置

### 4a. 分期與 plan 拆分

```text
Phase 0（進行中）: refactor T6/T7 收尾；本 spec + fixture（parallel gate 範圍）
Phase 1（中繼版 plan）: deploy/interim + kind + 2c 驗證資產（等 T7 落地後開工）
                        → pattern 搬進真實 GitOps repo（批次一次變更單）
Phase 2（demo controller plan）: 3a–3g；依賴 phase 1 的 kind 環境與 fixture
Phase 3（條件觸發，不在本 spec 排程）: production 化——webhook、lease、HA、
                        conversion、durable 身分遷移裁決（4b）
```

### 4b. Durable 身分遷移（phase 3 裁決，本 spec 僅並陳）

核心代價 = cursor：換 durable 名 = 丟 cursor 位置。

- **(i) 收養**：CR 以 annotation 記 legacy durable，controller 對舊 tool 投影舊名、
  新 tool 用 UID 派生。無 cursor 損失；永久揹相容分支與「兩種名並存」的認知成本。
- **(ii) drain 後換新**：STOPPED → 確認 drain → 建新 durable →（依 at-least-once
  語意）replay 或接受缺口 → runbook 刪舊 consumer。一次性痛，之後全域一致。

裁決依實際 backlog 數據與 replay 容忍度；demo 一律 greenfield UID 派生。

### 4c. ADR 處置

- 新增**短 ADR-0002**：「Interim delivery: GitOps-managed per-tool projection」。
  依據：ADR-0001 明文 changing delivery mechanism requires a later ADR。內容：
  投影形狀同終態；**寫入者**於 phase 1–2 為 GitOps render、phase 3 起為 controller；
  UID/revision/last-good/status 契約不變；production 進入時序 = 中繼版先行。
- ADR-0001 不改寫，僅加一行時序註記指向 ADR-0002。

### 4d. Lease 設計（存檔，phase 3 實作）

一 ToolListener UID 一個 Lease，holder = Pod 名。Runtime 連線前取得；每
duration/3 續約；續約失敗 → 在 local deadline（duration − 安全邊際）前停
admission、關 NATS（fail-closed）；替代者等過期才接手；controller 清理 worker
同受 lease 約束。**Trade-off 明文**：把 K8s API server 拉進資料路徑——API 抖動
可停消費。緩解：duration 30–60s、續約抖動、降級而非 crash。中繼版/demo 以操作
紀律替代（不 force-delete、只用 Recreate）。

---

## 5. 風險與假設

1. **變更單豁免假設未證實**——「`ToolListener` dynamic 寫入可歸類 routine/standard
   change」需變更管理方確認；佐證 = K8s audit log + RBAC + gate 1/8 測試證據。
   此假設是 phase 3 的最大觸發槓桿；被推翻則中繼版可能長期化（note §4 結論）。
2. **SpecParser 容忍未知根鍵**——已讀碼證實；2c-4 釘住測試防回歸。
3. **kubelet 同步延遲**（預設最長 ~1 分鐘）——對每週數次變更可接受；記
   `configMapAndSecretChangeDetectionStrategy` knob 備查。
4. **Controller ↔ Pod `/status` 網路可達**——kind 無虞；真實 cluster 留意
   NetworkPolicy。
5. **Durable 遷移 cursor 代價**——裁決刻意延後（4b），需 backlog 數據。
6. **並行協調**——refactor T6/T7 仍在跑；phase 1 只增 `deploy/`，不碰
   `demo/`、`runtime/`；等 T7 落地開工。
7. **NATS in-place filter 更新**已由 T2 consumer-safety 證據證實；NATS 版本升級
   時需重驗。

## 6. 接受準則（本設計自身的完成定義）

- 兩個 plan 各自通過本 repo 慣例（RED→GREEN、獨立 extra-high review、本地 commit）。
- Phase 1 完成 = 2c 四項測試在 kind 全綠 + runbook 演練一次留證據。
- Phase 2 完成 = 3g 三組斷言全綠 + demo 腳本一人可重播。
- 本 spec 經 owner 審 + grill-me 硬化後，交 writing-plans 產出兩個 plan。
