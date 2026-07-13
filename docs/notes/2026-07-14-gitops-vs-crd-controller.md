# 宣告管理方式比較：GitOps + Template vs `ToolListener` CRD + Controller

**日期:** 2026-07-14
**目的:** 為 production controller technical design 的第一個開放決策（guidance §10.1：宣告存哪、誰更新 desired state）提供比較依據。
**專案參數:** tool 數量約幾十個；變更每週數次；組織規定走部署管線的變更需報 change 流程；ADR-0001 已採納（one Pod / one process / one ListenerSession，controller + finalizer + lease 為前提）。

---

## 1. 候選方案

### A. GitOps + Template（無 controller）

宣告 = git repo 的 values 檔（每 tool 一個 entry）。Helm/Kustomize render 出每 tool 的
Deployment + ConfigMap projection，由 ArgoCD/Flux 或 CI apply。

- Onboard = PR 加 entry；offboard = runbook（人工 `desiredState: STOPPED` → 確認 drain →
  `nats` CLI 刪 consumer → 刪 entry）。
- Identity：git 無 UID，改用 values 內顯式 `instanceId`（onboard 時產生、CI 檢查不可變），
  durable 名由它派生。
- Admission 的資產衝突檢查 → CI 檢查。Status 聚合 → dashboard 直抓各 Pod `/status`。

### B. ConfigMap 當宣告庫 + Controller（❌ 假輕量，直接排除）

省掉 CRD 但 controller 照寫，並且要在 ConfigMap 裡手工重造 per-entry UID、generation、
validation（ConfigMap 無 schema、寫入時無法拒絕）、per-entry finalizer（做不到——
deletionTimestamp 屬於整個 ConfigMap）、status 回寫。K8s 文件明示此情境應用 CRD。
比兩端都差，不再納入比較。

### C. `ToolListener` CRD + Controller（ADR-0001 現行方向）

宣告 = CR 實例，為唯一 operator-writable source of truth。Controller watch 後生成
workload + ConfigMap projection；admission webhook 做 defaulting 與資產衝突拒絕；
finalizer 驅動退場協定；controller 回寫 status。

---

## 2. 逐維度比較（A vs C）

| 維度 | A. GitOps + Template | C. CRD + Controller |
|---|---|---|
| 需新建/運維的元件 | 無（沿用 GitOps 工具鏈） | CRD、controller、admission webhook（可用性元件） |
| Dynamic 變更傳導 | 改 values → ConfigMap data 變 → kubelet 同步 → 熱 reconcile；**不 rollout** | 改 spec → controller 更新 ConfigMap → 同左；**不 rollout** |
| Static 變更 | 該 tool Pod rollout | 同左，無差別 |
| **組織 change 流程** | 每次變更走部署管線 ⇒ **每週數次 × 幾十 tool 全數繼承部署級 change 流程** | Dynamic 變更 = 對核可系統的 API 資料寫入，不產 artifact、不 rollout ⇒ 有依據歸類為 routine/standard change；static 變更照走重流程。**邊界與變更分級對齊** |
| Per-tool identity | 自造 `instanceId` + CI 保證不可變；同名重 onboard 需人工換 ID | K8s UID 原生：唯一、不可變、同名重建必得新 UID ⇒ UID 派生 durable identity 的語意免費 |
| 退場（offboard） | Runbook 人工操作；刪錯 consumer / 忘刪成孤兒風險由人扛 | Finalizer 協定自動化：drain → 刪 consumer → 精確 UID/opId ack → 移除 workload；失敗 `CleanupBlocked` 重試；orphan GC 有記錄 |
| 寫入時驗證 | CI 擋 PR；擋不住直接 `kubectl edit` rendered 資源 | Admission 在 API 層拒絕（含資產身分衝突）；RBAC 收斂寫入口 |
| 變更偵測/併發 | git diff；無 optimistic concurrency | generation/observedGeneration、resourceVersion、watch/informer 現成 |
| Status 回報 | Dashboard 抓各 Pod `/status`，無宣告側回寫 | Controller 聚合寫回 `ToolListener.status`，宣告與觀測同處 |
| 審計 | git history（宣告側）；cluster 側操作另計 | K8s audit log：誰、何時、改了什麼欄位 |
| 測試環境負擔 | 輕：template 測試 + gate 8 的 projection 測試 | 重：admission/finalizer/lease 需 envtest 或 kind（acceptance gates 4/5/7/8） |
| Schema 演進 | values 檔自由但無版本機制 | v1alpha1→v1 需 conversion webhook，欄位語意變更成本高 |
| etcd/控制面依賴 | Workload 與 lease 本來就在 K8s | 同左 + 宣告也入 etcd。物件數十、每個幾 KB，離 etcd 限制數個數量級；且 lease fail-closed 已把 API server 拉進**資料路徑**，宣告入組態路徑為增量小成本 |
| 與 ADR-0001 關係 | **需先修 ADR**（plan 明文：不得默默偏離） | 一致 |

## 3. 兩條路共同的注意點

1. **變更傳導機制相同**：兩案最終都是「ConfigMap projection 內容變、Pod template 不變、
   runtime watch 檔案熱 reconcile」。controller 不改變傳導路徑，它加的是 admission、
   finalizer、identity、status。
2. **Gate 8（projection 更新不得改 Pod template hash）兩案都要測**，只是受測方由
   controller 換成 template。
3. **工具鏈地雷**（GitOps 案尤其致命，controller 案生成 manifest 時同樣要避開）：
   - Helm 慣用的 `checksum/config` Pod annotation 會把每次 dynamic 變更變成 rollout——不得用於 dynamic ConfigMap；
   - Kustomize `configMapGenerator` 預設內容 hash 後綴會改資源名 → 改 Pod template → rollout——必須 `disableNameSuffixHash: true`。

## 4. 結論

**建議定案 C（`ToolListener` CRD + Controller）。** 決定性因素依權重：

1. **組織 change 流程**：本環境每次走管線的變更都要報 change。GitOps 把最頻繁的操作
   （每週數次的 dynamic 變更）全綁上部署管線；CRD 把它移出管線、變成可審計的 API 資料
   操作，static/dynamic 邊界恰好與組織變更分級對齊。這是頻率最高、持續最久的成本項。
2. **退場安全**：runtime 側（refactor T2）已移除一切 consumer 刪除路徑，重新授權刪除
   本來就需要一個受信的協調者；finalizer 協定的自動化與精確 ack 由 CRD 語意直接支撐。
3. **Identity**：UID 派生 durable identity 的「同名重建 = 新身分」語意 K8s 原生保證。
4. GitOps 案的工程輕量被組織成本抵銷後，剩餘優勢只有測試環境較輕——而 kind/envtest
   遲早要立（acceptance gates 4/5/7/8 本就要求）。

**需查證的假設（寫入 spec 假設節）**：「`ToolListener` dynamic 欄位寫入可歸類為
routine/standard change、免逐次變更單」需與變更管理方確認；佐證材料 = K8s audit log +
RBAC + 「dynamic 變更可證明不觸碰 workload」的測試證據（gate 1/8）。

若此假設被推翻（CRD 寫入仍需逐次報 change），組織成本差距消失，方案 A 重新可考慮——
屆時退場頻率與人工風險成為主要判準，且必須先修 ADR-0001。

## 5. 附錄：兩條四層鏈對照

Config 在系統裡有四份——一份真相、三份衍生，下游全部可從上游重建，runtime 不持久化
任何 config（last-good 僅在記憶體）。兩案的鏈：

```text
GitOps:  operator ──PR──► ① git values entry                【真相】
                             │ CI render + ArgoCD/Flux sync  ←通用搬運工
                             ▼
                          ② ConfigMap projection（etcd）     【衍生】
                             │ kubelet 同步
                             ▼
                          ③ node 掛載檔 ──► ④ JVM in-memory
                                              │
                                              └► /status ► dashboard   ✗ 回不到①

CRD:     operator ──API──► ① ToolListener CR（etcd）         【真相】
                             │ domain controller             ←懂 UID/finalizer/status
                             ▼
                          ② ConfigMap projection（etcd）     【衍生】
                             │ kubelet 同步
                             ▼
                          ③ node 掛載檔 ──► ④ JVM in-memory
                                              │
                                              └► /status ► controller ► 寫回①.status ✓
```

- **②③④ 兩案逐字相同**——傳導機制不是差別；差別在 ① 的物件模型（純文字 vs 有
  status 子資源/deletionTimestamp/finalizer/UID 的物件）與 ①→② 搬運工是否懂 domain。
- **Status 回流**：GitOps 斷頭（觀測與真相永久分居）；CRD 閉環（declared/applied 在
  真相物件會合）。
- **刪除流**：GitOps 刪 entry = sync 直接 prune workload，Pod 被殺無 drain 協定、
  consumer 留置等人工 runbook，宣告刪除與資產清理之間無擋板；CRD 靠 finalizer 把刪除
  變成受監督流程（deletionTimestamp → termination request → drain+刪 consumer →
  精確 UID/opId ack → 移除 workload → 移除 finalizer）。
- 兩案 runtime 唯一持久資產都在 NATS 側（durable consumer cursor），不在 config 鏈上
  ——這是「config 消失 ≠ 退場」的根本原因。

## 6. 三方案對照（含現況）

宣告管理其實是兩個獨立的軸：**真相放哪**（git vs CR）×**投影切多細**（一大包 vs
每 tool 一份）。實際被占用的三格：

- **現況**：GitOps + 一大包 ConfigMap（= prototype 的 `sessions.yaml` 相容模型）
- **改良 GitOps**：GitOps + 每 tool 一份 ConfigMap（template render 時切開）
- **目標（ADR-0001）**：CRD + controller + 每 tool 一份 projection

| 維度 | 現況：GitOps 一大包 | 改良：GitOps 每 tool 一份 | CRD + controller |
|---|---|---|---|
| 壞檔爆炸半徑 | 一個語法錯全 tool specError、全變更凍結 | 限單一 tool | 限單一 tool（且 admission 在寫入時就擋掉） |
| 變更漣漪 | 改任一 tool，所有 Pod kubelet 重同步、全 runtime 重 parse（靠 dedupe 消化） | 只有目標 Pod 收到 | 只有目標 Pod 收到 |
| Per-tool 中繼資料（UID/generation/revision/opId） | 手工塞 YAML entry，自行維護 | instanceId 手造 + git sha 可充 revision；opId 仍無自然家 | 物件原生：UID/generation 免費，controller 逐 tool 蓋章 |
| 退場 | Runbook；termination 混在共用檔，entry 生命週期彆扭 | Runbook；per-tool CM 生命週期較乾淨，但仍無「擋住直到清完」語意 | Finalizer 協定：受監督、可重試、精確 ack、ownerReference GC |
| RBAC | 能改一包 = 能改全部 | 可按 tool 收斂（rendered 資源層） | 可按 CR 收斂 + admission 驗證 |
| Status 回流 | 斷頭 | 斷頭 | 閉環（①.status 會合） |
| 組織 change 流程 | 每次變更走部署管線 | 同左（不變） | Dynamic 變更 = API 資料操作（假設待查證） |
| 遷移成本（自現況起） | — | 低：改 render template 切檔 + Pod 改掛 per-tool CM（一次性 rollout） | 高：CRD/controller/webhook + 宣告轉 CR + **既有 durable 身分的收養/遷移**（operator 配置名 → UID 派生名，需明確遷移設計） |
| 與 ADR-0001 | 過渡相容模型（plan 明文暫留） | 仍需修 ADR 才能當終態 | 一致 |

**傳導漣漪示意（改 tool-b 的 dynamic 欄位，✱ = 被觸動）：**

```text
[1] 現況：GitOps 一大包
    git sessions.yaml ✱ ──► ConfigMap（一大包）✱
                              │ kubelet：掛它的每個 Pod 都同步
                              ▼
          tool-a 檔 ✱      tool-b 檔 ✱      tool-c 檔 ✱
              │                │                │
          parse→dedupe→丟   parse→套用 ✔    parse→dedupe→丟
          （N 個 Pod 全被叫醒，只有 1 個真的要）

[2] 改良 GitOps：每 tool 一份
    git values(tool-b) ✱ ──► CM-a(不動)  CM-b ✱  CM-c(不動)
                                           │ kubelet 只同步 CM-b
                                           ▼
                                      tool-b 檔 ✱ ──► 套用 ✔
          （漣漪 = 恰好一個 Pod；status 仍斷頭）

[3] CRD + controller
    kubectl edit toollistener/tool-b ✱ ──► controller ──► CM-b ✱（蓋 gen/revision）
                                                            │ kubelet
                                                            ▼
                                                       tool-b 檔 ✱ ──► 套用 ✔
                                                                         │
                    tool-b CR .status ◄── controller ◄── /status ────────┘
          （往下漣漪與 [2] 相同；多的是回流閉環）
```

三型的 Pod/Deployment 都不動（無 rollout）；[1]→[2] 消掉的是「全體被叫醒」，
[2]→[3] 加上的是觀測回流，不是傳導差異。

**讀法：**

1. 現況的三個痛點（爆炸半徑、漣漪、中繼資料）搬到「每 tool 一份」就消掉大半——
   這一步**不需要 controller**，只是 template/掛載的一次性手術。
2. 但改良 GitOps 消不掉的三樣（status 斷頭、退場無擋板、change 流程綁管線）正是
   CRD 案的核心價值——粒度治標，物件模型治本。
3. 因此合理路徑是**現況 → 改良 GitOps 作為戰術過渡（controller 開發期間）→ CRD 終態**，
   而非現況直跳終態；過渡步驟同時把 gate 8 的 projection 測試提前立起來。
4. 新增 grill-me 題目：**既有 durable consumer 的身分遷移**——現行是 operator 配置名
   （latch 相容輸入），CRD 終態是 UID 派生名。收養既有 consumer 還是 drain 後換新身分？
   設計文件必須明答。

## 7. 決策記錄（2026-07-14，與 owner 討論定案）

**定位翻轉：中繼站（改良 GitOps）是實戰版；controller 是 demo/方向驗證版。**

- **實戰版**＝production 實際運行模型，按 production 標準投資：per-tool 切檔（檔內沿用
  collection schema 單 entry，runtime 零變更）、gate 8 式 projection 測試、三地雷防護
  （穩定 CM 名／整 volume 掛載／無 checksum annotation）、offboard runbook 文件化、
  instanceId 慣例 + CI 不可變檢查。
- **Demo 版 controller**＝在 kind 走通終態（finalizer 退場、UID 派生身分、status 閉環），
  驗證 ADR 方向並留下演示證據；不做 webhook HA、conversion、升級策略等 production 硬化。
- **升級觸發條件**（demo → production controller）：「dynamic 變更可歸類 routine change」
  假設獲變更管理方證實，或 offboard 頻率/人工風險升高到 runbook 不可承受。
- **明文接受的實戰版限制**：無 lease fencing（靠不 force-delete 的操作紀律）、無 finalizer
  （退場走 runbook）、status 斷頭（靠 dashboard 抓 /status）、變更照走 change 流程。
- **ADR-0001**：方向不變，production 進入時序需加註（改良 GitOps 先行）——依 plan
  「不得默默偏離」原則，在 design doc/ADR 註記，不默改。
- **Demo controller 的驗證範圍（定案）**：gate 4（finalizer 退場握手）+ gate 8
  （projection 不動 Pod template hash）全做；gate 7 簡化（孤兒有記錄、GC 拒刪活宣告，
  不做完整 GC 排程）；gate 5（lease fencing）demo 不做——design doc 記完整設計與
  trade-off 供未來 production 硬化。
- **中繼版必須 demo + 實戰兩用**：不只是 spec 上的過渡段落——要有可演示的資產
  （per-tool projection、gate 8 測試、爆炸半徑/漣漪演示、offboard runbook）且同一套
  模式可直接搬進真實 GitOps repo 使用。

## 8. 進化路徑視角：最原本 → A → B → C

以「每次變更都要 rollout」的最原本為基線，三步進化各解**一類問題**，收益疊加：

```text
最原本：一大包 values.yaml render 出每 tool 的 deployment，config 內嵌在
        rendered manifest ─── 改任何東西 = re-render + deployment + 變更單
        （執行本來就是 per-tool Pod，隔離既有）
   │
   │ (1) 把「執行」與「config」分開【幅度：大；已完成 = 本 repo refactor】
   ▼
A: config 外部化、runtime 運行中 hot reload（現狀）
   │  改 config 不需要 deployment；但仍要進 gitops 改（pipeline → 變更單照走）。
   │  只有 image/資源這類 static 欄位才需要 deployment。
   │  ※ 交付粒度暫退一步：baseline 的 config 內嵌在各 tool 自己的 manifest
   │    （天然 per-tool），A 外部化時併成一大包共用檔——用粒度換 hot reload。
   ├─ 得到（這一步買到的核心能力）：
   │   1. 改 config 不需要 deployment——runtime 運行中 hot reload
   │   2. 壞 config 不 crash、也不弄壞線上行為——整份讀不懂就沿用
   │      last-good 照跑 + 回報 specError；只有自己 entry 非法才 fail-closed
   │      （停收訊息但 process 活著、宣告修好原地復原）
   │   3. NATS 斷線不倒站——降級 DEGRADED、有界重試、連回自動恢復（T1）
   │   4. durable 身分不會被誤刪——任何 config 變化都觸不到 consumer 刪除（T2）
   │   （per-tool 執行隔離為保留既有，非本步新增）
   ├─ 尚痛：config 交付併成一大包——牽一髮動全身；看不到 applied 了沒；
   │        下架靠人工；變更單照走
   │
   │ (2) config 按 tool 分裝——把 baseline 原有的 per-tool 粒度在外部化世界裡
   │     還原【幅度：小——只動打包/掛載方式，runtime 零變更】
   ▼
B: 每個 tool 一份 config（中繼版 = 實戰版）
   ├─ 得到：改誰只動誰、寫壞一份只影響一個 tool、每份 config 可蓋上
   │        身分/版本戳記、「改 config 不觸發 deployment」從此有測試把關（gate 8）
   ├─ 尚痛：看不到 applied 了沒；下架靠人工；變更單照走
   │
   │ (3) desired state 升級為正式物件，交給 controller 對帳【幅度：大——
   │     CRD + controller + 寫入把關 + kind 測試環境 + 身分遷移】
   ▼
C: ToolListener CRD + controller（demo 先行，觸發條件到才 production 化）
   └─ 得到：declared 與 applied 同處對帳（看得到 applied 了沒）、上下架
            自動化且有安全握手、寫錯當場被擋、（假設證實後）改 config 不進
            gitops/pipeline——直接 API 寫入 → 免變更單
```

**A 與 B 的關係——時間上可並行，語意上是疊加不是二選一：**

- 解的問題不同：A 管「runtime 怎麼消化 config」（本 repo，T6/T7 收尾中）；
  B 管「config 怎麼送到 runtime 面前」（GitOps repo/deploy 層，runtime 零變更）。
  互不碰對方的檔案，**可同時進行**。
- 但 B 不是繞過 A 的另一條路：「改 config 即時生效、壞檔不倒站」是 A 建立的
  能力——沒有 A，B 分裝好的 config 送到了也沒人會 hot reload。
- C 再疊在 A+B 上：controller 只是接管「誰來寫每個 tool 那份 config」，B 完成
  後 config 的形狀已經是 C 要的，C 的手術範圍因此縮小。

**投資視角**：(1) 的大投資已付清（本 repo refactor）；(2) 是薄手術、收益即刻
——CP 值最高；(3) 是下一筆大投資，等觸發條件（變更單豁免證實 / 退場頻率上升）。
