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
