# Handoff: Controller Technical Design 討論

**日期:** 2026-07-14
**來源 session:** listener prototype 實作 + single-session refactor 執行中
**下一步任務:** 為 production controller 進行 technical design（brainstorming → spec → grill-me）

> 註：本檔由前一 session 的貼上內容重建；貼上時數處損毀，已依 repo 內 ADR/plan/ledger 補齊並以 2026-07-14 實際驗證結果更新。

---

## 1. 專案現況（2026-07-14 已驗證）

- Repo: `/Users/johnson.chiang/workspace/dc_listener`，分支 `nats-prototype`，只 commit 本地、**不 push**
- 驗證指令：`git log --oneline -10` 與 `cat .superpowers/sdd/progress.md`（ledger 是 source of truth）
- 已驗證進度：
  - 原型 plan Task 1–7 全部完成（含獨立 review）
  - **ADR-0001 已採納**（commit `747f9e7`）：one Pod / one process / one ListenerSession
  - Refactor plan 執行中：T1 `7f8503b`（outage error boundary）✅ review 過；T2 `01d1619`（durable 所有權）✅ review 過
  - T3 `acbbc63`（single session reconciler）已 commit，但 ledger **尚無 T3 entry**（review 記錄未落）；T4–T7 未動
  - Plan 的「Parallel gate after Task 2」已開：controller technical design 可與 T3–T6 並行，**限文件/fixture**（design doc + `ToolListener.spec`→projection schema fixture），不得加 controller code、CRD manifest、Lease/finalizer 實作或相依

## 2. 必讀文件（優先順序）

1. `docs/adr/0001-single-session-pod-runtime.md` — **必讀**，controller 的所有需求都源自此
2. `docs/plans/2026-07-13-single-session-pod-refactor.md` — 「Explicit follow-up project」段落 = controller 範圍清單；「Scope and invariants」= runtime 側已定契約
3. `listener-lifecycle-message-admission-architecture-guidance-v1.1.md` — 領域模型（desiredState/observedState、7 states、conditions）
4. `docs/specs/2026-07-13-listener-session-nats-prototype-design.md` — 原型 spec（多 session 部分已被 ADR supersede，僅供歷史）

## 3. Controller Design 範圍（plan 明定，勿縮勿擴）

- `ToolListener` CRD：spec/status、admission/defaulting、status 擁有權（controller 寫 status，runtime 經 /status 回報）
- 每 tool 一份 ConfigMap projection：帶 ToolListener **UID、generation、configVersion、projection revision、operation ID**；更新內容不得改 Pod template hash
- **UID 派生 durable identity**（bounded deterministic hash）：不可操作者配置、資源生命週期內不可變、admission 拒絕不同活 UID 已擁有的資產身分
- 一 replica `Recreate` workload 生成：Pod template 不含 dynamic 欄位
- Lease per ToolListener UID：取得、續約、fail-closed NATS fencing、replacement handoff
- **Finalizer 退場協定**：deletionTimestamp → termination request 投遞（含 operationId + 精確資產身分 + `DeleteConsumer` policy）→ runtime drain + 刪 consumer → `CleanupComplete`（精確 UID/opId 才接受）→ 移除 workload → 移除 finalizer；失敗 = `CleanupBlocked` + 重試；強制孤兒需記錄 + GC 拒刪活宣告
- Controller 層 status 聚合、static-field rollout 測試

## 4. 關鍵銜接點（易漏，務必帶進設計）

1. **Runtime 已無任何 consumer 刪除路徑**（refactor T2 移除了 `deleteConsumer` API + delete/recreate fallback，且有 source-level 檢查）。Finalizer 協定要求 runtime 重新獲得一條**被授權的**刪除路徑——這是 controller design 必須定義的新 runtime 契約（termination request 的投遞格式與驗證）
2. `Event.Shutdown`（非破壞性 drain）已存在，但 production caller（JVM shutdown hook）在 refactor T4 才接上——**已驗證 T4 尚未落地**
3. 現行 `/status` **沒有** observed UID / generation / projection revision 欄位（只有 declared/applied configVersion、state、reason 等）——ADR 要求 runtime 回報這些觀測值，controller design 需定義 /status 契約擴充

## 5. 開放問題（grilling 重點）

- CRD vs「管理服務 + DB」：guidance 10.1 原列為開放決策，ADR 已傾向 ToolListener resource——確認是否定案、寫下理由
- Lease 參數（時長/續約間隔/local deadline）與代價：**fail-closed 把 K8s API 拉進資料路徑**（API server 抖動可停消費）——此 trade-off 需在設計中明文
- Acceptance gates 4/5/7/8（finalizer、partition 下 lease 交接、orphan GC、projection revision）需要 kind/k3d 級測試環境——何時立、範圍多大
- UID 派生 hash 的具體編碼（長度上限、cluster/stream 身分如何進 hash）
- Projection 投遞機制變更需另立 ADR（plan 已註記）

## 6. 工作方式慣例（本 repo 既定）

- 流程：brainstorming（一次一問）→ spec 寫入 `docs/specs/` → `/grill-me` 硬化 → writing-plans → subagent-driven（每 task 獨立 extra-high review）
- TDD 嚴格 RED→GREEN；commit 訊息精簡具體、**無 Co-Authored-By trailer**、不 push
- 無本機 gradle/JDK21：build/測試一律用 `gradle:8.7-jdk21` docker 容器（見 refactor plan「Standard verification」）
- `AGENTS.md`/`CLAUDE.md` 保持 untracked，永不 stage；工作樹有 T3+ 未 commit 的 demo/docs 變更，commit 時只加自己的檔案
