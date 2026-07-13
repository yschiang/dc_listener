# Single-Session Pod Runtime Refactor Plan

> **Implementation workflow:** Execute one task at a time with strict RED → GREEN → REFACTOR. Before
> each commit, start a new independent review agent with extra-high rigor; fix all blocking findings,
> rerun the task verification, then commit locally. Never push.

**Goal:** Refactor the prototype from one JVM supervising many tool sessions to one process owning exactly
one `ListenerSession`, while retaining hot listener-config reconciliation and proving multiple isolated
tool workloads through Docker Compose.

**Architecture:** `SESSION_NAME` is immutable process identity. The process selects exactly one entry from
the existing `sessions.yaml` compatibility schema, owns one state machine/NATS connection, and exposes one
session in the existing `/status` compatibility envelope. One session actor virtual thread remains as a
convenient blocking loop, not as an isolation boundary; the mailbox serializes file-poller events with NATS
processing. Compose models controller-generated workloads as three default services using the same
image/config file but different identities
and status ports. Controller, Lease, finalizer, UID-derived durable identity, and production CRD work remain
outside this prototype and are governed by ADR-0001.

**Tech stack:** Java 21, jnats 2.20.2, SnakeYAML 2.2, JUnit 5, POSIX shell, Docker Compose, NATS JetStream.

## Scope and invariants

- Keep the current YAML collection shape temporarily; do not introduce a second config schema in this
  refactor.
- `SESSION_NAME` is required at process startup and selects exactly one declaration.
- Other YAML entries are ignored by that process. Adding/removing another tool never mutates the running
  session.
- A malformed whole file keeps the last-good session state and reports `specError`.
- A missing or invalid selected entry fails closed as `INVALID_SPEC`; it must not terminate the process or
  delete a durable consumer.
- Config disappearance is not offboarding. Only the future finalizer protocol may authorize consumer
  deletion. The prototype must make no config-only deletion claim.
- Dynamic changes (`desiredState`, subject/filter, `configVersion`, retry, drain timeout) reconcile without
  restarting the process.
- The first valid selected `durable` is a temporary compatibility input latched for the process lifetime.
  Later changes are invalid and may not connect, delete, or switch consumers. The production controller
  will replace this input with UID-derived immutable identity.
- Until the finalizer handshake exists, the prototype contains no consumer-delete API or NATS delete call.
- Keep `/status` as `{ "cell": ..., "sessions": { ... } }` during migration, but it must contain at most
  the process-selected session.
- Use platform-neutral polling; do not assume macOS. The existing 500 ms stable-content polling remains.
- Preserve `AGENTS.md` and `CLAUDE.md` as untracked local files and never stage them.
- Existing uncommitted outage and demo work must be preserved and split into the tasks below rather than
  discarded or accidentally bundled.

## Standard verification and review gate

Run Java tests from the repository root:

```sh
docker run --rm \
  -v "$(pwd)/runtime:/work" -w /work \
  -v dc-listener-gradle:/home/gradle/.gradle \
  gradle:8.7-jdk21 gradle --no-daemon test --rerun-tasks
```

For every task:

1. Add the named failing test and run the narrowest relevant test to record RED. Task 1 is the sole
   documented exception because its RED outage was already observed and its test/fix changes are present
   together in the shared dirty tree; do not revert that tree merely to recreate history.
2. Make the smallest production change that produces GREEN.
3. Run the task checks plus the full Java test suite when Java changed.
4. Start a **new** independent review agent/task. Give it the task diff, ADR-0001, relevant tests, and ask
   for extra-high rigor on correctness, lifecycle safety, portability, and missing tests.
5. Address all blocking findings and rerun verification.
6. Commit only the task files with the exact concise commit message listed below. Do not push.

After this plan receives independent approval, commit this plan alone as
`plan single session runtime refactor` before beginning Task 1.

---

### Task 1: Finish the NATS fetch/ack outage error boundary

**Purpose:** Land the already-discovered real-outage fix independently, before architectural changes can
hide regressions.

**Files:**

- Modify: `runtime/src/main/java/dc/listener/session/NatsLink.java`
- Modify: `runtime/src/main/java/dc/listener/session/JnatsLink.java`
- Modify: `runtime/src/main/java/dc/listener/session/ListenerSession.java`
- Modify: `runtime/src/main/java/dc/listener/session/SessionStateMachine.java`
- Modify: `runtime/src/test/java/dc/listener/session/FakeNatsLink.java`
- Modify: `runtime/src/test/java/dc/listener/session/ListenerSessionTest.java`
- Modify: `runtime/src/test/java/dc/listener/session/SessionStateMachineTest.java`

- [ ] **Preserved RED evidence:** Record that the real NATS outage first left sessions falsely ACTIVE, then
  killed actor loops with `IllegalStateException: Connection is Closed` during ack. The focused tests and
  fixes now coexist in the dirty tree, so preserve them and do not selectively revert shared files.
- [ ] Treat `disconnectedFetchDegradesThenReconnects`, `disconnectedAckDoesNotKillSessionThread`, and
  `drainErrorReconnectsAppliedConfigThenResumesPendingChange` as regression tests for that observed RED.
- [ ] Make `fetch` reject a disconnected link before and after the jnats fetch call.
- [ ] Make `ack` part of the checked `LinkException` boundary and translate closed-connection failures to
  `MESSAGING_ENDPOINT_UNREACHABLE`.
- [ ] In ACTIVE and DRAINING, clear unacked in-memory handles and emit `FetchError` instead of allowing an
  exception to kill the session loop.
- [ ] On an outage during DRAINING, reconnect the currently applied configuration, return to DRAINING,
  then complete the pending config transition.
- [ ] Run focused tests:

  ```sh
  docker run --rm -v "$(pwd)/runtime:/work" -w /work \
    -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 \
    gradle --no-daemon test --tests '*ListenerSessionTest' \
    --tests '*SessionStateMachineTest' --rerun-tasks
  ```

- [ ] Run the full Java suite and independent review gate.
- [ ] Commit: `fix nats outage error boundary`

---

### Task 2: Remove unauthenticated durable deletion and latch consumer identity

**Purpose:** Enforce the ADR durable-ownership boundary before the single-session production cutover. With
no finalizer/UID/operation-ID protocol in this prototype, no code path is authorized to delete a consumer.

**Files:**

- Modify: `runtime/src/main/java/dc/listener/session/Event.java`
- Modify: `runtime/src/main/java/dc/listener/session/NatsLink.java`
- Modify: `runtime/src/main/java/dc/listener/session/JnatsLink.java`
- Modify: `runtime/src/main/java/dc/listener/session/ListenerSession.java`
- Modify: `runtime/src/main/java/dc/listener/session/SessionStateMachine.java`
- Modify: `runtime/src/main/java/dc/listener/reconcile/Reconciler.java`
- Modify: `runtime/src/test/java/dc/listener/session/FakeNatsLink.java`
- Modify: `runtime/src/test/java/dc/listener/session/ListenerSessionTest.java`
- Modify: `runtime/src/test/java/dc/listener/session/SessionStateMachineTest.java`
- Modify: `runtime/src/test/java/dc/listener/reconcile/ReconcilerTest.java`
- Create: `demo/consumer-safety-test.sh`

- [ ] **RED:** Add state-machine tests proving the first valid durable is latched, a later durable mutation
  becomes `FAILED/INVALID_SPEC` without adopting the new durable, and restoring the original durable
  recovers the same state machine.
- [ ] **RED:** Add slow-batch session tests for both ACTIVE → explicit `SpecInvalid` → restored ACTIVE and
  ACTIVE → durable-mutating `SpecChanged` → `FAILED/INVALID_SPEC` → original durable restored. They must
  prove old fetched handles are discarded for NATS redelivery, zero stale handles are acked after reconnect,
  the actor survives, and the same session instance recovers.
- [ ] Replace destructive `Event.Terminate` semantics with a non-destructive `Event.Shutdown`: drain,
  close NATS, and end the loop while retaining the durable. Rename the state flag/tests accordingly.
  `desiredState: STOPPED` remains a non-destructive, restartable lifecycle state.
- [ ] Temporarily change the old multi-session `Reconciler` so declaration disappearance delivers
  `SpecInvalid("declaration missing")` to the existing instance instead of retiring/replacing it. Add a RED
  remove→re-add test proving the same instance and original durable latch survive, and that re-adding with a
  changed durable fails closed without creating another consumer. Remove its retirement/reaper/replacement
  paths and obsolete replacement tests; `Shutdown` is reserved for process lifecycle. Task 4 deletes the
  remaining compatibility reconciler.
- [ ] Remove `NatsLink.deleteConsumer`, `JnatsLink.deleteConsumer`, and the fake's deletion flag. Process
  shutdown and configuration disappearance can close NATS but cannot delete the durable.
- [ ] Remove the broad `JetStreamApiException` delete-and-recreate fallback from `JnatsLink.connect()`.
  A rejected consumer update must close the link, fail closed with an appropriate classified error, and
  leave the existing durable untouched.
- [ ] Centralize session event handling so **any** event that transitions the state machine to `FAILED`
  clears in-memory in-flight handles before the link is closed, including internal rejection of a
  durable-mutating `SpecChanged`. Do not ack handles created by a previous connection after recovery.
- [ ] Make `ListenerSession` reject blank identity, make `start()` idempotent, and expose immutable `name()`
  for ownership validation.
- [ ] **Real NATS acceptance:** `demo/consumer-safety-test.sh` captures the selected consumer name, creation
  timestamp, and cursor/pending evidence; applies a subject/filter update; proves the same durable was
  updated in place without cursor reset; then attempts a durable mutation and proves no second consumer was
  created. From its first version, accept `STATUS_URL` and `RUNTIME_SERVICE` overrides so the same evidence
  can run before and after the Compose cutover. Install cleanup traps before mutation, back up/restore the
  config, and restore ACTIVE state on exit. If the pinned NATS version cannot update the filter in place,
  stop and revise the design rather than reintroducing delete/recreate.
- [ ] Add a source-level safety check that `runtime/src/main` contains no `deleteConsumer` or NATS consumer
  delete call.
- [ ] Run both `sh -n demo/consumer-safety-test.sh` and `dash -n demo/consumer-safety-test.sh` before the
  real NATS acceptance.
- [ ] Run focused state-machine/session tests, the full Java suite, the real NATS safety acceptance, and the
  independent review gate.
- [ ] Commit: `enforce durable consumer ownership`

---

### Task 3: Introduce a one-owner SingleSessionReconciler

**Purpose:** Replace the session map/reaper/onboarding behavior with a reconciler that can affect only its
immutable selected tool.

**Files:**

- Create: `runtime/src/main/java/dc/listener/reconcile/SingleSessionReconciler.java`
- Create: `runtime/src/test/java/dc/listener/reconcile/SingleSessionReconcilerTest.java`

**Target interface:**

```java
public final class SingleSessionReconciler {
    public SingleSessionReconciler(Path file, ListenerSession session);
    public synchronized void reload();
    public synchronized void applySnapshot(String text);
    public String sessionName();
    public ListenerSession session();
    public String specError();
}
```

- [ ] **RED:** Add tests proving the supplied session has a nonblank immutable identity, starts/owns one
  idempotent session loop, selects only that identity, ignores changes (including durable changes) to other
  entries, deduplicates unchanged selected specs, and applies a selected dynamic config change.
- [ ] **RED:** Add failure-contract tests:
  malformed YAML preserves last-good ACTIVE state while reporting `specError`; a missing selected entry
  and an invalid selected entry produce `FAILED/INVALID_SPEC`; neither terminates the process nor mutates
  consumer identity; restoring the selected declaration recovers the same session instance with no stale
  in-flight handles.
- [ ] Implement one eagerly started `ListenerSession`, last-delivered valid/invalid deduplication, and
  selected-entry projection from `SpecParser.Parsed`.
- [ ] Do not retain a sessions map, retiring set, replacement thread, or factory.
- [ ] Run:

  ```sh
  docker run --rm -v "$(pwd)/runtime:/work" -w /work \
    -v dc-listener-gradle:/home/gradle/.gradle gradle:8.7-jdk21 \
    gradle --no-daemon test --tests '*SingleSessionReconcilerTest' --rerun-tasks
  ```

- [ ] Run the full Java suite and independent review gate.
- [ ] Commit: `add single session reconciler`

---

### Task 4: Switch the process entry point and status to single-session mode

**Purpose:** Make the production runtime path contain exactly one session and remove the multi-session
supervisor.

**Files:**

- Modify: `runtime/src/main/java/dc/listener/Main.java`
- Modify: `runtime/src/main/java/dc/listener/status/StatusServer.java`
- Create: `runtime/src/test/java/dc/listener/MainTest.java`
- Modify: `runtime/src/test/java/dc/listener/status/StatusServerTest.java`
- Delete: `runtime/src/main/java/dc/listener/reconcile/Reconciler.java`
- Delete: `runtime/src/test/java/dc/listener/reconcile/ReconcilerTest.java`

- [ ] **RED:** Change status tests to construct `SingleSessionReconciler` and prove the compatibility
  `sessions` object contains exactly the selected tool, even when the YAML contains several declarations.
- [ ] **RED:** Add a startup configuration test by extracting a package-private environment parsing method
  (or a minimal immutable startup-config record) that rejects missing/blank `SESSION_NAME` and accepts
  `NATS_URL`, `SESSIONS_FILE`, `PROCESS_DELAY_MS`, status port, and bounded shutdown timeout defaults. It
  must also reject invalid numeric values with a clear startup error.
- [ ] Wire `Main` to create one `ListenerSession`, one `SingleSessionReconciler`, one `FileWatcher`, and one
  `StatusServer`. Log the immutable tool identity at startup.
- [ ] Register a JVM shutdown hook that emits non-destructive `Shutdown`, waits only up to the configured
  bound for drain/close, then stops the status server. Extract the hook body/coordinator for tests proving
  graceful shutdown retains the durable, closes NATS, terminates one actor loop, and returns on timeout.
  SIGTERM must not rely on config disappearance, and no second shutdown may start another loop.
- [ ] Retain the named virtual session/poller threads as a small blocking-I/O convenience and remove comments
  that claim they provide session isolation. Do not introduce unmanaged non-daemon platform threads.
- [ ] Change `StatusServer` to read one reconciler/session while keeping the current JSON compatibility
  envelope. Replace `cellId` semantics with a process/runtime identifier only if tests and demo consumers
  are updated in the same task; do not invent cluster-wide aggregation.
- [ ] Delete the old multi-session reconciler and its tests once no production caller remains.
- [ ] Run focused Main/status tests, then the full Java suite and independent review gate.
- [ ] Commit: `wire one session per runtime`

---

### Task 5: Model isolated tool workloads in Docker Compose

**Purpose:** Demonstrate one image/template instantiated as one process per tool without hand-copying
runtime implementation or embedding dynamic listener fields in the service definition.

**Files:**

- Modify: `docker-compose.yml`
- Modify: `demo/demo-contract-test.sh`
- Modify if needed: `demo/smoke-test.sh`
- Modify: `demo/consumer-safety-test.sh`
- Modify: `runtime/Dockerfile` only if the single-session startup contract requires it

**Compose contract:**

| Service | `SESSION_NAME` | Host status port | Container status port |
|---|---:|---:|---:|
| `listener-tool-a` | `tool-a` | `8081` | `8080` |
| `listener-tool-b` | `tool-b` | `8082` | `8080` |
| `listener-tool-c` | `tool-c` | `8083` | `8080` |
| `listener-tool-d` (`onboarding` profile) | `tool-d` | `8084` | `8080` |

- [ ] **RED:** Extend the shell contract test to fail until all three default runtime services exist, each
  exposes one distinct host port, each sets its matching `SESSION_NAME`, and the obsolete `listener-runtime`
  service is absent. Also require a `listener-tool-d` service under only the explicit `onboarding` profile,
  with `SESSION_NAME=tool-d` and host port 8084.
- [ ] Use one shared Compose anchor/template for build, config mount, NATS URL, sessions file, and process
  delay so per-tool declarations contain only identity/port differences.
- [ ] Keep `config/sessions.yaml` mounted as a stable file and keep subject/version/retry/drain fields out
  of the Compose service definitions.
- [ ] Update smoke assertions to query 8081/8082/8083 and prove each endpoint exposes only its own tool.
- [ ] Update the consumer-safety acceptance defaults to `listener-tool-a`/port 8081 while preserving its
  explicit service/status overrides; rerun it after the service/port cutover.
- [ ] Validate portability and syntax:

  ```sh
  for script in demo/demo-contract-test.sh demo/smoke-test.sh demo/consumer-safety-test.sh; do
    sh -n "$script"
    dash -n "$script"
  done
  demo/demo-contract-test.sh
  docker compose config --quiet
  ```

- [ ] Run the real smoke test, then the independent review gate.
- [ ] Commit: `run one tool per compose service`

---

### Task 6: Rewrite the one-person demo for process isolation

**Purpose:** Preserve the five useful demonstrations without claiming in-process onboarding/offboarding.

**Files:**

- Modify: `demo/demo-lib.sh`
- Modify: `demo/watch-status.sh`
- Modify: `demo/run-demo.sh`
- Modify: `demo/01-change-flow.sh`
- Modify: `demo/02-degraded.sh`
- Modify: `demo/03-isolation.sh`
- Modify: `demo/04-replay.sh`
- Replace or rename: `demo/05-onboarding.sh`
- Modify: `demo/README.md`
- Modify: `demo/demo-contract-test.sh`

- [ ] **RED:** Extend the contract test for an endpoint map (`tool-a`→8081, `tool-b`→8082,
  `tool-c`→8083), aggregate status output, service-specific logs, and the revised scenario-5 wording.
- [ ] Make `watch-status.sh` poll all three endpoints every 2 seconds by default and support a single
  snapshot. A 30-second interval may remain an operator option, not the demo default.
- [ ] Scenario 1: capture all a/b/c container IDs/start times plus tool-a durable creation identity; change
  only tool-a subject/version through the mounted config; wait for declared/applied convergence and admitted
  messages; prove all three containers are unchanged, b/c remain ACTIVE and advance, and the same tool-a
  durable/cursor was updated in place.
- [ ] Scenario 2: stop NATS for a bounded interval with a trap that always restores it. Poll all endpoints in
  one loop and remember whether each tool was observed DEGRADED so sequential waits cannot miss a transient;
  then verify every tool returns ACTIVE.
- [ ] Scenario 3: install EXIT/INT/TERM traps that restore `listener-tool-b`; stop only that service; prove
  tool-a/tool-c remain ACTIVE, their admitted counts advance, and their container IDs/start times do not
  change; restore tool-b and prove recovery. This validates process/Pod isolation rather than in-JVM
  session isolation.
- [ ] Scenario 4: first reach STANDBY, then capture admitted baseline and actual pending backlog; resume
  RUNNING and wait for `admittedCount >= standbyBaseline + capturedPending`.
- [ ] Scenario 5: install EXIT/INT/TERM cleanup before mutation; back up `sessions.yaml`; require tool-d and
  port 8084 to be absent; capture a/b/c identities; add/validate tool-d's desired entry; prove adding YAML
  alone still leaves port 8084 absent; explicitly run
  `docker compose --profile onboarding up -d listener-tool-d`; prove a/b/c identities did not change and
  port 8084 contains only tool-d. Cleanup must always stop/remove only the tool-d demo workload and restore
  the original config. It must not claim permanent offboarding or delete the durable. State clearly that
  production performs this workload operation through the controller.
- [ ] Ensure `demo/run-demo.sh` provides complete one-person `up`, status, scenarios, logs, smoke, and down
  commands, with Linux/POSIX-compatible shell.
- [ ] Run `sh -n` and `dash -n` in a loop over every demo script, then run the contract test and all five
  real scenarios from a clean stack using Linux/Ubuntu-compatible Docker tooling and the independent review
  gate.
- [ ] Commit: `update demo for process isolation`

---

### Task 7: Align normative docs and close the prototype acceptance loop

**Purpose:** Leave one authoritative production direction and a reproducible handoff.

**Files:**

- Modify: `docs/specs/2026-07-13-listener-session-nats-prototype-design.md`
- Modify: `docs/plans/2026-07-13-listener-session-nats-prototype.md`
- Modify: `docs/plans/2026-07-13-single-session-pod-refactor.md`
- Modify if needed: `README.md`

- [ ] **RED/document contract:** Search for normative claims that the initial production runtime is a
  multi-session ListenerCell, uses virtual threads for isolation, dynamically onboards by adding YAML, or
  deletes consumers when an entry disappears. Record each conflicting section before editing.
- [ ] Mark historical prototype sections as superseded by ADR-0001 and link to this refactor plan. Preserve
  history, but make it unmistakable that multi-session is not the initial production model.
- [ ] Document what the prototype now proves and what it deliberately does not prove: no CRD/controller,
  Lease fencing, finalizer cleanup handshake, UID-derived durable identity, or production workload
  generation yet.
- [ ] Run final verification from a clean demo stack:
  full Java tests, shell syntax/contract tests, `docker compose config --quiet`, real smoke, dynamic config
  no-restart proof, `demo/consumer-safety-test.sh`, outage recovery, isolated service failure, replay, and
  the profiled tool-d onboarding/controller-contract scenario including its cleanup.
- [ ] Run a final **new** independent reviewer over the complete ADR-to-code diff with extra-high rigor.
  The reviewer must check architecture conformance, accidental consumer deletion paths, one-session
  ownership, Linux portability, test evidence, and stale documentation.
- [ ] Update this plan's checkboxes/evidence without folding unrelated local files into the commit.
- [ ] Commit: `align docs with single session runtime`

## Explicit follow-up project: production controller

Do not pull these items into this refactor. Create a separate technical design and implementation plan for:

- `ToolListener` CRD, admission/defaulting, and status ownership;
- stable per-tool ConfigMap projection with UID/generation/revision/configVersion;
- deterministic UID-derived durable identity and collision/ownership validation;
- one-replica `Recreate` workload generation with no dynamic fields in the Pod template;
- Lease acquisition, renewal, fail-closed NATS fencing, and replacement handoff;
- finalizer-driven drain/delete acknowledgement and forced-orphan GC safety;
- controller-level status aggregation and static-field rollout tests.

## Completion evidence

The refactor is complete only when:

- three Compose runtime processes each expose exactly one tool;
- changing tool-a dynamic config preserves tool-a process/container identity and does not affect b/c;
- the tool-a subject/filter update preserves durable name, creation timestamp, and nondecreasing cursor;
- a selected durable mutation fails closed and creates no second consumer;
- stopping/crashing tool-b does not stop a/c processing;
- NATS loss degrades and recovers every runtime without killing its session thread;
- selected declaration disappearance cannot delete a consumer;
- `runtime/src/main` contains no consumer-delete API or NATS consumer delete call;
- all automated and real demo checks pass;
- every task and the final aggregate diff have independent-agent approval;
- all commits are local and no remote was pushed.
