# ListenerSession Prototype — Single-Operator Demo

This runbook lets one person operate the whole demo with two terminals. Each tool runs as its own
process/container (ADR-0001: one Pod, one process, one `ListenerSession`) on its own host status port:

| Tool | Service | `/status` endpoint |
|---|---|---|
| tool-a | `listener-tool-a` | http://localhost:8081/status |
| tool-b | `listener-tool-b` | http://localhost:8082/status |
| tool-c | `listener-tool-c` | http://localhost:8083/status |
| tool-d | `listener-tool-d` (onboarding profile) | http://localhost:8084/status |

Each endpoint reports **only its own tool**. `demo/watch-status.sh` polls all of them and merges the
rows into one table.

The scenario scripts drive their own `config/sessions.yaml` and Docker edits so a run is deterministic
and repeatable, and each restores what it changed through an EXIT/INT/TERM trap. Pauses are observation
checkpoints; set `DEMO_AUTO=1` to run a scenario unattended (skips the prompts, still does every edit,
wait, and assertion). This automation is what lets the same scenarios serve as CI-style checks.

The prototype is intentionally **single replica per tool**. Multi-Pod consumer ownership and coordinated
drain belong to the production controller design, not here.

## Prerequisites

- Docker with Compose v2
- `curl` and `jq`
- Optional: `column` for aligned status output

Run the automated confidence checks first:

```sh
demo/demo-contract-test.sh
demo/smoke-test.sh
```

## Terminal layout

Terminal A starts the stack and stays on the live dashboard:

```sh
demo/run-demo.sh up
demo/run-demo.sh status
```

Terminal B runs one scenario at a time:

```sh
demo/run-demo.sh 1
DEMO_AUTO=1 demo/run-demo.sh 1   # unattended
```

You can also use the interactive launcher:

```sh
demo/run-demo.sh menu
```

## Recommended full-demo order

### 1. Safe upstream subject change without a rollout

```sh
demo/run-demo.sh 1
```

Changes only tool-a's subject/version (`tool.a.events` → `tool.a.events.v2`, `v1` → `v2`). Proves all
three tool containers keep their IDs and start times, tool-a's durable is updated **in place** (same
name/creation, cursor not reset), and tool-b/tool-c stay `ACTIVE` and keep advancing.

### 2. Dependency failure without process crash

```sh
demo/run-demo.sh 2
```

Stops NATS briefly. A single combined poll loop records that every tool was observed `DEGRADED`, then
proves all recover to `ACTIVE` with their containers intact. The outage is kept short — under tool-b's
~50s retry-exhaustion window (5s × 10 attempts).

### 3. Process/Pod isolation

```sh
demo/run-demo.sh 3
```

Stops the entire `listener-tool-b` container. Proves tool-a/tool-c stay `ACTIVE`, keep advancing, and
never change their container IDs/start times. tool-b then restarts and recovers from its durable. This
validates process/Pod isolation — a separate process, not in-JVM session isolation.

### 4. STANDBY replay

```sh
demo/run-demo.sh 4
```

Pauses tool-a with `STANDBY`, lets a real backlog build on the durable, captures the admitted baseline and
pending backlog, then resumes `RUNNING` and waits for `admittedCount >= baseline + capturedPending`.

### 5. Onboarding as a workload operation

```sh
demo/run-demo.sh 5
```

Onboarding a tool creates a **new workload**, not an in-process config add. The scenario adds tool-d's
declaration, shows that YAML alone leaves port 8084 absent and tool-a/b/c untouched, then explicitly runs
`docker compose --profile onboarding up -d listener-tool-d` and proves tool-a/b/c are not redeployed while
`:8084` exposes only tool-d. Cleanup removes **only** the tool-d workload and **retains its durable** — it
does not offboard permanently and does not delete a consumer. In production the controller performs this
workload operation from a `ToolListener` declaration; only the finalizer-driven offboarding path deletes a
durable.

## Useful commands

```sh
demo/run-demo.sh once            # one aggregate status snapshot
demo/run-demo.sh logs tool-a     # tail one tool's process logs
demo/run-demo.sh logs            # tail all three tool processes
docker compose ps
docker compose logs --tail=100 upstream-publisher
```

To stop the stack and delete demo JetStream data:

```sh
demo/run-demo.sh down
```

## Troubleshooting

- A tool endpoint is unreachable: run `demo/run-demo.sh up`, then `demo/run-demo.sh logs tool-a`.
- A session stays `DEGRADED`: check its `reason`, the NATS container, and whether its subject overlaps
  stream pattern `tool.>`.
- A host port (8081/8082/8083/8084 or 4222) is busy: stop the conflicting process; the ports are fixed.
- Scenario left config edits behind after a hard kill: each scenario restores on exit, but if one was
  `kill -9`'d, run `git restore -- config/sessions.yaml`.
