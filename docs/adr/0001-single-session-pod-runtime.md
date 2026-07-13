# ADR-0001: One Pod, One Process, One ListenerSession

- **Status:** Accepted
- **Date:** 2026-07-13
- **Decision owners:** Listener runtime team
- **Supersedes:** The multi-session ListenerCell as the initial production deployment model

## Context

The existing prototype places multiple independently configured `ListenerSession` instances in one JVM.
Each session owns a virtual-thread actor loop, mailbox, NATS connection, retry lifecycle, drain state, and
status snapshot. A cell-level reconciler dynamically adds and removes sessions.

That model reduces Pod count and demonstrates a future bounded ListenerCell, but it makes the data-plane
runtime responsible for supervision that Kubernetes already provides: failure isolation, restart,
resource boundaries, scheduling, and workload lifecycle.

The initial production priority is stronger isolation and simpler failure reasoning. Expected tool count
has not yet demonstrated that Pod/JVM overhead is the dominant constraint. We also need upstream subject
and consumer changes to stop causing unnecessary Pod rollouts.

## Decision

### 1. Runtime deployment boundary

Each tool is represented by exactly one runtime workload:

```text
ToolListener declaration
        ↓
Controller-generated workload
        ↓
Pod
└─ one container
   └─ one process
      └─ one ListenerSession
```

The initial production runtime will not host multiple tool sessions in one process. A virtual thread may
still be used as an implementation convenience for blocking I/O, but it is not an isolation boundary or
a required architectural primitive.

### 2. Workload generation

Operators do not hand-maintain a Deployment manifest per tool. A controller consumes a declarative
`ToolListener` resource and creates or updates the workload from one shared template.

`ToolListener.spec` is the only operator-writable source of truth. Admission/defaulting validates the
persisted resource. `ListenerSpec` is not a second API resource: it is the controller's runtime projection
of `ToolListener.spec`, delivered through a stable, per-tool ConfigMap/projected file. Every projection
carries the `ToolListener` UID, generation, `configVersion`, projection revision, and any controller-owned
operation ID. The runtime watches that file, keeps the last good revision, and reports the observed UID,
revision, generation, and configVersion through its status endpoint. The controller owns writes to
`ToolListener.status` and copies runtime observations there. Updating the ConfigMap contents must not
change the Pod template hash.

The durable consumer name is controller-derived from cluster identity, stream identity, and
`ToolListener` UID (using a bounded deterministic encoding/hash). It is immutable for that resource
lifetime and is not operator-configurable. Admission rejects any projected asset identity already owned by
a different live UID.

Onboarding a new tool creates a new workload. It does **not** redeploy existing tools. Offboarding removes
only that tool's workload and consumer assets.

The controller implementation is a follow-up project. The current prototype will validate the
single-session runtime and the static/dynamic configuration boundary; Docker Compose will represent
several controller-generated workloads explicitly.

#### Offboarding protocol

`ToolListener` owns a finalizer. Deleting it does not immediately cascade-delete the workload:

1. The controller observes `deletionTimestamp`, creates a unique `terminationOperationId`, keeps the
   workload alive, and projects a termination request containing that ID, the `ToolListener` UID, exact
   stream/durable asset identity, and cleanup policy `DeleteConsumer`.
2. The runtime stops fetch/admission, drains in-flight messages, deletes that tool's durable consumer,
   and reports `CleanupComplete` with the same UID and operation ID.
3. The controller accepts only an exact UID/operation-ID acknowledgement, removes the workload after it,
   then removes the finalizer.

Normal process restart, Pod rollout, `STANDBY`, and `STOPPED` must retain the durable consumer. Only the
finalizer-driven permanent offboarding path may delete it.

Cleanup failure is retried and reported as `CleanupBlocked`; the finalizer remains. A timeout must not
silently orphan the consumer. An operator may explicitly force orphaning, after acknowledging that the
controller must later garbage-collect the recorded exact stream/durable asset. An orphan record retains
owner UID and termination operation ID; GC refuses deletion while any live declaration claims that exact
asset. Re-onboarding the same logical name creates a new UID and therefore a different durable identity.

### 3. Static workload configuration versus dynamic listener configuration

The workload template contains only settings that require a rollout:

| Static workload fields | Change behavior |
|---|---|
| Runtime image / application code | Roll out that tool Pod |
| CPU and memory requests/limits | Roll out that tool Pod |
| Service account, volumes, sidecars | Roll out that tool Pod |
| JVM startup parameters | Roll out that tool Pod |
| NATS endpoint and credentials in the initial production version | Roll out that tool Pod |
| Controller-derived durable consumer identity | Immutable for the ToolListener UID lifetime |
| Tool identity used to select its declaration | Normally immutable for the Pod lifetime |

Upstream and lifecycle settings live in a separately watched `ListenerSpec` and must not be copied into
the Deployment template:

| Dynamic ListenerSpec fields | Runtime behavior |
|---|---|
| `desiredState` | Converge through lifecycle state machine |
| NATS subject / consumer filter | Drain, reconnect, and resume in the same Pod |
| `configVersion` | Report declared/applied convergence without rollout |
| Retry policy and drain timeout | Apply as runtime configuration |

Changing a dynamic field must not mutate the Pod template or restart the process. The prototype uses a
plain watched file; production uses the controller-managed per-tool ConfigMap/projected file defined above.
Changing delivery mechanism requires a later ADR and must preserve the same UID/revision/last-good/status
contract.

The current `NATS_URL` and credentials are startup-only and explicitly static. Making either dynamic
requires a later decision plus an implementation that projects the referenced Secret/resource version,
triggers reconcile on rotation, and proves drain/reconnect behavior. A credential reference by itself is
not considered dynamically reloadable.

### 4. Runtime responsibility

The single-session runtime retains:

- the authoritative lifecycle state machine;
- NATS connection, durable consumer, fetch, ack, and reconnect behavior;
- retry classification and exhaustion;
- admission gating and at-least-once handling;
- drain and in-flight semantics;
- last-good configuration handling;
- one-session status and health reporting.

It removes from the production path:

- the map of sessions;
- cross-session supervision and aggregation;
- dynamic in-process onboarding/offboarding;
- terminating-session replacement and reaper logic;
- per-session thread isolation as a fault-containment mechanism.

### 5. Isolation and scaling

Kubernetes provides the primary isolation boundary. A tool crash, memory leak, rollout, resource change,
or retry exhaustion affects only that tool Pod.

Scaling is expressed as workload policy, not by running the same mutable consumer lifecycle from several
uncoordinated Pods. Any future active-active consumption must define durable-consumer ownership,
coordinated drain, and status aggregation before enabling more than one replica for a tool.

The initial contract is one replica per `ToolListener` with Deployment strategy `Recreate`. The controller
must wait until the old Pod is terminated before creating its replacement; `maxSurge` rolling overlap is
forbidden. This accepts brief per-tool rollout downtime in exchange for one durable-consumer lifecycle
owner. Graceful shutdown drains best-effort but retains the durable for the replacement process.

`Recreate` covers normal rollout but not node/control-plane partitions or failed-Pod replacement. Production
therefore also requires one Kubernetes Lease per `ToolListener` UID. A runtime may connect, fetch, ack, or
create/update/delete consumer assets only while it holds an unexpired lease. It must renew before expiry;
on API/renewal failure it stops admission, closes NATS, and fails closed before its local lease deadline.
A replacement waits for lease expiry and then acquires it before touching NATS. Consumer cleanup by a
controller recovery worker follows the same lease.

Force-deleting or replacing a Pod without observing the lease handoff violates the ownership contract and
is prohibited. Future active-active replicas require a new fencing/ownership design; `replicas: 1` alone is
not treated as fencing.

## Consequences

### Positive

- The runtime has one lifecycle owner and one NATS connection to reason about.
- Kubernetes supplies crash isolation, restart, scheduling, resource limits, and rollout behavior.
- Changes to fields classified dynamic above (subject/filter, desired state, version, retry, drain)
  no longer imply a Deployment change or Pod restart.
- Existing tools are unaffected when another tool is onboarded, offboarded, or rolled out.
- Health, logs, metrics, and status have an unambiguous tool identity.
- The controller eliminates duplicated hand-written Deployment YAML while preserving Pod isolation.

### Negative

- The cluster contains one Pod and usually one JVM per tool.
- A large tool population may increase scheduler, memory, image-start, and observability costs.
- Config-driven onboarding still creates a workload; it only removes the manual Deployment workflow.
- A production controller/control plane must be designed, implemented, and operated.
- Status aggregation across all tools moves to the controller or observability layer.

## Alternatives considered

### Multi-session ListenerCell in one JVM

This remains a valid future optimization if measured Pod/JVM overhead becomes material. It is not the
initial production model because it requires custom session supervision, assignment, cross-session
status, and multi-Pod consumer ownership before those costs are justified.

### Multiple tool processes inside one container

Rejected. Kubernetes cannot independently restart, resource-limit, probe, or scale child processes.
A custom PID 1 supervisor would recreate orchestration responsibilities inside the container.

### Multiple tool containers inside one Pod

Rejected as the general model. It is suitable for fixed, tightly coupled sidecars, but tool membership
would be encoded in the Pod template; onboarding or changing membership would roll out every tool in the
Pod and share scheduling/eviction fate.

### Manually maintained one-Deployment-per-tool manifests

Rejected. It preserves isolation but retains the operational duplication and timing coupling this work
is intended to remove. Workloads must be generated from declarations and a shared template.

## Migration plan

1. Preserve and finish the NATS fetch/ack error boundary fixes discovered by the real outage demo.
2. Extract or adapt the current runtime into a single-session controller with no session map/reaper.
3. Keep the current YAML schema temporarily, selecting exactly one entry through immutable tool identity;
   consider a single-object schema only after compatibility tests exist.
4. Change `/status` to report one authoritative session while retaining a compatibility envelope if demo
   and monitoring tools still depend on the existing JSON shape.
5. Change Compose to run one listener-runtime service per tool, each selecting one declaration and using
   a distinct host status port.
6. Rewrite demos to prove: upstream change without rollout, per-Pod fault isolation, replay, and
   controller-contract onboarding. Do not claim config-only in-process onboarding.
7. Mark the multi-session production model, P3 demo wording, and in-process onboarding sections in the
   existing prototype design/plan as superseded by this ADR; preserve them only as prototype history.
8. Define the `ToolListener` API, ConfigMap projection, UID-derived durable identity, Lease/fail-closed
   protocol, finalizer/status contract, `Recreate` workload template, and forced orphan recovery as a
   separate controller technical design.

Each migration step must use tests first, receive independent code review, and land as a local commit.

## Acceptance gates

The direction is not production-ready until automated tests prove:

1. Changing a dynamic field reaches the new applied `configVersion` while Pod UID, process start time,
   and Pod-template hash remain unchanged.
2. Changing a static field rolls out only the selected tool; other tool Pod UIDs and admitted counters
   remain stable.
3. A normal restart/rollout retains the durable cursor and never executes consumer deletion.
4. Offboarding holds the finalizer, drains, deletes exactly one durable consumer, reports
   `CleanupComplete` with the exact UID/operation ID, and only then removes the workload/finalizer. A stale
   acknowledgement from an earlier operation or same-name resource is rejected.
5. Normal `Recreate` rollout and node-partition/eviction replacement never produce overlapping NATS
   operations: the old runtime fails closed by lease expiry and the replacement acquires the lease before
   connect/fetch/ack/consumer mutation.
6. One tool's crash, retry exhaustion, rollout, and invalid spec leave other tool Pods active and
   processing.
7. Two declarations cannot claim one stream/durable identity. Orphan GC records exact asset + owner UID,
   refuses deletion of a live claim, and cannot delete a same-name resource's new durable.
8. Updating the controller-managed ConfigMap projection advances observed revision without changing Pod
   UID, process start time, or Pod-template hash.
9. Existing normative prototype design/plan sections are either updated or explicitly marked superseded,
   so only one production deployment model is authoritative.

## Revisit criteria

Reconsider a bounded multi-session ListenerCell only after measuring:

- expected and peak tool count;
- idle and active RSS/CPU per runtime Pod;
- NATS connection and consumer overhead;
- scheduler/control-plane impact of the projected Pod count;
- operational burden of per-tool logs, metrics, rollout, and autoscaling.

If those measurements show that one-Pod-per-tool cost is material, the existing multi-session prototype
and state-machine tests can inform a separately reviewed Cell supervisor. The optimization must not weaken
the dynamic configuration boundary established by this ADR.
