# ADR: Kubernetes Session Isolation Add-on

- **Status:** Proposed
- **Date:** 2026-07-31
- **Author:** @vixenclawsastraagent
- **Tracking:** [Issue #1461](https://github.com/openabdev/openab/issues/1461)
- **Baseline:** `c5a75ac6e8fdc11a3b229a0c609769e90d261daf`

## 1. Context

OpenAB maps a Discord or Slack thread to a logical ACP session. With the local
runtime, each session has its own ACP subprocess and may use a different
working directory, but all subprocesses for one configured agent still run in
the same long-lived OpenAB Pod.

Different worktrees in that Pod are useful workflow separation, but they are
not an access-control boundary. The subprocesses still share the Pod's
filesystem namespace, writable HOME or PVC, cgroup, network namespace, and
workload identity. An agent in thread A can therefore use an absolute path,
shell command, MCP tool, or another executable to inspect thread B's worktree
whenever Unix permissions allow it.

The target use case is one centrally managed team bot serving several
concurrent AI-assisted development threads. Requiring one bot deployment per
person or per task would avoid some sharing, but would also duplicate bot
credentials, configuration, upgrades, and operations. The requested boundary
is instead one isolated execution environment per thread while retaining
intentional access to selected organization-managed resources.

Workspace directives and sandboxing remain useful defense-in-depth controls.
They do not satisfy this requirement because the boundary must hold before the
first prompt and for every executable available to the agent.

## 2. Decision

Introduce a separately packaged, default-off Kubernetes session-isolation
add-on.

When an agent selects this mode:

1. one chat thread maps to one logical OpenAB session;
2. one logical session owns at most one active Kubernetes worker Pod;
3. each worker owns its private writable HOME, checkout and Git metadata, ACP
   state, temporary files, credentials, and persistent volume;
4. workers may access only explicitly approved shared read-only inputs or
   authorized shared services; and
5. compute and retained storage are managed with separate lifetimes.

The non-negotiable security invariant is:

> A session's ACP/CLI process cannot enumerate, read, write, or delete another
> session's private writable state, even if it knows the other session's
> absolute paths.

Separate Pod and volume boundaries enforce this invariant before ACP
initialization. Prompts, skills, branches, worktree paths, and application-level
file checks are not treated as the isolation boundary.

This mode is an add-on rather than a replacement for OpenAB's architecture.
When its configuration is absent, local ACP and AgentCore behavior remain
unchanged.

## 3. Scope

The first version is intentionally bounded:

- one cluster;
- one trusted controller/relay instance and one dedicated worker namespace per
  configured scope;
- one controller replica;
- a small, administrator-owned set of worker profiles;
- one private persistent volume per retained session;
- a deterministic fake ACP worker for integration and isolation tests; and
- Kubernetes-native state rather than a new database or CRD.

### 3.1 Current MVP lifecycle target

- automatically persist non-destructive compute suspension only for a
  `Ready` session whose compute-idle deadline has expired;
- report that a `Suspended` session's storage deadline has expired as an
  advisory observation, without mutating or deleting its resources; and
- begin destructive release only after an explicit, fenced request.

### 3.2 Deferred lifecycle behavior

Automatic destructive release based only on storage-retention expiry is
deferred. A later opt-in policy may add it after operators have validated
retention, backup, reclaim-policy, observability, and recovery behavior. An
expired deadline is not deletion authority in the MVP.

A scope is the stable ownership boundary for one configured team or agent.
The controller combines it with OpenAB's logical session key so identical
Discord or Slack thread identifiers in different deployments cannot collide.

Each bridge activation also receives a fresh attempt identifier. Controller
operations are fenced by explicit generation, attempt ID, lifecycle-anchor
UID, and observed Pod UID. A stale bridge, worker, or retry cannot mutate or
delete a newer worker generation.

## 4. Architecture and trust boundaries

```text
IM layer

 Discord / Slack
       |
       | message in thread A or B
       v

Trusted broker Pod

+----------------------------------------------------------------+
| OpenAB                                                         |
| thread -> logical-session routing                              |
|                                                                |
| bridge A (ACP stdio)                 bridge B (ACP stdio)       |
+------------------+-----------------------------+---------------+
                   |                             |
                   | authenticated relay; scoped session + fence
                   v                             v

Trusted control plane

+----------------------------------------------------------------+
| Session controller + relay                                     |
| - sole holder of worker-resource Kubernetes RBAC               |
| - reconcile ensure / suspend / resume / release / expiry       |
| - rendezvous between each broker bridge and its worker         |
+------------------+-----------------------------+---------------+
                   |                             |
                   | creates and observes        |
                   v                             v

Untrusted execution Pods

+-----------------------------+  +-----------------------------+
| Worker Pod A                |  | Worker Pod B                |
| ACP/CLI A                   |  | ACP/CLI B                   |
| private HOME/PVC/.git/tmp A |  | private HOME/PVC/.git/tmp B |
| no Kubernetes API token     |  | no Kubernetes API token     |
+-----------------------------+  +-----------------------------+
                   |                             |
                   +-------------+---------------+
                                 |
                         read-only / authorized
                                 v
                +----------------------------------+
                | Approved shared resources        |
                | pinned read-only skills/policies |
                | model or API gateway             |
                | tenant-aware artifact/cache API  |
                +----------------------------------+

 Worker A --X--> Worker B private writable state
 Worker B --X--> Worker A private writable state
```

### 4.1 Broker and bridge

OpenAB remains responsible for IM routing and logical session identity. Its
core change is a narrow opt-in seam that supplies the bridge with a stable
logical key and a new per-spawn attempt ID before the ACP child starts.

The bridge remains an ACP stdio subprocess from OpenAB's perspective. It does
not receive Kubernetes RBAC. It authenticates to the controller/relay and
passes ACP traffic between OpenAB and the assigned worker.

The bridge is envelope-aware but is not a second ACP agent implementation. It
intercepts only the lifecycle and security-sensitive setup needed to:

- activate or resume the correct fenced worker;
- replace broker filesystem paths with the profile-owned worker directory;
- prevent broker-provided filesystem or MCP mounts from crossing the trust
  boundary;
- suppress broker-host filesystem and terminal capabilities, and reject any
  worker request that attempts to delegate those operations back to the
  broker;
- suspend compute without deleting retained state; and
- request explicit, fenced destructive release.

All other valid ACP requests, notifications, responses, and non-host-capability
agent requests pass through with bounded frames, messages, queues, and
timeouts. Filesystem and terminal work executes inside the worker Pod.

### 4.2 Controller and relay

The controller and relay are one trusted process in the initial design. A
separate controller is required because a short-lived bridge cannot reliably
reconcile TTL expiry, abandoned resources, or broker restarts. Combining the
controller and relay avoids an unnecessary extra service.

The controller:

- authenticates every bridge and worker connection to one configured scope;
- owns the Kubernetes permissions for that scope's worker namespace;
- creates, observes, suspends, replaces, and deletes worker resources;
- fences all mutations against the latest durable lifecycle anchor;
- reconciles intermediate states after restart; and
- enforces active-worker and retained-storage policy.

Knowing another session's resource name or session digest is not authorization
to access it.

The transport calls one controller-service facade rather than selecting domain
coordinators itself. That facade maps each configured profile name to exactly
one current, fully versioned profile for new sessions, derives activation
timestamps and deadlines from the controller's own policy, and exposes only
closed, sanitized failure codes. Existing sessions continue on the exact
profile revision pinned in their durable anchor, so a configuration update does
not silently migrate or strand retained state. Historical revisions may remain
loaded for that purpose. Activation and worker registration treat a session
identifier only as a routing hint: the composition root re-reads the lifecycle
anchor under the shared session lock, selects the exact profile revision stored
there, then performs a second authoritative read while validating the complete
activation or registration authority. A worker or transport cannot choose its
durable profile revision. Scope mismatches are rejected before profile lookup
or Kubernetes access.

### 4.3 Worker

Each worker Pod runs a small supervisor and one selected ACP/CLI process. The
worker opens an authenticated outbound connection to the relay, so it needs no
inbound Service and no Kubernetes client credentials.

Worker Pods use:

- separate Pod UIDs, cgroups, network namespaces, resource requests and
  limits, and ServiceAccounts;
- `automountServiceAccountToken: false` and no RoleBinding;
- a read-only root filesystem with private writable HOME and temporary mounts;
- a private volume that is never mounted into another active worker; and
- default-deny network policy plus explicit egress where the cluster CNI
  supports enforcement.

Organization-managed skills may be baked into the image or mounted read-only
at a pinned version. Any generated state, credentials, mutable configuration,
or behavior-affecting cache remains session-private.

## 5. State and fencing

The initial durable source of truth is one namespaced ConfigMap lifecycle
anchor per retained session. This is deliberately not SQLite: a controller
database would introduce another PVC, backup policy, availability boundary,
and source of truth before the operating model has been proven.

State responsibilities are separated:

| State | Initial owner |
|---|---|
| OpenAB logical-session mapping | broker storage |
| lifecycle phase, profile, deadlines, and fencing metadata | ConfigMap anchor |
| checkout, HOME, ACP state, and mutable files | private PVC |
| generation-specific bootstrap credential | immutable Secret |
| live execution identity and resource accounting | worker Pod |

The anchor never stores source code, prompts, model output, credentials, or
tokens. Kubernetes `resourceVersion` supplies compare-and-swap concurrency;
the explicit generation and identity fields supply domain fencing. They are
not interchangeable.

Malformed anchors, identity mismatches, stale fences, missing required
resources, and ambiguous ownership fail closed. Controller reconciliation must
tolerate every state between API operations and must use UID preconditions for
destructive deletion.

The controller persists `Deleting` before beginning destructive work, but that
durable intent is not a release acknowledgement. The broker removes its
mapping only after the controller has reconciled the exact PVC and generation
children to authoritative absence with selector LISTs and deterministic
GETs, deleted the exact anchor, observed it absent, and repeated a read-only
child-absence proof. If anchor deletion succeeds but the acknowledgement is
lost, the retry performs that post-anchor proof before returning `Released`.
A timeout, transport failure, process exit, or malformed acknowledgement is
never permission to discard the broker mapping.

Delete requests use both observed UID and `resourceVersion` preconditions, so
a stale observation receives a conflict instead of deleting a replacement
object. This follows the Kubernetes [Preconditions API](https://kubernetes.io/docs/reference/kubernetes-api/definitions/preconditions-v1-meta/).
The MVP additionally assumes the trusted controller is the sole writer of
managed worker resources in its namespace. Session locks serialize operations
inside the single controller replica; narrowly scoped RBAC prevents another
writer from recreating perfectly matching resources between proofs.

### 5.1 Active-worker capacity admission

`max_active_workers` is the admission threshold for one controller scope across
all worker profiles. Under the MVP's single-controller assumption, the
controller treats the lifecycle anchor as the durable reservation and counts
anchors as follows:

| Anchor state | Reserves an active-worker slot? | Reason |
|---|---|---|
| `Provisioning` | Yes | Reservation must precede Pod creation, including before `pod_uid` is known. |
| `Ready` | Yes | Worker compute is live. |
| `Busy` | Yes | Worker compute is live and processing a prompt. |
| `Suspending` | Yes | Compute absence has not yet been proven. |
| `Deleting` | Yes | Count conservatively until the anchor disappears because V1 lacks a durable compute-absent marker for every crash point. |
| `Blocked` | Only when `pod_uid` is present | A recorded Pod may still require cleanup; a clean blocked anchor must reacquire capacity before resume. |
| `Suspended` | No | Compute absence was proven and only retained storage remains. |

Only transitions that create a new reservation--an absent session or a
`Suspended` or clean `Blocked` session entering `Provisioning`--perform
admission. Retries of an existing `Provisioning` reservation continue even
when the limit is currently reached.

Within one controller scope, every profile shares one process-local admission
gate. The gate serializes a complete inventory LIST and the subsequent durable
anchor CREATE or compare-and-swap update; it is released before worker
provisioning. This keeps different profiles from independently admitting past
the scope-wide limit without holding the gate during Pod creation.

This is intentionally a single-controller MVP admission guarantee, not a
distributed hard quota. The add-on deployment must also configure a namespace
`ResourceQuota` as the cluster-enforced backstop for Pod and resource
consumption. That quota also covers an ambiguous Kubernetes write whose server
outcome is unknown after client cancellation or transport failure. The
process-local gate does not make multiple controller replicas safe. A future
multi-replica controller must add Lease-based leader election or equivalent
distributed coordination before relying on this admission contract.

If an operator lowers `max_active_workers` below the current reservation count,
existing reservations are grandfathered so their reconciliation and cleanup
can continue. The controller rejects every new reservation until the count
falls below the new limit; it does not evict or destructively release sessions
to satisfy the policy. Cleanup may conservatively move an already stopped
anchor into a counted state such as `Deleting` without admission; that can
temporarily raise the tally, but it does not create worker compute and must
never be blocked by the compute-cap policy.

## 6. Lifecycle and cost

```text
 Absent -> Provisioning -> Ready <-> Busy
                              |
                    idle / capacity / close
                              v
             Suspended (Pod gone, PVC retained)
                              |
                        next message
                              v
                       Provisioning

 Suspended -- explicit reset ---------------------> Deleting --> Absent
      |
      +-- retention expiry --> report deadline-expired observation (no mutation)
```

Compute idle expiry suspends the worker Pod but retains resumable private
storage. In the MVP, storage retention expiry is advisory: reconciliation
reports a stale-tolerant deadline observation but does not mutate the anchor,
PVC, or generation resources. The observation is not release authorization.
Only an explicit reset starts fenced, controller-managed destructive cleanup.
Broker shutdown follows the non-destructive path. Bridge or broker failure
leaves the session reconcilable; it never implies destructive release.

### 6.1 Unexpected connection loss and controller restart

Unexpected loss is not treated as a graceful close. Once the trusted relay
decides that an authenticated broker or worker lane is lost, it stops routing
that generation and submits the exact session binding and observed Pod UID for
durable containment. Under the shared session lock, the controller performs a
fresh anchor read and compare-and-swap transition from `Provisioning`, `Ready`,
or `Busy` to the recovery-only `Blocked` phase. Entering `Blocked` clears any
in-flight prompt-turn identifier. It never starts destructive release and
retains the lifecycle anchor and private PVC.

A delayed callback whose scope, session, generation, attempt, incarnation, or
Pod UID no longer matches is a stale observation and cannot affect the current
generation. The in-memory rendezvous registry must additionally assign a
unique connection ID to each installed lane. When a matching lane closes, the
registry atomically changes `Active(connection ID)` to `Quiescing(connection
ID)` before calling the controller; it must not briefly remove the slot and
allow another socket with the same binding to install. A transient persistence
failure keeps the slot quiescing and fail closed until retry succeeds.

One process-local rendezvous entry retains the exact binding and Pod UID plus
at most one bridge connection ID and one worker connection ID. The second lane
is paired only when that complete authority matches; a duplicate lane or
different generation never replaces an installed connection. Routing resolves
an exact peer only while the entry is active. A close transition returns a
retryable containment ticket containing both connection IDs, then releases the
registry mutex before any controller or socket I/O. Failed or cancelled
containment work remains discoverable from the quiescing registry; the entry is
removed only after a non-error durable-containment result. A delayed completion
ticket cannot remove a newer rendezvous.

Controller restart discards every process-local authenticated lane. Before it
admits relay traffic or reports readiness, the controller therefore lists all
anchors and schedules every observed `Provisioning`, `Ready`, and `Busy`
session for the same fresh-read `Blocked` transition. The LIST is only a hint;
each candidate is locked and re-read before mutation. LIST, GET, or CAS failure
keeps startup containment incomplete and readiness false. Once all candidates
are durably contained or freshly proven to have moved to a safe phase, normal
reconciliation may clean `Blocked` and `Suspending` compute and continue any
previously authorized `Deleting` intent. Readiness need not wait for Pod
deletion, because the old generation is already durably barred from routing.

The MVP deliberately provides no transparent same-generation reconnect. After
compute absence is proven, a newly spawned bridge uses a fresh attempt ID to
advance the clean `Blocked` anchor to the next generation and loads the
retained ACP session from its private PVC. An interrupted prompt is not
automatically replayed: its worktree may contain partial private changes, so a
human or higher-level workflow decides whether to retry.

`Released` proves that the Kubernetes PVC API object is absent; it does not by
itself prove that a backing PersistentVolume or cloud disk has been physically
destroyed. Whether and when backing storage is removed depends on the PV or
StorageClass reclaim policy, the CSI/provisioner implementation, and any
protection finalizers. Kubernetes documents these separate semantics under
[Persistent Volume reclaiming](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#reclaiming)
and [StorageClass reclaim policy](https://kubernetes.io/docs/concepts/storage/storage-classes/#reclaim-policy).
Operators that require physical deletion guarantees must select and verify a
compatible storage policy outside this controller's API-object proof.

This separation addresses the cost concern without weakening isolation:

- the [scope-wide active-worker admission policy](#51-active-worker-capacity-admission)
  and namespace `ResourceQuota` bound concurrent compute;
- an idle deadline bounds unused Pods;
- a storage-retention deadline identifies abandoned PVCs for release, while
  namespace storage quota bounds aggregate retained capacity;
- controller reconciliation handles resources left by crashes; and
- metrics and alerts expose active workers, retained storage, cleanup failure,
  and sessions blocked on policy.

Concrete TTLs and quotas are operator policy and belong in the worker profile
or chart values, not in this ADR.

For ACP lifecycle integration, `session/close` means non-destructive compute
suspension. ACP standardized that capability in April 2026. Destructive reset
uses a separately advertised OpenAB extension because ACP `session/delete`
does not guarantee Kubernetes Pod and PVC cleanup semantics. Ordinary ACP
agents never receive the extension.

See [ACP session close](https://agentclientprotocol.com/announcements/session-close-stabilized),
[ACP session delete](https://agentclientprotocol.com/announcements/session-delete-stabilized),
and [ACP extension naming](https://agentclientprotocol.com/protocol/v1/extensibility).

## 7. Activation and backward compatibility

Activation requires both:

1. installing the add-on controller, RBAC, profiles, policies, and images; and
2. selecting `[kubernetes_session]` for a configured OpenAB agent.

Installing the add-on alone does not change any agent. Selecting the mode
without a reachable, authenticated controller fails closed; OpenAB never falls
back to a shared local process because that would silently weaken the selected
isolation contract.

When `[kubernetes_session]` is absent:

- local ACP and AgentCore use their existing code paths and semantics;
- workspace directives keep their existing behavior;
- normal ACP children receive no reserved session identity or lifecycle
  extension;
- `openab-core` and the default image gain no Kubernetes client dependency;
- the existing Helm chart renders no add-on resources; and
- existing configuration remains valid without migration.

Kubernetes session mode is mutually exclusive with AgentCore and an explicit
local agent command. Raw `[[ws:/broker/path]]` directives are rejected in this
mode. Repository selection and worker paths are administrator-owned profile
inputs, not chat-controlled mounts from the broker Pod.

## 8. Threats and controls

| Threat | Required control |
|---|---|
| Worker reads another thread's worktree | separate Pod and private volume; no shared writable Git common directory |
| Worker obtains Kubernetes control | token automount disabled; no worker RoleBinding; RBAC held only by controller |
| Stale bridge replaces or deletes a newer worker | scope, generation, attempt ID, anchor UID, and Pod UID fencing |
| Crash leaves compute or disks indefinitely | durable deadlines, idempotent reconciliation, quotas, metrics, and alerts |
| Shared skills become a write channel | pinned read-only mount or image layer; generated state stored privately |
| Shared cache leaks another session | service API authorization and tenant namespace; never mount another worker's cache |
| Broker path is smuggled into worker setup | bridge-owned fixed working directory and rejected filesystem/MCP mounts |
| Controller outage weakens isolation | fail closed; never fall back to local shared execution |

## 9. Consequences

The add-on provides a kernel-enforced filesystem and runtime boundary while
allowing one team bot to serve many threads. Its Kubernetes implementation and
release artifacts can evolve independently behind a small broker seam.

The trade-off is an additional trusted service and explicit operational
responsibility for quotas, storage classes, retention, reconciliation,
credentials, networking, and observability. Suspended PVCs continue to cost
money until released or expired. A controller outage affects opted-in
sessions, though it does not alter default OpenAB deployments.

## 10. Alternatives considered

### Workspace directives, worktrees, bubblewrap, or one bot per team

These remain useful workflow or defense-in-depth tools. They do not establish
the requested per-thread Pod, volume, identity, network, and cgroup boundary.

### Kubernetes client in `openab-core` or in each bridge

Rejected. It would place RBAC in the broker or short-lived child, couple the
default runtime to Kubernetes, and leave TTL/orphan reconciliation without a
durable owner.

### One external Kubernetes job per incoming message

Rejected. A message is not the lifecycle boundary: an ACP session must preserve
conversation and development state across multiple turns while permitting
compute suspension.

### SQLite, PostgreSQL, or a CRD in the first version

Deferred. Namespaced ConfigMaps, Secrets, PVCs, and Pods are sufficient to
validate a modest single-cluster operating model. Adding a database or custom
API before that evidence would create premature operational machinery.

## 11. Deferred enterprise path

If production scale or policy requires it, the same lifecycle model can evolve
to:

- an `OABSession` CRD with `spec`/`status`, finalizers, admission policy,
  Kubernetes Events, and controller metrics;
- Lease-based leader election and multiple controller replicas;
- PostgreSQL for cross-cluster scheduling, global inventory, billing,
  compliance history, or disaster recovery;
- multiple worker clusters and richer administrator-owned profiles; and
- optional hardened `runtimeClassName` profiles such as gVisor or Kata.

Those changes must retain compare-and-swap updates, explicit generation and
attempt fencing, UID-preconditioned deletion, private writable state, and
fail-closed activation.

The first implementation does not introduce a generic `SessionRuntime`
abstraction. That interface should be extracted only after Kubernetes and at
least one other runtime demonstrate the same stable lifecycle contract.

## 12. Open policy decisions

Implementation may proceed with deterministic test profiles while maintainers
decide:

1. the first supported production worker flavour and image ownership;
2. how a profile seeds a private repository checkout;
3. default compute and storage retention policy;
4. supported storage classes and access modes; and
5. whether a later shared controller should serve multiple authenticated
   scopes.
