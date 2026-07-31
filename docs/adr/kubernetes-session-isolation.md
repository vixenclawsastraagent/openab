# ADR: Kubernetes Session Isolation Runtime

- **Status:** Proposed
- **Date:** 2026-07-31
- **Author:** @vixenclawsastraagent
- **Tracking:** [Issue #1461](https://github.com/openabdev/openab/issues/1461)
- **Baseline:** `c5a75ac6e8fdc11a3b229a0c609769e90d261daf`

## 1. Decision Summary

Add an opt-in Kubernetes session-isolation mode in which one logical OpenAB
session owns at most one active worker Pod. Existing local ACP subprocess and
AgentCore behavior remain unchanged unless the new configuration is present.

This mode is an add-on, not a replacement architecture for OpenAB. OpenAB
continues to be a lightweight IM-to-ACP broker by default. Operators who
install the add-on and select it for a configured agent get the isolated
runtime; every other deployment and agent continues to use the current
architecture.

The first implementation will use:

1. a small, opt-in OpenAB core seam that gives the ACP bridge a stable,
   broker-scoped logical session identity before the child process starts;
2. a `kubernetes-acp` bridge process per active logical session;
3. one trusted session controller with the Kubernetes credentials required to
   reconcile session resources;
4. one worker Pod per active session, running the selected ACP/CLI behind a
   worker supervisor; and
5. native, namespaced Kubernetes resources as the initial persistent control
   plane, without introducing a CRD in the first version.

The bridge, controller, and worker supervisor will live in one focused Rust
crate with separate modes or binaries. `openab-core` will not depend on the
Kubernetes client, and `oabctl` will remain outside the dynamic session path.

The add-on requires a narrow core extension point because the bridge must
receive a stable session identity before ACP starts and must distinguish
non-destructive suspension from destructive deletion. That extension point is
dormant when the add-on is not selected. It does not move Kubernetes
reconciliation into OpenAB core or change the existing local execution model.

The security invariant is:

> A session's ACP/CLI process cannot enumerate, read, write, or delete another
> session's private writable state, even if it knows the other session's
> absolute paths.

This is enforced before ACP initialization and prompt 1 by separate Pod and
volume boundaries, not by prompts, skills, Git branches, working directories,
or individual agent tools.

## 2. Bounded First-Version Assumptions

The first implementation proceeds under these deliberately narrow
assumptions:

1. The first production-flavoured worker image may support one ACP agent
   variant, while the controller and relay remain agent-agnostic.
2. The integration test will use a deterministic fake ACP child so isolation
   tests do not depend on an external model or API.
3. The broker and session controller are trusted components. Worker Pods and
   their ACP/CLI processes are not trusted with Kubernetes API access.
4. A fresh session receives a private checkout inside its own writable volume.
   It never uses a Git linked worktree whose common directory is mounted from
   the broker or another worker.
5. Organization-managed skills may be image-baked or mounted read-only at a
   version-pinned revision. Generated skill state remains session-local.
6. The initial implementation rejects raw `[[ws:/broker/path]]` selection in
   Kubernetes session mode. A later version may reinterpret aliases as
   administrator-owned repository or runtime-profile identifiers.
7. The initial operational defaults are a 15-minute compute idle TTL and a
   72-hour resumable-storage TTL. Both remain operator-configurable.
8. The target cluster provides dynamic volume provisioning. End-to-end network
   isolation requires a CNI that actually enforces `NetworkPolicy`.

## 3. Context and Problem

OpenAB currently maps a Discord or Slack thread to a logical ACP session and
starts a local ACP subprocess for that session. A workspace directive can give
different sessions different current working directories, but the processes
remain inside the same long-lived OpenAB Pod and share its writable filesystem,
HOME, mounted PVC, cgroup, network namespace, and workload identity.

That arrangement is useful workflow separation but not an access-control
boundary. An ACP process in one thread can use an absolute path, a shell, an
MCP filesystem tool, or another CLI to reach another thread's worktree whenever
the shared Unix permissions allow it.

The organizational use case is a team-level bot serving several concurrent
AI-assisted development threads. The goal is not one bot per person. The goal
is to let one team use one centrally managed bot while ensuring that each
development session has its own runtime and mutable filesystem boundary.

## 4. Required Invariants

### 4.1 Session identity

- One chat thread maps to one logical OpenAB session.
- One logical session owns at most one active worker Pod.
- Kubernetes session isolation is selected per configured team/agent, not as a
  global mode for every OpenAB deployment or agent.
- The controller namespaces the session identity by a stable,
  operator-configured broker scope; `<platform>:<thread_id>` alone is not
  globally unique across bot deployments.
- Every lifecycle record, quota, retention deadline, and worker resource is
  charged to and isolated by that configured scope.
- The first version binds each opted-in scope to a dedicated worker namespace.
  The controller cannot create namespaces or mutate another scope's namespace.
- Each bridge connection also carries a new attempt identifier. Controller
  mutations are generation-fenced so a stale bridge or worker cannot delete,
  replace, or reconnect to a newer generation.

### 4.2 Private mutable state

Every worker receives private writable:

- HOME;
- checkout or standalone worktree and mutable Git metadata;
- ACP/CLI session state;
- scratch space and `/tmp`;
- session-specific configuration overlays;
- credentials or tokens assigned to that session; and
- mutable caches that can affect session behavior.

No other worker volume mounts these paths. Absolute paths from one worker do
not resolve to another worker's volumes.

### 4.3 Intentional sharing

Workers may consume:

- version-pinned, administrator-managed skills and policy files as read-only
  inputs;
- an approved model or API gateway;
- a source mirror that never exposes a session's private Git state;
- a dependency cache with explicit tenant namespacing; and
- an artifact service with authorization enforced at its API boundary.

Shared services may not reveal or mutate another session's worktree, HOME,
credentials, conversation state, or behavior-affecting mutable cache entries.

### 4.4 Runtime and identity boundary

- Worker Pods use distinct Pod UIDs, cgroups, network namespaces, resource
  limits, and ServiceAccounts.
- Worker Pods and their ServiceAccounts set
  `automountServiceAccountToken: false`.
- Worker ServiceAccounts receive no Kubernetes RoleBinding.
- Only the controller can create, observe, and delete session resources in the
  dedicated worker namespace.
- Worker ingress is denied. The worker establishes an authenticated outbound
  connection to the controller.

### 4.5 Backward compatibility

- Absence of the Kubernetes session configuration preserves current behavior.
- Existing `[agent]`, `[agentcore]`, workspace, session pool, and Helm defaults
  do not silently change.
- The Kubernetes session configuration is mutually exclusive with AgentCore
  and an explicitly configured local `[agent].command`.
- The default OpenAB binary and `openab-core` do not acquire a Kubernetes
  client dependency.
- Existing agent images do not gain a controller, worker supervisor, new RBAC,
  or background Kubernetes reconciliation.
- The existing OpenAB Helm chart renders no add-on Deployment, Service, RBAC,
  profile, quota, or NetworkPolicy. Those resources live in the separately
  installed add-on chart.
- A normal local ACP subprocess receives no reserved session-identity
  environment variables and no new lifecycle messages.
- Existing configuration remains valid without migration or newly required
  fields.
- Add-on startup or controller failures affect only agents configured to use
  the Kubernetes session mode.

### 4.6 Add-on activation boundary

The add-on has two explicit activation steps:

1. install its controller, RBAC, and one or more cluster-owned worker profiles;
2. select the Kubernetes session mode for an OpenAB agent through
   configuration.

Installing the controller alone does not change an OpenAB agent. Adding the
configuration alone fails fast with a clear connectivity or authentication
error if the controller is unavailable; it never falls back to a shared local
ACP process because that would silently weaken the requested isolation.

The first version installs one controller per scope's dedicated worker
namespace. Teams that do not install the add-on and select the mode create no
lifecycle records or worker resources. A future shared controller would need
an additional credential-to-scope authorization layer; knowing another
scope's opaque resource identifier must never grant access to it.

## 5. Architecture

```text
IM layer

 Discord / Slack
       |
       | message in thread A or B
       v

Trusted broker layer

+------------------------------------------------------------------+
| OAB broker Pod                                                   |
|                                                                  |
| thread -> logical session routing                                |
|                                                                  |
|  session A                     session B                          |
|  kubernetes-acp bridge A       kubernetes-acp bridge B            |
+--------------+-------------------------+-------------------------+
               |                         |
               | authenticated, ordered WebSocket relay
               | stable session identity + fenced attempt
               v                         v

Trusted control layer

+------------------------------------------------------------------+
| Session controller                                               |
| - sole holder of worker-resource Kubernetes RBAC                 |
| - ensure / suspend / replace / close / expire                    |
| - generation fencing and orphan reconciliation                   |
| - compute TTL and storage-retention TTL                          |
+--------------+-------------------------+-------------------------+
               |                         |
               | reconciles              | reconciles
               v                         v

Untrusted execution layer

+-----------------------------+  +-----------------------------+
| Worker Pod A                |  | Worker Pod B                |
| worker supervisor           |  | worker supervisor           |
| ACP/CLI A                   |  | ACP/CLI B                   |
| private HOME/PVC A          |  | private HOME/PVC B          |
| private checkout/.git A     |  | private checkout/.git B     |
| private /tmp A              |  | private /tmp B              |
| ServiceAccount A, no token  |  | ServiceAccount B, no token  |
+-----------------------------+  +-----------------------------+
               ^                         ^
               | read-only / controlled  |
               +------------+------------+
                            |
              +-------------------------------+
              | Approved shared resources     |
              | versioned read-only skills    |
              | model/API gateway             |
              | namespaced artifact/cache API |
              +-------------------------------+

Denied by the runtime:

  Worker A --X--> Worker B private writable state
  Worker B --X--> Worker A private writable state
```

### 5.1 OpenAB core seam

`SessionPool` already knows the logical session key before
`AcpConnection::spawn`, but the spawn API currently receives only global agent
configuration and a working directory.

The opt-in seam will:

- pass the logical session key to the bridge through a reserved child-process
  environment variable;
- prevent `[agent].env` from overriding that reserved value;
- keep a per-spawn attempt identifier in the bridge/controller handshake; and
- expose explicit lifecycle actions before the connection is dropped.

The core seam will use stable standard ACP lifecycle methods where available:

- `session/close` for non-destructive idle, capacity, or broker-shutdown
  suspension; and
- `session/cancel` for only the current in-flight turn.

ACP [stabilized `session/close` on 23 April
2026](https://agentclientprotocol.com/announcements/session-close-stabilized).
Capability support is detected by the presence of the
`sessionCapabilities.close` object, not a boolean value.

[`session/delete` remains a Draft ACP
RFD](https://agentclientprotocol.com/rfds/session-delete) at this baseline, so
destructive cleanup will not depend on it. The add-on bridge instead negotiates
a versioned `_openab/session/release` extension for destructive reset and
retention expiry. ACP reserves method names beginning with `_` for custom use.
The extension is sent only to a bridge that explicitly advertised the matching
OpenAB lifecycle capability; ordinary ACP agents never receive it. A future
change may adopt `session/delete` after it stabilizes without changing the
controller's internal destroy operation.

The pool must perform lifecycle work outside the global pool-state lock. A
lock-free control handle will carry the ACP stdin, pending-response channel,
session ID, and negotiated capabilities so a cancellation or bounded lifecycle
request does not wait forever on a streaming connection mutex.

For strict Kubernetes mode, a failure to persist a new session mapping is a
dispatch failure. It must not be logged and ignored after provisioning a
worker.

### 5.2 Session identity and resource names

The bridge receives:

- the logical session key from OpenAB; and
- a stable deployment/agent scope from trusted configuration.

It derives an opaque identifier from:

```text
sha256(scope || NUL || logical_session_key)
```

Only a DNS-safe truncated digest appears in Kubernetes names and labels. Raw
platform or thread identifiers are not written to Kubernetes resources or
controller/worker logs. Existing broker logs are outside this new guarantee
until separately migrated to opaque identifiers.

Each bridge/controller connection receives a random attempt ID. The controller
increments an explicit monotonic generation through a Kubernetes
`resourceVersion` compare-and-swap and accepts worker relay traffic only from
the expected Pod UID, generation, and one-time registration token.
`resourceVersion` is only an optimistic-concurrency precondition; it is never
used or exposed as the worker fencing generation. This follows the Kubernetes
[resource-version and conflict
semantics](https://kubernetes.io/docs/reference/using-api/api-concepts/).

### 5.3 Controller and native resource model

The first version will not require an `OABSession` CRD. For each retained
session, the controller creates a deterministic ConfigMap lifecycle anchor in
a dedicated namespace. The anchor records only non-secret control state:

- profile name and version;
- current generation;
- state;
- current Pod UID, when active;
- last activity time;
- compute deadline; and
- storage-retention deadline.

The private PVC, worker Pod, generation-specific registration Secret, and
tokenless ServiceAccount are owner-referenced to that anchor using its UID.
These dependents are namespaced; the ConfigMap cannot own the cluster-scoped
PV. Garbage collection is crash-recovery defense in depth, not the primary
cleanup algorithm, consistent with Kubernetes
[owner/dependent scope
rules](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/).

Destructive cleanup is ordered and idempotent:

1. compare-and-swap the anchor to `Deleting` and reject new prompts;
2. delete the observed worker Pod using a UID precondition;
3. wait until that exact Pod UID is gone;
4. delete the PVC using a UID precondition and observe finalizer completion;
5. delete the generation Secret and tokenless ServiceAccount; and
6. request anchor deletion using UID and `resourceVersion` preconditions; and
7. retain the session mapping until a subsequent read observes that exact
   anchor as absent.

The controller uses idempotent create-or-observe operations. A replacement Pod
is not created until the previous Pod UID is observed deleted. An
`AlreadyExists` response is followed by a read and validation of the full
session digest, anchor UID, and generation; a same-named object is never
adopted based on its name alone.

The initial ConfigMap store also validates every replacement as a legal domain
state successor before issuing its compare-and-swap. A current
`resourceVersion` therefore cannot be combined with a deserialized state that
changes an attempt within one generation, moves the generation backwards, or
skips a generation. The v1 anchor itself has no owner reference or finalizer;
either appearing through admission or drift is rejected because it could make
the anchor disappear unexpectedly or block ordered cleanup.

API-server write responses are validated rather than assumed to echo the
request. The store never removes an owner reference or finalizer that it does
not own, following Kubernetes'
[finalizer guidance](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/).
If admission adds either to an anchor, recovery fails closed and requires the
responsible controller or an operator who understands that lifecycle contract
to resolve it. The v1 ConfigMap anchor defines no OpenAB-owned finalizer.

If a newly created response is otherwise mutated, the store requests deletion
with the returned UID and `resourceVersion`. An accepted delete request only
means deletion has started; it does not claim that finalizer processing or
physical removal has completed. The controller must observe absence before
reusing the session identity or releasing retained state.

If admission mutates a replacement, the store makes one guarded canonical
repair only when the persisted `anchor.json` parses successfully and is
already exactly equal to the intended successor. The repair may correct the
store-owned envelope, such as required labels, extra data keys, `binaryData`,
or the mutable flag, but it never overwrites a different persisted state. A
newer generation, a different attempt in the same generation, or an
unparseable state therefore fails closed without a second PUT.

The repair uses the latest response as its base. The replacement
compare-and-swap uses the opaque `resourceVersion`; the UID is separately
checked as immutable object identity. A successful repair returns the repaired
observation as the committed result, so callers do not retry the stale write.
A conflicting, malformed, lifecycle-mutated, state-mutated, or repeatedly
mutated recovery is not retried or adopted: the failure remains observable for
reconciliation and operator action.

#### 5.3.1 Quick state-management solution for this version

The Kubernetes API is the state store for the initial add-on. This version does
not introduce SQLite, PostgreSQL, or another controller-owned database.

State is split by responsibility:

| State | Initial storage |
|---|---|
| Thread to outer ACP session mapping | Existing OpenAB broker persistence |
| Runtime profile, phase, generation, Pod UID, and TTL deadlines | Per-session ConfigMap anchor |
| Optimistic concurrency | Kubernetes `resourceVersion` |
| Worker fencing | Explicit monotonic generation, attempt ID, and Pod UID |
| Worker registration credential | Immutable per-generation Secret |
| Checkout, HOME, Git, and ACP/CLI files | Per-session PVC or ephemeral volume |
| Controller leader identity, when HA is introduced | One Kubernetes Lease per scope |

The ConfigMap contains small control-plane metadata only. It does not contain
prompts, source files, model output, repository credentials, or worker tokens.
The controller throttles activity timestamp updates so normal ACP streaming
does not generate one Kubernetes API write per message chunk. ConfigMap state
updates and child-resource creation are not transactional, so reconciliation
must safely resume from every intermediate state.

This solution is intentionally bounded to one Kubernetes cluster and dedicated
worker namespace per opted-in scope, a modest session fleet, and one controller
replica. It provides restart reconstruction and idempotent
reconciliation without operating another persistent service. Teams that do not
enable the mode have no ConfigMap anchors, session PVCs, Secrets, Leases, or
retention duties created on their behalf.

SQLite is not used as the canonical controller store. Running it in a
Kubernetes controller would add a controller PVC, backup and recovery duties,
single-writer or replication constraints, and another source of truth while
still requiring the controller to reconcile Kubernetes resources.

#### 5.3.2 Formal enterprise state management, deferred

The next formal step for a Kubernetes-local enterprise deployment is an
`OABSession` CRD and an HA controller/operator:

- `spec` holds desired profile and retention policy;
- `status` holds observed phase, generation, Pod UID, and conditions;
- finalizers enforce ordered Pod and PVC cleanup;
- Kubernetes Leases provide controller leader election;
- admission policy validates profiles and immutable security fields; and
- Events and metrics provide an auditable operational history.

The Kubernetes API and its etcd backing store remain the durable source of
truth in that design. A separate relational database is not automatically more
formal or reliable for Kubernetes resource lifecycle.

Leader election does not itself fence a stale controller. The formal HA design
must retain anchor compare-and-swap, explicit generations, Pod UID
preconditions, and immediate mutation shutdown on lost leadership.

A centralized API with PostgreSQL should be considered only when requirements
expand beyond one cluster, such as:

- cross-cluster or regional scheduling;
- organization-wide inventory and indexed reporting;
- long-term compliance audit independent of Kubernetes Events;
- global quotas, billing, or policy;
- disaster recovery across cluster loss; or
- several controllers coordinating against one global session namespace.

That enterprise control plane, its migrations, availability model, and
Kubernetes synchronization protocol are explicitly deferred from this change.

### 5.4 Relay contract

The relay is an ordered, bounded, full-duplex byte stream carrying ACP
JSON-RPC. It must preserve:

- request and response ordering;
- streaming notifications;
- agent-initiated requests such as permission requests;
- concurrent `session/cancel`;
- EOF and half-close behavior;
- bounded frame and queue sizes; and
- failure propagation to both peers.

The worker initiates the network connection to the controller. The controller
pairs it with the authenticated bridge connection only when the opaque session
identity, generation, Pod UID, and one-time token all match.

The controller may inspect the minimum ACP envelope required to distinguish an
in-flight `session/prompt` from idle traffic. It does not interpret prompts,
tool contents, or model output.

### 5.5 Worker contract

The worker supervisor:

1. validates its fixed, cluster-owned profile inputs;
2. starts the ACP/CLI using an argv vector rather than a shell command;
3. uses a fixed worker-local HOME and checkout path;
4. relays the ACP process's stdin and stdout;
5. terminates the child process tree on shutdown; and
6. never receives broker credentials or Kubernetes API credentials.

The worker Pod uses:

- `restartPolicy: Never`;
- a read-only root filesystem;
- a private PVC or an explicitly ephemeral volume;
- `ReadWriteOncePod` storage for the strict persistent profile, with a
  documented CSI prerequisite;
- a per-Pod `emptyDir` for `/tmp`;
- `runAsNonRoot`;
- `allowPrivilegeEscalation: false`;
- `RuntimeDefault` seccomp;
- all Linux capabilities dropped;
- explicit CPU and memory requests and limits;
- `activeDeadlineSeconds` as a controller-independent maximum Pod lifetime;
  and
- optional administrator-selected `runtimeClassName`.

The per-generation worker registration Secret has a new deterministic name
for each generation and is immutable. It is never updated in place, because a
Secret update could otherwise be projected into a stale but still-running Pod.
The worker namespace must also follow Kubernetes
[Secret security
practices](https://kubernetes.io/docs/concepts/security/secrets-good-practices/).

## 6. Configuration Contract

The first user-facing shape follows the existing AgentCore-specific precedent
instead of introducing a generic runtime framework:

```toml
[kubernetes_session]
controller_url = "wss://openab-session-controller.openab-system.svc/relay"
profile = "codex-strict"
scope = "team-a-openab-codex"
```

Presence of `[kubernetes_session]` is the application-level opt-in signal.
The add-on is installed separately, once per opted-in scope, into that scope's
dedicated worker namespace:

```bash
helm upgrade --install team-a-openab-session charts/openab-session \
  --namespace team-a-openab-workers \
  --create-namespace
```

The existing OpenAB chart continues to pass `configToml` through unchanged and
does not render controller RBAC, quotas, or network policy. The selected broker
uses an add-on-flavoured image containing the bridge binary. A broker without
the configuration uses an existing image and never contacts the controller.

The broker-to-controller credential is projected into the broker Pod as a
file; TOML contains only its path. The credential value is never written into
TOML, process arguments, or the worker Pod.

The named profile is cluster-owned and versioned. It selects:

- worker image and argv;
- private storage class, size, and retention policy;
- repository seed policy;
- resource requests and limits;
- worker ServiceAccount template;
- NetworkPolicy;
- read-only skill inputs;
- approved shared endpoints;
- compute and execution timeouts; and
- optional `runtimeClassName`.

Chat content cannot select an image, mount, Secret, ServiceAccount,
NetworkPolicy, runtime class, or arbitrary host/broker path.

## 7. Lifecycle and Cost Model

Compute and storage have separate lifetimes.

```text
Absent
   |
   | ensure(session)
   v
Provisioning ----failure----> Blocked
   |
   v
Ready <------prompt completes------ Busy
  |                                  |
  | prompt                           | cancel
  +----------------------------------+
  |
  | compute idle TTL / capacity suspension
  v
Suspended (Pod gone, PVC retained)
  |
  | next message before retention expiry
  v
Provisioning on the same PVC, with a new fenced generation

Suspended --retention expiry / destructive close--> Deleting --> Absent
```

Rules:

- Compute idle TTL deletes the worker Pod but retains resumable storage.
- A prompt marked in flight is not reclaimed by the ordinary idle TTL.
- A separate maximum execution TTL handles stuck or abandoned turns.
- Storage retention TTL begins or refreshes on meaningful session activity.
- Explicit non-destructive close removes compute immediately and follows the
  configured storage-retention policy.
- Explicit reset or destructive release removes compute and private storage
  only after generation-fenced controller acknowledgement.
- A broker or bridge crash is not interpreted as destructive deletion.
  Controller reconciliation suspends orphaned compute and retains storage.
- Cleanup failure is observable through logs, metrics, and Kubernetes Events.

Resource limits cap concurrent exposure but do not make TTL correctness
unnecessary:

- namespace `ResourceQuota` limits Pod count, PVC count, requested storage,
  Secret and ConfigMap count, CPU, and memory;
- a `LimitRange` supplies safe per-worker defaults;
- controller configuration limits active workers and retained sessions; and
- quota exhaustion rejects a new session clearly and fails closed.

`activeDeadlineSeconds` bounds one worker Pod's lifetime even while the
controller is unavailable. Kubernetes has no general age TTL for ConfigMaps,
Secrets, or PVCs, so retained-storage cleanup still depends on reconciliation,
monitoring, and operational cost alerts. ResourceQuota limits concurrent
resources, not how long their cost persists; Kubernetes' generic
[TTL-after-finished controller applies to Jobs
only](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/).

The persistent profile requires a dynamically provisioned StorageClass whose
PV reclaim policy is explicitly `Delete`. A `Retain` or unknown StorageClass
is rejected by the strict profile because deleting a PVC would not necessarily
delete its cluster-scoped PV or backing disk.

The profile also requires the documented
[`ReadWriteOncePod` access
mode](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes);
plain `ReadWriteOnce` can still allow multiple Pods on one node.

## 8. Threat Model

### Trusted

- OpenAB broker and its configuration;
- session controller;
- cluster administrators and runtime-profile maintainers; and
- the authenticated broker-to-controller channel.

### Untrusted or potentially compromised

- prompts and attachments;
- ACP/CLI and tools inside a worker;
- generated code and repository contents;
- MCP servers reachable by a worker unless explicitly trusted; and
- stale bridge or worker generations.

### Primary threats and controls

| Threat | Required control |
|---|---|
| Worker reads another session's paths | No shared writable volume; separate Pod mount namespace |
| Worker reaches Kubernetes API | No mounted SA token, no worker RBAC, egress policy |
| Worker impersonates another session | Per-generation token, Pod UID binding, authenticated relay |
| Stale worker reconnects | Explicit generation fencing, Pod UID binding, immutable per-generation token |
| Duplicate worker after timeout/retry | Deterministic identity, idempotent reconciliation, old UID deletion gate |
| Bridge dies and leaves costly resources | Controller-side reconciliation and independent TTL |
| Shared skills are modified | Versioned read-only image layer or read-only mount |
| Shared cache leaks state | Explicit API authorization and tenant namespace |
| Session creation succeeds but mapping persistence fails | Fail-closed persistence before prompt dispatch |
| Resource exhaustion from many threads | Quotas, controller caps, compute TTL, storage TTL |

## 9. Current Target List

This list is the implementation source of truth for the first reviewable
version:

- add-on packaging with explicit, default-off activation;
- per-team/agent scope ownership of lifecycle state, quotas, and retention;
- opt-in Kubernetes session configuration with unchanged local defaults;
- no Kubernetes dependency or reconciliation responsibility in
  `openab-core`;
- stable, scoped session identity available before ACP initialization;
- bounded lifecycle control for close, release, and cancel;
- authenticated bridge/controller/worker relay;
- controller reconciliation using native namespaced resources;
- Kubernetes-native quick state management with no new controller database;
- one private worker Pod, PVC, HOME, checkout, `/tmp`, Secret, and
  ServiceAccount per retained session;
- worker ServiceAccount token disabled and no worker Kubernetes RBAC;
- versioned read-only shared skills;
- separate compute and storage TTLs plus concurrent quota limits;
- one production-flavoured worker image and one deterministic test image;
- Helm resources for controller, RBAC, security policy, quota, and profiles;
- unit, relay, reconciliation, chart, and kind isolation tests; and
- configuration and operator documentation.

## 10. Deferred or Removed from the First Version

- a generic `SessionRuntime` trait covering local, AgentCore, and Kubernetes;
- an `OABSession` CRD and HA operator;
- SQLite, PostgreSQL, or another add-on-owned state database;
- multi-cluster or global enterprise session inventory;
- implementation of the agent-level `oabctl` Kubernetes runtime;
- support for every existing OpenAB agent image in the first PR;
- arbitrary user-selected `[[ws:path]]` inside the broker filesystem;
- shared Git common directories or linked worktrees across workers;
- cross-cluster scheduling;
- automatic scale-to-zero of the controller;
- service-mesh-specific identity;
- backup and disaster recovery beyond the documented PVC retention contract;
- treating bubblewrap alone as equivalent to Pod, cgroup, network, and
  workload-identity isolation; and
- broad refactoring of `SessionPool` unrelated to the lifecycle seam; and
- changing the current default OpenAB architecture, local ACP runtime, or
  AgentCore runtime.

## 11. Alternatives Considered

### A. Workspace directives only

Rejected. They select a current working directory but do not change filesystem
visibility or the shared Pod boundary.

### B. One bot deployment per team

Insufficient for this use case. It isolates teams from one another but leaves
concurrent threads within the same team's bot sharing one Pod and filesystem.

### C. Git worktrees in the broker PVC

Rejected as a security boundary. Linked worktrees share a common Git directory,
and processes in the same mount namespace can still reach sibling paths.

### D. Kubernetes client directly in `openab-core`

Rejected for the first version. It couples the thin broker to Kubernetes,
expands core dependencies, and makes local behavior harder to test. The
session identity and lifecycle seam is sufficient for an external bridge.

### E. Bridge directly creates Pods with no controller

Rejected. A bridge can be killed by process-group cleanup and cannot reconcile
its orphaned worker or reliably distinguish destructive reset, idle eviction,
hung recovery, and broker shutdown after it has exited.

### F. `OABSession` CRD from day one

Deferred. A deterministic ConfigMap anchor provides namespaced persistence,
optimistic concurrency, owner references, and restart reconstruction without
requiring a new cluster-wide API type. A CRD remains appropriate if status
conditions, admission, cross-controller integration, or richer querying later
justify it.

### G. Bubblewrap around every ACP process

Not selected for the full contract. A correctly configured, fail-closed
whole-process sandbox could provide a lighter filesystem-isolation profile,
but it does not naturally provide distinct Kubernetes cgroups, network
namespaces, resource limits, or workload identities, and nested user namespace
support varies by cluster.

### H. SQLite in the session controller

Rejected for this version. It would require its own persistent volume,
single-writer or replication policy, backups, schema migrations, and recovery
procedure. Kubernetes resources would still need reconciliation, so SQLite
would duplicate rather than remove control-plane state. It may remain useful
for local test fixtures, but not as the deployed source of truth.

### I. PostgreSQL control plane in the first version

Deferred. PostgreSQL becomes justified when the product needs multi-cluster
coordination, global queries, billing, durable compliance history, or
cross-cluster disaster recovery. Those are enterprise control-plane
requirements, not prerequisites for proving per-thread Pod isolation in one
cluster.

## 12. Implementation Tasks

- [ ] Task 1: Add the opt-in core session identity and lifecycle seam.
  - Acceptance: local defaults are unchanged; a fake bridge receives the
    correct stable key before `initialize`; reset, idle, capacity, hung, and
    shutdown paths have explicit tested semantics; non-add-on agents receive
    no new environment variables or lifecycle traffic.
  - Verify: focused `openab-core` unit tests, `cargo fmt`, `cargo clippy`,
    `cargo test`.
  - Files: `crates/openab-core/src/acp/`, `config.rs`, `dispatch.rs`,
    `src/main.rs`.

- [ ] Task 2: Implement the authenticated, bounded relay protocol.
  - Acceptance: bridge and worker can carry bidirectional ACP streaming,
    concurrent cancellation, agent-initiated requests, EOF, and backpressure;
    stale generation and bad tokens fail closed.
  - Verify: deterministic in-process relay tests and protocol fuzz/property
    cases for frame bounds.
  - Files: new focused Kubernetes session crate.

- [ ] Task 3: Implement controller state transitions and resource rendering.
  - Acceptance: repeated ensure is idempotent; at most one Pod UID is active;
    stale attempts cannot mutate the current generation; rendered resources
    satisfy the Pod, PVC, Secret, ServiceAccount, security, and ownership
    contract.
  - Verify: fake Kubernetes API tests or an injectable reconciler plus manifest
    snapshot tests.

- [ ] Task 4: Implement worker supervision and one agent flavour.
  - Acceptance: the worker starts the configured argv without a shell, relays
    ACP correctly, kills the child tree, and uses only worker-local paths.
  - Verify: fake ACP child integration tests and image smoke tests.

- [ ] Task 5: Add chart resources, images, and operator documentation.
  - Acceptance: Helm renders controller RBAC, token posture, profile,
    NetworkPolicy, quota, and image configuration from the separate add-on
    chart; the existing OpenAB chart remains unchanged.
  - Verify: `helm lint`, required `helm template` commands, chart tests, and
    image smoke tests.

- [ ] Task 6: Run kind isolation and lifecycle tests.
  - Acceptance: the minimum acceptance suite in Section 13 passes, including
    reverse A/B tests, restart, TTL, fencing, and quota failure.
  - Verify: reproducible kind script and CI job using a NetworkPolicy-capable
    CNI when network assertions are enabled.

## 13. Acceptance Tests

1. Start sessions A and B from different chat thread keys.
2. Confirm different Pod, PVC, ServiceAccount, Secret, Pod UID, and generation
   identities.
3. In A's actual ACP process, create uncommitted sentinel files in HOME and the
   checkout plus a local-only Git ref.
4. From B's actual ACP process, attempt filesystem discovery and direct
   `stat`, read, overwrite, and delete operations against A's recorded paths.
5. B receives `ENOENT` or `EACCES`, cannot discover the sentinel content or
   local-only ref, and cannot change A's hashes.
6. Repeat with A and B reversed.
7. Both workers can read the same pinned shared skill revision, but writes fail
   with `EROFS` or `EACCES`.
8. Compute idle TTL removes A's Pod while retaining its PVC.
9. A new message creates a new Pod UID on the same retained storage and resumes
   according to the ACP capability contract.
10. Storage expiry or destructive close removes the Pod, PVC, Secret,
    ServiceAccount, and lifecycle anchor; the backing PV is no longer billable
    when the StorageClass promises deletion.
11. A controller restart produces no duplicate worker.
12. A stale generation and invalid relay token are rejected.
13. Quota exhaustion refuses a new session without deleting live or retained
    session state.
14. Existing local and AgentCore configurations pass their original tests
    without new environment variables, subprocess messages, or Helm resources.
15. Rendering the Helm chart without session isolation produces no controller,
    worker RBAC, worker profile, quota, or NetworkPolicy resources.
16. An add-on controller outage fails a configured isolated session closed and
    never falls back to a local shared-filesystem ACP process.
17. A team/agent without `[kubernetes_session]` creates no lifecycle anchor,
    worker PVC, worker Secret, Lease, or retention record.
18. Two enabled scopes cannot read, mutate, close, or reconnect to each
    other's controller records or worker resources.

## 14. Verification Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release

helm lint charts/openab
helm template test charts/openab
helm template test charts/openab --set agents.kiro.enabled=false
```

The Kubernetes crate will add focused unit and integration commands once its
package name is fixed. The kind test must document its Kubernetes version,
StorageClass, and NetworkPolicy-capable CNI.

## 15. Open Questions

1. Which existing agent flavour should be the first production image: Codex,
   Kiro, or native `openab-agent`?
2. Should non-destructive `/close` retain storage by default for 72 hours, or
   should the default be immediate deletion with retention explicitly enabled?
3. What is the first supported repository seed mechanism: public Git URL,
   administrator-provisioned mirror, or a profile-owned credential provider?
4. Should workspace aliases be rejected entirely in the first version, or may
   aliases resolve only to cluster-owned profile/repository identifiers?
