# Plan: Kubernetes session worker runtime and opt-in packaging

Status: approved by the feature owner on 2026-08-04.

Source of truth:
[approved worker-runtime specification](../specs/kubernetes-session-worker-runtime.md)
and [Kubernetes session isolation ADR](../adr/kubernetes-session-isolation.md).
This document describes component boundaries, dependency order, risks, and
verification checkpoints. The executable task checklist follows only after
this plan is approved.

## Baseline and integration decision

The feature branch is currently 78 commits ahead of and one commit behind
`upstream/main`. The upstream commit adds MCP-over-ACP browser control and
overlaps the Kubernetes isolation work in two composition surfaces:

- `crates/openab-core/src/acp/pool.rs`; and
- `src/main.rs`.

A three-way merge simulation reports only those two content conflicts. The
plan therefore merges `upstream/main` into the feature branch before new
worker work rather than rebasing 78 commits and resolving the same conceptual
overlap at multiple historical points. The merge preserves existing reviewed
feature commit identities; GitHub's upstream PR diff still excludes upstream's
own commit. A final upstream sync is repeated before opening the draft PR.

Conflict resolution must compose, not choose between, the two features:

- `PoolState` retains Kubernetes `lifecycle_handles` and upstream's
  feature-gated facade-token state;
- `SessionPool` retains strict Kubernetes capacity and upstream's
  feature-gated facade registrar/URL;
- every create, eviction, reset, suspend, and purge path preserves both
  lifecycle cleanup and exact facade-token revocation semantics; and
- root composition constructs the configured local/Kubernetes pool, applies
  upstream facade-session wiring only to the local runtime, and then places it
  in `Arc`.

Upstream's facade is deliberately colocated: it writes a broker-local
`.openab/mcp-facade.json`, injects `OPENAB_SESSION_TOKEN` into a locally spawned
agent, and points that agent at a loopback service. Kubernetes isolation runs a
trusted bridge on the broker and the agent in another Pod. Strict sessions
therefore receive no facade registrar, URL, config write, or token. Designing
an authenticated non-loopback facade for remote workers would require the
separately approved production credential/service contract.

The integration checkpoint runs upstream's new `acp-mcp` and root `acp` tests,
the affected pool tests, and all add-on tests before worker behavior changes.

## Component dependency map

```text
upstream integration
        |
        +--> profile relay/CA/pull-secret contract
        |             |
        |             +--> exact worker Pod resources
        |                         |
        +--> shared WSS client transport
                      |           |
                      +-----+-----+
                            v
                 worker registration state machine
                            |
                            v
                 child process + stdio supervisor
                            |
              +-------------+-------------+
              |                           |
              v                           v
       deterministic fake ACP      separate image targets
              |                           |
              +-------------+-------------+
                            v
                  default-off add-on chart
                            |
                            v
                    Kind isolation suite
                            |
                            v
                 full regression + draft PR
```

## Major components and implementation order

### 1. Integrate current upstream behavior

Merge `upstream/main`, resolve only the two known composition conflicts, and
verify both behavior sets before proceeding. Local sessions preserve upstream
facade token creation, revocation, and ACP tunnel registration. Strict
Kubernetes sessions preserve capacity, lifecycle handles, isolated bridge
selection, and their existing refusal to copy broker-local paths or credentials
into workers.

`pool.rs` combines `lifecycle_handles` with feature-gated `facade_tokens`, and
combines `strict_capacity` with feature-gated facade registrar/URL fields. It
retains the branch's mapping-load error, mapping-repair retry, strict
session/load retry, lifecycle gate, conditional mapping persistence, and
capacity commit. Local eviction/reset paths retain upstream's exact token
revocation. `src/main.rs` applies `.with_facade_sessions(...)` only when
`[kubernetes_session]` is absent and reports the broker-local limitation when
both `[mcp]` and Kubernetes isolation are configured. The pool builder itself
also refuses to retain facade wiring in strict mode, providing defense in depth
for future composition callers.

Checkpoint evidence:

- merge-tree conflict scope remains understood and no unrelated file is
  manually rewritten;
- semantic auto-merges in connection/config/Cargo/gateway surfaces are reviewed
  even though they have no textual marker;
- upstream `acp_mcp::`, `acp::pool::`, and root `--features acp` suites pass;
- a regression test proves strict Kubernetes sessions mint no facade token and
  write no broker-local facade file;
- standalone Kubernetes add-on all-feature tests and clippy pass; and
- the resolved diff has focused comments explaining only the composed state.

### 2. Add the immutable worker transport profile contract

Extend profile parsing with a typed relay URL, immutable CA ConfigMap intent,
and bounded deduplicated Pod image-pull Secret names. Resolve the CA object
using the existing immutable-skills pattern, but give it a separate type,
fixed `ca.crt` key, certificate-only validation, and annotations.

Propagate the resolved transport into `MvpWorkerProfile` and
`DesiredGeneration`. Generated Pods receive the exact URL and CA path, mount
the shared CA read-only, and reference pull Secrets only through
`PodSpec.imagePullSecrets`. The registration Secret remains exactly token plus
binding. Generation preflight and registration revalidate the CA UID and
resourceVersion; session cleanup never owns or deletes it.

Checkpoint evidence:

- parser tests reject every malformed or unbounded input and unknown field;
- resource snapshots prove exact env, volume, mode, annotation, and pull
  references;
- current-reference failure blocks startup while historical failure degrades
  only the affected revision;
- post-resolution CA replacement blocks Pod creation and token consumption;
  and
- two desired sessions share only the read-only CA/skills references, never a
  writable volume or ServiceAccount.

### 3. Extract a narrow shared WSS client transport

Move only reusable CA loading, strict WSS URI validation, sensitive request
header construction, rustls connector creation, and WebSocket limit setup out
of the broker bridge binary. Keep bridge activation/lifecycle logic and worker
registration logic in their own modules.

Represent `/v1/bridge` and `/v1/worker` as a closed endpoint choice so a caller
cannot smuggle an arbitrary path into otherwise shared request code.

The extraction must preserve byte-for-byte bridge argv when no private CA is
configured and preserve all existing bridge TLS/error behavior. It must not
introduce a generic runtime, retry client, reconnect policy, or alternate
transport.

Checkpoint evidence:

- existing bridge command, CA, request, and WebSocket tests remain unchanged
  in meaning and pass;
- shared helpers reject plaintext, wrong paths, user information, query,
  fragments, invalid roots, and wrong hostname/SNI;
- credential/header buffers are sensitive, bounded, and zeroized; and
- the `worker-runtime` feature does not enable kube or controller dependencies.

### 4. Implement the worker registration state machine

Add a Linux worker binary and worker library module that install latched
signals before file or network work, load the fixed bootstrap contract, connect
once, send Registration as the first text application frame, and wait up to
300 seconds for one no-request-ID ACK.

The worker does not start a child before ACK. Ping/Pong control frames are
allowed without extending the deadline. Fatal results, ACP-before-ACK,
duplicate results, invalid frames, close, timeout, and ambiguous send outcomes
are terminal. There is no same-generation reconnect, registration resend,
local fallback, or ACP replay.

Checkpoint evidence:

- exact token/binding/Pod-UID boundary and plus-one tests pass;
- a delayed worker-first pairing starts no child before ACK;
- all five sanitized fatal codes and every invalid handshake direction are
  terminal;
- cancellation at each startup boundary leaves no spawned child or retry; and
- full frame/message/write-buffer limits match the controller contract.

### 5. Implement workspace and ACP child supervision

Prepare only real `/session/home` and `/session/workspace` directories beneath
the canonical private mount, rejecting symlinks and escape paths. After ACK,
spawn exactly one absolute ACP executable without a shell, in its own process
group and with bootstrap/transport variables removed from its environment.

Relay one bounded logical message at a time in each direction between child
stdio and WSS. Natural backpressure preserves FIFO without an unbounded queue.
On any signal, child, socket, protocol, or write failure, drop the socket first
to trigger controller fencing, then TERM the child process group, wait a fixed
grace, KILL survivors, and reap them.

Checkpoint evidence:

- layout, ownership, writability, symlink, and escape tests pass;
- child stdout is ACP-only and stderr remains separate;
- exact 64-MiB logical boundaries, CRLF, malformed JSON, FIFO, and
  backpressure tests pass;
- child exit and signal races leave no process-tree survivor; and
- loss before and after ACK produces zero reconnects, restarts, or replay.

### 6. Add the deterministic fake ACP worker

Provide a test-only ACP child implementing the minimum OpenAB lifecycle needed
for repeatable tests: initialize, new/load session, prompt, cancel, and the
existing close/release capability. It preserves request IDs, returns fixed
content, and exposes only narrow workspace probes. It cannot run a shell or an
arbitrary executable.

Checkpoint evidence:

- ACP capability/schema tests match the bridge's lifecycle expectations;
- session load observes retained private PVC state after Pod replacement;
- workspace probes cannot address paths outside the worker workspace; and
- fake behavior is deterministic with no network or model dependency.

### 7. Package separate add-on images

Add a standalone multi-target Dockerfile for broker, controller, worker-base,
and worker-test artifacts. Reuse pinned builders and established non-root/tini
patterns without modifying `Dockerfile.unified` or default image targets.

The worker-base contains the supervisor but no production-specific ACP CLI or
credential contract. The worker-test adds only the deterministic fake ACP.
Every runtime image is pinned by digest when selected by a worker profile.

Checkpoint evidence:

- every target builds from a clean context;
- runtime images contain only their intended binaries and required CA/runtime
  support;
- worker containers run non-root with the declared read-only root layout; and
- default OpenAB image builds remain unchanged.

### 8. Add the default-off Helm add-on

Create `charts/openab-kubernetes-session` separately from `charts/openab`.
It deploys the controller, authenticated relay Service, probes, narrow RBAC,
network policies, configuration mounts, and references to an existing worker
namespace and immutable CA ConfigMap. `enabled=false` renders no add-on runtime
resources.

The chart does not create or delete the worker namespace, session anchors,
worker Pods, PVCs, per-generation Secrets/ServiceAccounts, or the operator's
CA object. Controller RBAC is namespaced and has no exec/log/port-forward,
Namespace, PV, Role, or RoleBinding mutation authority. Worker network policy
allows only explicit DNS, controller, and profile-approved service egress.

Checkpoint evidence:

- disabled render is empty for add-on runtime resources;
- enabled render passes Helm lint/template checks with exact RBAC verbs;
- no public Ingress, LoadBalancer, NodePort, plaintext relay, or worker Service
  is rendered;
- the existing OpenAB chart renders identically; and
- retained storage can be reclaimed only through controller-owned explicit
  release policy, not Helm uninstall.

### 9. Prove isolation and lifecycle in Kind

Build/load the test images, install the add-on, and drive two logical thread
fixtures through separate bridge processes. Assert distinct worker Pods,
PVCs, UIDs, cgroups/resource specs, ServiceAccounts, and writable workspaces.
Assert both can read pinned skills/CA and only approved services, while neither
can discover or mutate the other's private files.

Then fail one Pod, prove the replacement keeps logical session and PVC state,
suspend compute by TTL, explicitly release the session, and prove bounded
cleanup of its anchor and private storage without affecting the peer session.

NetworkPolicy structure is asserted from the rendered and live objects. Active
network enforcement is tested only through fixed harness endpoints with a
NetworkPolicy-capable CNI; the fake ACP worker does not gain arbitrary network
or shell execution merely to make the test convenient.

Checkpoint evidence is the deterministic script output plus captured object
names/UIDs and negative filesystem/network assertions. Missing Docker, Kind,
Helm, or kubectl prerequisites fail clearly rather than skipping the test.

### 10. Run final regression and prepare contribution

Fetch upstream again, integrate any new overlap, run all root and standalone
add-on quality gates, review the complete diff against the approved spec, and
push only with the `vixenclawsastraagent` identity. Prepare a draft PR with the
required Review Contract, `Closes #1461`, Discord discussion URL, validation
evidence, accepted residual risks, and explicitly deferred enterprise work.

## Parallelization

Safe parallel work after upstream integration:

- profile/resource tests and narrow client-transport extraction can proceed in
  parallel after env names, CA path, and profile schema are frozen;
- worker handshake tests and process-supervision adapters can proceed in
  parallel once the worker module interfaces and termination ordering are
  frozen;
- fake ACP behavior and image scaffolding can proceed in parallel after the
  worker CLI contract is frozen; and
- Helm template tests can be drafted while images build, but final values and
  Kind work wait for all image names, ports, and probes.

Required sequential boundaries:

- upstream integration precedes all new edits;
- resource injection and client transport precede end-to-end worker startup;
- handshake ACK precedes child spawn by invariant;
- working binaries precede final images;
- images and chart precede Kind; and
- Kind plus full regression precede push/PR.

No parallel worker may edit the same source files or commit shared worktree
changes without an explicit ownership handoff.

## Main risks and mitigations

| Risk | Mitigation |
|---|---|
| Upstream ACP/MCP state is lost while resolving conflicts | Compose both state sets and run upstream feature-gated tests before new work |
| Broker-local MCP facade credentials leak into isolated workers | Wire facade sessions only for local mode and regression-test zero mint/config injection in strict mode |
| CA name is replaced between resolution and use | Immutable versioned names, no-reuse admission policy, UID/resourceVersion annotations, pre-create and pre-registration checks |
| Registration outcome is ambiguous | Single attempt, ACK-before-child, no retry/replay, controller fencing and new generation |
| Worker-first lane waits forever and consumes a Pod | Fixed 300-second ACK deadline plus signal-aware cancellation |
| Agent escapes or aliases the private workspace | Canonical private root, reject symlinks/non-directories, no host/broker mounts |
| Backpressure becomes unbounded memory | Existing frame ceilings and at most one retained logical message per direction |
| Child descendants survive session loss | Dedicated process group, socket-drop-first, bounded TERM/KILL/reap |
| Shared resource becomes a writable isolation bypass | Only immutable read-only ConfigMaps or authenticated services; never shared writable volumes |
| Add-on changes existing deployments | Separate feature, binaries, images, chart, and absent-config regression tests |
| Idle Pods/PVCs grow without bound | Existing compute/storage TTLs, scope capacity, quotas, explicit release, and Kind lifecycle proof |
| Private images cannot pull without exposing credentials | Pod-level imagePullSecrets only; never mount or grant controller read access |

## Verification checkpoints

| Checkpoint | Required evidence |
|---|---|
| Upstream integration | root pool/MCP tests, root ACP tests, add-on all-feature tests/clippy |
| Profile/resources | parser, resource builder, profile resolution, generation and registration suites |
| Shared client transport | bridge regression plus TLS/request boundary suites |
| Worker handshake | registration-first, delayed ACK, fatal/timeout/cancellation and frame-limit suites |
| Process relay | layout, stdio, backpressure, signal and process-tree suites |
| Images | clean builds, non-root smoke tests, expected binary inventories |
| Helm | lint, disabled/enabled renders, exact RBAC/network-policy inspection, default-chart regression |
| Kind | two-session isolation, shared-read-only access, replacement, TTL and release |
| Final | format, no-default check, all-feature clippy/test/release, root workspace gates, diff/security review |

All commits remain small, independently verifiable, conventionally named, and
authored by the configured `vixenclawsastraagent` Git identity. Any neighboring
improvement discovered during implementation is recorded as a follow-up unless
new evidence shows the approved isolation contract cannot be met without it.

## Plan review decision

Approval of this plan authorizes the merge-first upstream integration strategy,
the ten-stage component order, and the listed parallel boundaries. It does not
authorize deferred production agent flavours, credential injection, CRDs,
databases, multi-controller operation, shared writable storage, or changes to
default OpenAB deployment behavior.
