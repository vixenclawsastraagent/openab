# Tasks: Kubernetes session worker runtime and opt-in packaging

Status: approved by the feature owner on 2026-08-04.

Sources of truth:

- [approved worker-runtime specification](../specs/kubernetes-session-worker-runtime.md)
- [approved implementation plan](../plans/kubernetes-session-worker-runtime.md)
- [Kubernetes session isolation ADR](../adr/kubernetes-session-isolation.md)

The tasks are ordered by dependency. Each implementation task starts with the
listed failing behavior test where practical, makes only the smallest change
needed to pass, runs the exact focused checks, and creates the listed atomic
commit. The configured author and GitHub identity must remain
`vixenclawsastraagent` throughout.

## Execution rules

- Preserve the current worktree and branch; never replay the feature's history.
- Recheck `git status`, author identity, and the staged diff before every
  commit.
- Do not modify `Dockerfile.unified`, `charts/openab`, or local ACP behavior.
- Do not add production agent flavours, credential injection, CRDs, SQL,
  leader election, multi-controller coordination, or shared writable storage.
- Stop at each checkpoint. A failed checkpoint is fixed before the next task.
- Record any unrelated improvement as follow-up work rather than expanding a
  task.

Approval also freezes three intentionally boring MVP bounds that the approved
spec left symbolic: relay URLs are at most 2,048 UTF-8 bytes, a profile may
reference at most 16 image-pull Secrets, and child processes receive a
10-second TERM grace inside the Pod's existing 30-second termination grace.
Each value is a named constant with exact-limit and plus-one tests; changing it
later is a compatible configuration-bound adjustment rather than a protocol
change.

## Stage A: integrate the current upstream baseline

- [ ] **Task 1 — Merge `upstream/main` and compose the two runtime modes.**
  - Depends on: approved plan.
  - Work: fetch `upstream/main`; merge it with a conventional merge message;
    resolve only `crates/openab-core/src/acp/pool.rs` and `src/main.rs`; audit
    semantic auto-merges in Cargo, configuration, gateway, and connection
    code. Preserve both lifecycle/capacity state and MCP facade-token state.
    Keep facade registration, token minting, and broker-local facade config
    local-only. When `[mcp]` and strict Kubernetes session mode coexist, emit a
    clear broker-local limitation warning.
  - Test first: add or adapt a pool/composition regression proving strict mode
    never retains a facade registrar, mints a facade token, writes
    `.openab/mcp-facade.json`, or injects `OPENAB_SESSION_TOKEN`; retain the
    upstream local-mode positive tests.
  - Files: `crates/openab-core/src/acp/pool.rs`, `src/main.rs`, their focused
    tests, plus conflict-only merge results.
  - Acceptance: the merge has no unresolved markers; local mode retains all
    upstream MCP-over-ACP behavior; strict mode retains capacity, mapping
    repair, fencing, lifecycle cleanup, and zero facade credentials.
  - Verify:
    - `cargo test -p openab-core --features acp-mcp 'acp_mcp::'`
    - `cargo test -p openab-core --features acp-mcp 'acp::pool::'`
    - `cargo test --features acp`
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features`
    - `cargo clippy --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-targets --all-features -- -D warnings`
  - Commit: `chore(sync): merge upstream main`.

### Checkpoint A

Review the merge commit and compare `upstream/main..HEAD`. No worker-runtime
implementation starts until both local MCP behavior and strict-mode isolation
tests pass.

## Stage B: freeze and inject the worker transport profile

- [x] **Task 2 — Parse the revisioned relay and pull-secret contract.**
  - Depends on: Task 1.
  - Test first: extend `tests/profile_config.rs` with the valid example and
    rejection cases for non-WSS URLs, missing host, user information, query,
    fragment, wrong path, byte limit, unknown fields, malformed DNS names,
    excessive list length, and duplicates.
  - Work: introduce validating types for the exact `/v1/worker` URL, immutable
    CA ConfigMap name, and bounded deduplicated `image_pull_secrets`; add them
    to an immutable profile revision without changing absent add-on behavior.
  - Files: `src/profile_config.rs`, `tests/profile_config.rs`.
  - Acceptance: chat/session input cannot choose any transport field, and
    parser errors are stable and sanitized.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test profile_config`.
  - Commit: `feat(kubernetes): parse worker relay profile`.

- [x] **Task 3 — Resolve and pin the immutable CA ConfigMap.**
  - Depends on: Task 2.
  - Test first: extend `tests/profile_resolution.rs` for one bounded
    certificate-only `ca.crt`, immutability, UID/resourceVersion pinning,
    missing/mutable/deleting/wrong-key/oversized/non-certificate failures, and
    current-versus-historical revision behavior.
  - Work: add a CA-specific resolved type and resolver using the established
    immutable-skills lookup pattern without giving sessions ownership of the
    CA object.
  - Files: `src/controller/profile_resolution.rs`, supporting profile types,
    `tests/profile_resolution.rs`.
  - Acceptance: current bad trust configuration blocks startup; a bad
    historical revision degrades only that revision; no CA bytes enter session
    state or a registration Secret.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test profile_resolution`.
  - Commit: `feat(kubernetes): pin worker relay CA`.

- [x] **Task 4 — Render the exact worker Pod transport resources.**
  - Depends on: Tasks 2–3.
  - Test first: extend `tests/resource_builder.rs` with exact env, read-only
    CA volume/mount, `defaultMode`, pin annotations, and deduplicated
    `PodSpec.imagePullSecrets`; assert the generation Secret still contains
    only `token` and `binding.json`.
  - Work: carry the resolved transport through `MvpWorkerProfile` and
    `DesiredGeneration`, then render the fixed URL/CA paths and Pod-only pull
    references.
  - Files: `src/resources.rs`, profile/state types needed for propagation,
    `tests/resource_builder.rs`.
  - Acceptance: two sessions may reference the same immutable CA and skills
    objects but share no writable volume or ServiceAccount.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test resource_builder`.
  - Commit: `feat(kubernetes): render worker transport`.

- [x] **Task 5 — Revalidate CA identity at both consumption boundaries.**
  - Depends on: Task 4.
  - Test first: add generation and registration cases in which the CA name is
    replaced after resolution or becomes deleting; assert no Pod is created
    and no registration token is consumed.
  - Work: compare pinned name, UID, and resourceVersion immediately before Pod
    create/adopt and immediately before accepting worker registration.
  - Files: `src/controller/generation.rs`,
    `src/controller/registration.rs`, focused integration tests.
  - Acceptance: every drift path fails closed with a sanitized error and
    leaves controller intent recoverable.
  - Verify:
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test kubernetes_generation`
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test controller_registration`
  - Commit: `feat(kubernetes): fence worker CA drift`.

### Checkpoint B

Run the complete profile, resource, generation, and registration suites.
Inspect one rendered Pod and Secret fixture to confirm the trust bundle is
shared read-only while all mutable state remains session-private.

## Stage C: build the one-shot worker runtime

- [ ] **Task 6 — Extract the closed WSS client transport.**
  - Depends on: Task 1; may run in parallel with Tasks 2–5 after the profile
    field names and CA path are frozen.
  - Test first: add focused client-transport tests for exactly `/v1/bridge` and
    `/v1/worker`, native plus private roots, hostname/SNI verification,
    sensitive Authorization handling, frame limits, and rejection of
    plaintext, arbitrary paths, user information, query, fragment, bad PEM,
    and oversized inputs.
  - Work: move only CA loading, request construction, rustls connector setup,
    and WebSocket limits from the bridge into `client_transport.rs`; leave
    bridge activation and lifecycle behavior in place.
  - Files: `src/client_transport.rs`, `src/bridge/websocket.rs`, `src/lib.rs`,
    `tests/client_transport.rs`, existing bridge tests.
  - Acceptance: bridge argv and behavior without a private CA are unchanged;
    the helper accepts a closed endpoint enum rather than an arbitrary path;
    no kube/controller dependency enters the worker feature.
  - Verify:
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test client_transport`
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test bridge_websocket`
  - Commit: `refactor(kubernetes): share WSS client transport`.

- [ ] **Task 7 — Add the worker feature, CLI grammar, and bootstrap loader.**
  - Depends on: Tasks 2 and 6.
  - Test first: add parser/loader tests for `serve -- <absolute executable>`,
    the fixed environment, exactly 32 raw token bytes, at-most-4-KiB bare
    binding, bounded CA PEM, Pod UID, and redacted errors; include every
    boundary and plus-one case.
  - Work: add a `worker-runtime` Cargo feature, typed bootstrap module, and a
    thin binary entrypoint. Install latched termination handling before file
    or network work. Unsupported runtime targets fail explicitly.
  - Files: `Cargo.toml`, `Cargo.lock`, `src/worker/bootstrap.rs`,
    `src/worker.rs`, `src/bin/openab-kubernetes-session-worker.rs`,
    `tests/worker_bootstrap.rs`.
  - Acceptance: the feature compiles without kube/controller dependencies;
    secrets are not present in `Debug`, argv, or logs.
  - Verify:
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --features worker-runtime --test worker_bootstrap`
    - `cargo check --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --no-default-features`
  - Commit: `feat(kubernetes): load worker bootstrap`.

- [ ] **Task 8 — Prepare the private worker workspace.**
  - Depends on: Task 7.
  - Test first: cover missing directories, creation, existing real
    directories, non-directory entries, symlinks at every component, canonical
    escape attempts, ownership/writability failures, and exact HOME/workspace
    results.
  - Work: implement one-purpose workspace preparation beneath canonical
    `/session`; never accept a chat-selected or broker path.
  - Files: `src/worker/workspace.rs`, `tests/worker_workspace.rs`.
  - Acceptance: only `/session/home` and `/session/workspace` are exposed to
    the child, and every alias/escape fails before child spawn.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --features worker-runtime --test worker_workspace`.
  - Commit: `feat(kubernetes): prepare private workspace`.

- [ ] **Task 9 — Implement registration-first, one-shot activation.**
  - Depends on: Tasks 6–7.
  - Test first: use an in-memory/local TLS peer to prove Registration is the
    first text application frame, no child starts before ACK, Ping/Pong does
    not extend the 300-second deadline, and fatal result, ACP-before-ACK,
    duplicate result, malformed/binary/oversized frame, close, timeout, signal,
    and ambiguous send are terminal with zero reconnects.
  - Work: implement `build_worker_request` and
    `await_registration_ack`; zeroize token/header buffers after the handshake.
  - Files: `src/worker/registration.rs`, worker transport glue,
    `tests/worker_registration.rs`.
  - Acceptance: exactly one connection and one registration attempt occur per
    process; the ACP executable is not touched before a valid no-request-ID
    ACK.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --features worker-runtime --test worker_registration`.
  - Commit: `feat(kubernetes): register worker once`.

### Checkpoint C

Run the base, bridge, bootstrap, workspace, and registration tests together.
Review the dependency tree to confirm `worker-runtime` has no Kubernetes client
and the controller's 300-second activation ceiling is the sole deadline.

- [ ] **Task 10 — Add bounded ACP line and WebSocket framing.**
  - Depends on: Task 9.
  - Test first: cover LF/CRLF, empty input, malformed JSON, exact 64-MiB
    logical acceptance, plus-one rejection before allocation, split reads,
    FIFO ordering, child EOF, protocol-result/control-envelope leakage, and
    one-message-per-direction backpressure.
  - Work: implement the narrow bidirectional codec/relay adapters using the
    existing V1 ACP envelope and outer-frame limits; keep child stderr
    separate and supervisor stdout empty.
  - Files: `src/worker/relay.rs`, `tests/worker_relay.rs`.
  - Acceptance: no unbounded queue or ACP replay exists and both directions
    stop together on a terminal error.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --features worker-runtime --test worker_relay`.
  - Commit: `feat(kubernetes): relay bounded ACP messages`.

- [ ] **Task 11 — Supervise exactly one ACP process tree.**
  - Depends on: Tasks 8 and 10.
  - Test first: cover absolute executable enforcement, no shell, workspace
    cwd, sanitized child environment, process-group creation, child exit,
    TERM/KILL grace behavior, descendant cleanup, signal/socket/write races,
    reaping, and socket-drop-before-process-termination ordering.
  - Work: add injectable process/signal/clock boundaries for deterministic
    tests and the Linux process-group implementation. Never restart the child.
  - Files: `src/worker/process.rs`, `src/worker/supervisor.rs`,
    `tests/worker_supervision.rs`.
  - Acceptance: every terminal path leaves no child or descendant and performs
    no reconnect, registration resend, or local fallback.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --features worker-runtime --test worker_supervision`.
  - Commit: `feat(kubernetes): supervise ACP process tree`.

- [ ] **Task 12 — Add the deterministic fake ACP child.**
  - Depends on: Task 10.
  - Test first: assert request-ID preservation and exact responses for
    initialize, session/new, session/load, session/prompt, session/cancel, and
    close/release; assert workspace probes cannot address outside paths and no
    shell, arbitrary executable, model, or network behavior exists.
  - Work: add a feature-gated test binary with fixed content and only narrow
    read/write probes required by isolation tests.
  - Files: `src/fake_acp.rs`,
    `src/bin/openab-kubernetes-session-fake-acp.rs`, `Cargo.toml`,
    `tests/fake_acp.rs`.
  - Acceptance: output is deterministic and implements only the lifecycle the
    current bridge needs.
  - Verify: `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test fake_acp`.
  - Commit: `test(kubernetes): add deterministic fake ACP`.

- [ ] **Task 13 — Compose and test the complete worker process.**
  - Depends on: Tasks 7–12.
  - Test first: add a local private-CA WSS integration covering delayed ACK,
    fake-child startup, bidirectional ACP, bootstrap-variable removal, normal
    close, connection loss before/after ACK, and child failure.
  - Work: wire the thin binary through bootstrap, workspace, one-shot
    registration, process supervision, and relay; map failures to stable
    sanitized exit codes.
  - Files: worker composition root, `tests/worker_runtime.rs`.
  - Acceptance: child startup happens only after ACK and every connection loss
    produces zero retry/replay while fencing can observe socket closure.
  - Verify:
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features --test worker_runtime`
    - `cargo fmt --manifest-path crates/openab-kubernetes-session/Cargo.toml --all -- --check`
    - `cargo clippy --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-targets --all-features -- -D warnings`
  - Commit: `feat(kubernetes): run isolated worker runtime`.

### Checkpoint D

Run the complete standalone-crate all-feature test and clippy gates. Manually
review every secret lifetime, allocation boundary, child-start edge, and
terminal transition against the approved spec before packaging.

## Stage D: package the opt-in add-on

- [ ] **Task 14 — Build separate broker, controller, and worker images.**
  - Depends on: Task 13.
  - Test first: define clean-context build and binary-inventory smoke checks
    for four named targets: `broker`, `controller`, `worker-base`, and
    `worker-test`.
  - Work: add `Dockerfile.kubernetes-session` using the repository's pinned
    builder, non-root user, read-only-root-compatible layout, and tini pattern.
    The base worker contains no production ACP CLI; test adds only fake ACP.
  - Files: `Dockerfile.kubernetes-session`, narrowly scoped image test script
    if needed, `.dockerignore` only if additive and default-safe.
  - Acceptance: default image Dockerfiles are byte-for-byte unchanged and all
    four images contain only their intended binaries/runtime support.
  - Verify:
    - `docker build -f Dockerfile.kubernetes-session --target broker -t openab-session-broker:test .`
    - `docker build -f Dockerfile.kubernetes-session --target controller -t openab-session-controller:test .`
    - `docker build -f Dockerfile.kubernetes-session --target worker-base -t openab-session-worker:test .`
    - `docker build -f Dockerfile.kubernetes-session --target worker-test -t openab-session-worker-test:test .`
  - Commit: `build(kubernetes): add session images`.

- [ ] **Task 15 — Add a default-off controller chart with narrow RBAC.**
  - Depends on: Task 14.
  - Test first: render `enabled=false` and require no runtime resources; render
    enabled mode and assert the Deployment, ClusterIP relay Service, probes,
    config mounts, existing worker namespace/immutable CA references, and
    exact namespaced RBAC verbs. Assert the chart never grants exec, logs,
    port-forward, namespace/PV, Role, or RoleBinding mutation.
  - Work: create `charts/openab-kubernetes-session` without editing
    `charts/openab`; the chart owns controller deployment resources only, not
    dynamic session objects or operator trust data.
  - Files: new chart metadata, values, helpers, controller ServiceAccount,
    Role/RoleBinding, Deployment, Service, and focused render assertions.
  - Acceptance: disabled output is empty, relay is not publicly exposed, and
    uninstall cannot delete retained session PVCs/anchors or the CA object.
  - Verify:
    - `helm lint charts/openab-kubernetes-session`
    - `helm template test charts/openab-kubernetes-session --set enabled=true`
    - `helm unittest charts/openab-kubernetes-session`
    - `helm template test charts/openab`
  - Commit: `feat(kubernetes): add session controller chart`.

- [ ] **Task 16 — Render default-deny and explicitly allowed networking.**
  - Depends on: Task 15.
  - Test first: assert default-deny ingress/egress, DNS, worker-to-controller,
    and profile-approved service egress; assert no worker Service, public
    exposure, plaintext relay, wildcard external egress, or mutable shared
    volume.
  - Work: add controller and worker NetworkPolicies plus bounded values for
    approved service destinations. Document that enforcement requires a
    NetworkPolicy-capable CNI.
  - Files: add-on chart NetworkPolicy templates, values/schema/docs, render
    assertions.
  - Acceptance: structural policy is deterministic and cannot be expanded by
    chat content.
  - Verify:
    - `helm lint charts/openab-kubernetes-session`
    - `helm template test charts/openab-kubernetes-session --set enabled=true`
    - `helm unittest charts/openab-kubernetes-session`
  - Commit: `feat(kubernetes): restrict session networking`.

### Checkpoint E

Build all images from a clean context, inspect their user and binary inventory,
then inspect both disabled and enabled chart renders. Compare `charts/openab`
and all default Dockerfiles against the Stage A baseline.

## Stage E: prove isolation and lifecycle in Kind

- [ ] **Task 17 — Bootstrap a deterministic Kind add-on smoke test.**
  - Depends on: Tasks 14–16.
  - Test first: each missing prerequisite (Docker, Kind, Helm, kubectl, usable
    CNI) exits non-zero with a specific message; no isolation assertion may be
    silently skipped.
  - Work: add `scripts/test-kubernetes-session-kind.sh` to create a disposable
    cluster, build/load pinned test images, install the chart, create the
    operator-owned immutable CA and worker namespace, wait for probes, and
    clean up only harness-owned resources.
  - Files: Kind script and fixed harness fixtures.
  - Acceptance: a controller and one fake worker complete registration over
    private-CA WSS with no public Service or worker Kubernetes token.
  - Verify: `scripts/test-kubernetes-session-kind.sh --smoke`.
  - Commit: `test(kubernetes): bootstrap Kind isolation`.

- [ ] **Task 18 — Prove two-session writable-state isolation.**
  - Depends on: Task 17.
  - Test first: define deterministic failures for same Pod/PVC/SA, visible peer
    marker, writable shared skills, unexpected egress, missing resource limits,
    or an inbound worker Service.
  - Work: drive two logical thread fixtures through separate bridges; capture
    object names/UIDs; write distinct HOME/workspace/Git markers; prove each
    worker cannot discover or mutate the peer while both read the pinned skills
    and CA and call only harness-approved services.
  - Files: Kind script and constrained fixture endpoints.
  - Acceptance: Pods, PVCs, ServiceAccounts, process/resource boundaries, and
    writable state are distinct; shared platform data is read-only or a
    controlled service.
  - Verify: `scripts/test-kubernetes-session-kind.sh --isolation`.
  - Commit: `test(kubernetes): prove thread Pod isolation`.

- [ ] **Task 19 — Prove replacement, TTL suspension, and explicit release.**
  - Depends on: Task 18.
  - Test first: require logical identity and PVC marker retention across one
    failed-Pod replacement, then require bounded compute suspension and final
    deletion of only the released session's anchor/private PVC.
  - Work: extend the harness through replacement, idle compute TTL, storage
    retention, explicit close/release, and peer-session non-interference.
  - Files: Kind script and lifecycle fixtures.
  - Acceptance: there is at most one active Pod per logical session; storage
    persists until explicit release/retention policy and never leaks across
    sessions.
  - Verify: `scripts/test-kubernetes-session-kind.sh`.
  - Commit: `test(kubernetes): verify session lifecycle`.

### Checkpoint F

Archive the deterministic Kind output and Kubernetes object identifiers. A
passing test must include negative filesystem and network assertions plus
replacement, TTL, release, and peer-non-interference evidence.

## Stage F: contribution readiness

- [ ] **Task 20 — Document the opt-in deployment and deferred production work.**
  - Depends on: Tasks 15–19.
  - Work: document enablement, required immutable CA/namespace/image digests,
    one-thread/one-Pod invariant, trusted broker/controller boundaries,
    read-only shared skills and controlled services, ConfigMap MVP state,
    compute/storage TTLs, explicit release, rollback/disable behavior, and the
    fact that production agent flavours/credentials and enterprise state are
    follow-ups.
  - Files: add-on chart README and the smallest necessary operator docs; update
    the spec/ADR only when implementation evidence requires a clarification.
  - Acceptance: a user can enable or leave the add-on absent without assuming
    a default architecture change, and no document claims workspace selection
    alone is a security boundary.
  - Verify:
    - `rg -n 'enabled|immutable|read-only|TTL|release|ConfigMap|credential' charts/openab-kubernetes-session docs`
    - `helm lint charts/openab-kubernetes-session`
  - Commit: `docs(kubernetes): explain session add-on`.

- [ ] **Task 21 — Run the complete release and compatibility gates.**
  - Depends on: Task 20.
  - Work: run all root and standalone add-on checks; inspect the complete diff
    for secret leakage, broad RBAC, mutable/shared mounts, default-path edits,
    TODOs without issues, and deviations from the spec. Fix failures in small
    cause-specific commits and rerun the affected checkpoint.
  - Acceptance: every required gate passes without `--no-verify`, skips, lint
    suppression, or disabled tests.
  - Verify:
    - `cargo fmt --all -- --check`
    - `cargo check --workspace`
    - `cargo clippy --workspace -- -D warnings`
    - `cargo clippy --workspace --features unified -- -D warnings`
    - `cargo test --workspace`
    - `cargo test -p openab-gateway --features acp`
    - `cargo test -p openab-core --features acp-mcp 'acp_mcp::'`
    - `cargo test -p openab-core --features acp-mcp 'acp::pool::'`
    - `cargo test --features acp`
    - `cargo build --features unified`
    - `cargo fmt --manifest-path crates/openab-kubernetes-session/Cargo.toml --all -- --check`
    - `cargo check --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --no-default-features`
    - `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features`
    - `cargo clippy --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-targets --all-features -- -D warnings`
    - `cargo build --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --release --all-features`
    - `helm template test charts/openab`
    - `helm template test charts/openab-kubernetes-session --set enabled=true`
    - `helm unittest charts/openab-kubernetes-session`
    - `scripts/test-kubernetes-session-kind.sh`
  - Commit: none when clean; any correction uses its own conventional commit.

- [ ] **Task 22 — Final upstream sync and draft PR handoff.**
  - Depends on: Task 21.
  - Work: fetch `upstream/main`; merge new changes rather than rebasing the
    feature history; rerun affected and final gates; verify Git and active `gh`
    identities; push only to the `vixenclawsastraagent` fork; open an upstream
    draft PR containing the Review Contract, `Closes #1461`, the Discord
    discussion URL, exact validation evidence, residual risks, and explicit
    enterprise deferrals.
  - Acceptance: the fork branch is reproducible, the upstream diff excludes
    upstream's own history, the PR remains draft, and no deployment or default
    OpenAB behavior is changed outside the opt-in add-on.
  - Verify:
    - `git log -1 --format='%an <%ae>'`
    - `gh auth status`
    - `git diff --check upstream/main...HEAD`
    - `gh pr view --repo openabdev/openab --json isDraft,headRefName,body,url`
  - Commit: `chore(sync): merge upstream main` only if upstream advanced;
    otherwise no commit.

## Task review decision

Approval of this checklist authorizes Tasks 1–22 in order, including the
merge-first integration, focused commits, local container/Kind mutations, push
to the `vixenclawsastraagent` fork, and creation of an upstream **draft** PR.
It does not authorize a deployment to an existing cluster, mutation of the
upstream repository outside the draft PR, or any item listed under Ask First
or Never in the approved specification.
