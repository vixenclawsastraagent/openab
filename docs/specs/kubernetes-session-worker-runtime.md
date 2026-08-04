# Spec: Kubernetes session worker runtime and opt-in packaging

Status: approved by the feature owner on 2026-08-04.

This specification narrows the remaining implementation work under
[ADR: Kubernetes session isolation](../adr/kubernetes-session-isolation.md).
It does not reopen the controller lifecycle, fencing, state, or broker bridge
contracts that are already implemented and tested.

## Approved assumptions

1. One team may continue to use one OpenAB bot and one trusted broker Pod for
   many Discord or Slack threads. The broker receives IM events and performs
   routing, but it must not run any session's agent/CLI workload when
   Kubernetes session mode is selected.
2. Each logical thread session owns at most one active worker Pod. Different
   sessions never share writable HOME, workspace, Git metadata, PVC, process
   namespace, cgroup, or worker ServiceAccount.
3. A worker opens one outbound WSS connection to the controller. It has no
   inbound Service, Kubernetes API credential, RoleBinding, transparent
   reconnect, message replay, or fallback to local execution.
4. Worker transport is administrator-owned and revisioned with the worker
   profile. A profile supplies an exact `wss://.../v1/worker` URL and a
   versioned immutable CA ConfigMap in the worker namespace.
5. The CA ConfigMap is centrally managed, mounted read-only by many worker
   Pods, and pinned by name, UID, and resourceVersion. It is not copied into
   the single-use registration Secret and is not deleted with a session.
6. The worker supervisor waits for the controller's successful registration
   acknowledgement before starting the ACP child. Any ambiguous failure exits
   without reconnecting; the controller lifecycle creates a new fenced
   generation when recovery is safe.
7. The first repository-owned worker artifact is a generic supervisor plus a
   deterministic fake ACP worker for isolation tests. A production agent
   flavour can extend the pinned worker image without changing the protocol.
8. Images and Helm resources are packaged separately from OpenAB's default
   image and `charts/openab`. Enabling this mode is an explicit deployment and
   configuration choice; absent configuration preserves existing behavior.
9. ConfigMap anchors remain the MVP lifecycle state backend. A CRD,
   PostgreSQL, multi-controller coordination, and cross-cluster scheduling are
   enterprise follow-ups, not part of this change.

## Objective

Complete the missing worker side of the default-off Kubernetes session add-on
so one OpenAB bot can safely serve multiple concurrent thread-scoped AISDLC
sessions without placing their agents in a shared filesystem or Pod.

The trusted broker routes one thread through one ACP bridge. The controller
creates and fences one session-private worker generation. A small worker
supervisor authenticates to the controller, then relays ACP JSON-RPC between
that connection and exactly one child CLI inside the worker Pod.

The trust and information flow is:

```text
Discord / Slack
      |
      | one event for an exact thread
      v
+-------------------------------------------------------------+
| Trusted broker Pod                                          |
|                                                             |
| OpenAB                                                       |
|   +-- thread A --> bridge A --+                              |
|   +-- thread B --> bridge B --|-- outbound WSS /v1/bridge   |
|                                                             |
| No agent CLI or thread workspace in this Pod in this mode    |
+-----------------------------|-------------------------------+
                              v
                    +--------------------+
                    | Controller Pod     |
                    | routing + fencing  |
                    | Kubernetes RBAC    |
                    +--+--------------+--+
                       |              ^
       create/fence A  |              | worker outbound WSS
                       v              |
              +-----------------------+--+
              | Worker Pod A             |
              | supervisor <--> ACP CLI A|
              | private HOME/worktree/PVC|
              +--------------------------+

       create/fence B                 worker outbound WSS
                       |              ^
                       v              |
              +-----------------------+--+
              | Worker Pod B             |
              | supervisor <--> ACP CLI B|
              | private HOME/worktree/PVC|
              +--------------------------+

Worker A and B may both read a pinned skills ConfigMap and call explicitly
allowed model, artifact, or cache services. They never share writable mounts.
```

An IM event necessarily reaches the trusted OpenAB broker Pod. The isolation
boundary promised here is that no prompt or tool call is dispatched to a
shared agent runtime: after routing, each session's ACP traffic reaches only
its fenced worker Pod.

## Tech stack

- Rust 2021 in the independently versioned
  `openab-kubernetes-session` crate.
- Tokio 1 for process supervision, bounded asynchronous I/O, signals, and
  deadlines.
- rustls 0.22.4 with tokio-rustls 0.25 for standard hostname/SNI verification;
  the worker trusts only its mandatory controller-pinned private CA, with no
  custom or permissive verifier. The bridge retains its existing native-root
  behavior. tokio-tungstenite 0.21 owns framing only after the worker's strict
  zeroizing HTTP upgrade succeeds.
- Existing version-one OpenAB relay envelopes and ACP payload limits.
- kube 4.2.0 and k8s-openapi 0.28.0 for exact resource construction,
  observation, and UID/resourceVersion fencing.
- A separate multi-target Dockerfile, add-on Helm chart, and Kind test harness.
- ConfigMap lifecycle anchors and session-private PVCs; no new database.

## Configuration contract

Each immutable worker profile revision gains a required relay block and an
optional list of image-pull Secret names. For example, this profile fragment
shows only the new fields:

```toml
[profiles.codex.revisions.v1]
image_pull_secrets = ["ghcr-pull"]

[profiles.codex.revisions.v1.relay]
url = "wss://openab-session-controller.openab-system.svc:8443/v1/worker"
ca_config_map_name = "openab-session-controller-ca-2026-08"
```

The parser must reject unknown fields and reject relay URLs that are not WSS,
have no host, contain user information, query, or fragment components, exceed
the fixed byte limit, or do not use the exact `/v1/worker` path. The CA and
image-pull Secret names must be bounded Kubernetes DNS subdomains; lists must
be bounded and deduplicated.

The CA ConfigMap:

- lives in the fixed worker namespace;
- is immutable and has a versioned name that is never reused;
- contains one bounded, certificate-only `data["ca.crt"]` PEM bundle;
- is resolved at startup and pinned by name, UID, and resourceVersion;
- is revalidated immediately before Pod creation and worker registration;
- is mounted read-only at `/var/run/openab-controller-ca/ca.crt`; and
- has no session ownerReference or session-management labels.

Kubernetes volumes reference a ConfigMap by name rather than UID. Deployment
RBAC or admission policy must therefore deny update, delete, and same-name
recreation while a CA revision is referenced. The runtime pin detects drift
after resolution; the never-reuse policy preserves the same identity across a
controller restart or a suspended session with no live generation.

The generated Pod receives only fixed controller-owned variables:

```text
OPENAB_SESSION_CONTROLLER_URL
OPENAB_SESSION_CONTROLLER_CA_FILE
OPENAB_REGISTRATION_TOKEN_FILE
OPENAB_REGISTRATION_BINDING_FILE
OPENAB_WORKER_POD_UID
OPENAB_SESSION_ROOT
OPENAB_WORKSPACE
HOME
```

The 32-byte registration token remains only in the immutable generation
Secret. After verified TLS exists, it is hex-encoded exactly once while
constructing the sensitive HTTP Authorization header, zeroized before the
request write, never placed in argv or child environment, and never logged.
The complete worker-owned plaintext request and bounded response scratch are
also explicitly zeroized. This guarantee covers buffers owned by the worker;
it does not claim erasure of compiler, TLS-library, kernel, or network copies.
The Secret continues to contain only `token` and `binding.json`.

`image_pull_secrets` populate only `PodSpec.imagePullSecrets`. They are not
mounted, read by the controller, copied to the per-generation ServiceAccount,
or treated as ACP-visible credentials.

## Worker runtime contract

The new `openab-kubernetes-session-worker` binary is selected by the existing
profile-owned absolute supervisor executable. Its arguments use an explicit
separator followed by one absolute ACP child executable and its arguments:

```text
openab-kubernetes-session-worker serve -- /usr/local/bin/<acp-cli> <args...>
```

One worker process performs this sequence exactly once:

1. Install latched SIGINT and SIGTERM handling, then strictly parse its
   arguments and fixed environment contract. Read and validate the exactly
   32-byte raw token, at-most-4-KiB bare `WorkerRegistrationV1` binding, Pod
   UID, WSS URL, and at-most-256-KiB certificate-only CA PEM before starting a
   child.
2. Create or verify real directories at `/session/home` and
   `/session/workspace`; reject symlinks, non-directories, paths escaping the
   canonical `/session` root, and non-writable directories. The volume root
   may be owned by the storage driver, but each private child directory must
   retain the worker's exact effective UID and primary GID. New child
   directories use mode `0700`; conforming existing directories and their
   contents are preserved. Retain directory capabilities, revalidate their
   identity, ownership, and writability immediately before spawn, and change
   the child working directory through the retained workspace capability
   rather than by re-resolving a pathname.
3. Connect once with standard TLS hostname/SNI validation. Send exactly one
   HTTP GET for `/v1/worker` with one sensitive
   `Authorization: Bearer <64 lowercase hex>` header and one
   `x-openab-pod-uid` header. Do not send Origin, subprotocol, query, or
   user-information fields. Do not use tungstenite's client handshake
   serializer: the worker writes one bounded zeroizing plaintext request,
   accepts only a strict bounded HTTP/1.1 101 response, then hands the verified
   stream and any over-read tail to tungstenite framing. Worker builds cap the
   `log` facade at compile-time `INFO`, compiling tungstenite's DEBUG/TRACE
   payload and close-reason sites out of the binary.
4. Send one text `WorkerToControllerV1::Registration` envelope as the first
   application frame. Ping and Pong control frames may be handled while
   waiting, but no ACP child is running and no ACP frame is accepted yet.
5. Wait for at most 300 seconds, matching the controller activation ceiling,
   for exactly one no-request-ID
   `ControllerToWorkerV1::ProtocolResult` ACK. A fatal result, ACP-before-ACK,
   correlated result, invalid frame, timeout, close, or uncertain transport
   outcome is terminal and never retried. After this transition, the relay
   treats any further `ProtocolResult` as a duplicate protocol violation and
   terminates the process tree.
6. Ensure every worker-owned token/header/request buffer has already been
   explicitly zeroized, remove every transport/bootstrap
   variable from the child environment, spawn exactly one ACP child in its own
   process group, and relay newline-delimited ACP JSON-RPC over its piped
   stdin/stdout. Child stderr remains stderr; the supervisor never writes logs
   or framing data to stdout.
7. Apply existing ACP payload and outer-frame limits before allocation and
   keep both directions FIFO with bounded backpressure. The supervisor writes
   nothing to its own stdout; captured child stdout is treated only as ACP.
8. On child exit, Pod termination, WebSocket loss, protocol violation, or
   write deadline, stop both directions and drop the socket first so the
   controller fences the lane. Then terminate the complete child process
   group, wait a fixed grace period, kill and reap any survivor, and exit.
   Never reconnect or replay an in-flight ACP message.

The supervisor does not interpret prompts, select repositories, manage
sessions, call Kubernetes, or authorize shared services. Those responsibilities
remain with the existing ACP CLI, trusted profile, and controller.

## Commands

- Format:
  `cargo fmt --manifest-path crates/openab-kubernetes-session/Cargo.toml --all -- --check`
- Test:
  `cargo test --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-features`
- Lint:
  `cargo clippy --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --all-targets --all-features -- -D warnings`
- Minimal-feature check:
  `cargo check --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --no-default-features`
- Release build:
  `cargo build --manifest-path crates/openab-kubernetes-session/Cargo.toml --locked --release --all-features`
- Default Helm regression:
  `helm template test charts/openab`
- Add-on Helm render:
  `helm template test charts/openab-kubernetes-session --set enabled=true`
- Kind isolation test:
  `scripts/test-kubernetes-session-kind.sh`

The image, Helm, and Kind commands become mandatory once their corresponding
files exist. Tests that require a container runtime must fail with a clear
prerequisite message rather than silently skip isolation assertions.

## Project structure

- `crates/openab-kubernetes-session/src/worker/`: typed worker environment,
  workspace preparation, TLS request construction, handshake, stdio relay,
  supervision, and focused unit tests.
- `crates/openab-kubernetes-session/src/bin/openab-kubernetes-session-worker.rs`:
  thin worker composition root and sanitized process exit.
- `crates/openab-kubernetes-session/src/client_transport.rs`: narrow CA, WSS
  request, and bounded client-transport helpers shared by the existing bridge
  and new worker; this is not a generic runtime abstraction.
- `crates/openab-kubernetes-session/src/bin/openab-kubernetes-session-fake-acp.rs`:
  test-only deterministic ACP child with no shell execution.
- `crates/openab-kubernetes-session/src/profile_config.rs`: revisioned relay,
  CA ConfigMap intent, and image-pull Secret configuration.
- `crates/openab-kubernetes-session/src/resources.rs`: pinned CA representation
  and exact Pod environment, volume, annotations, and image-pull references.
- `crates/openab-kubernetes-session/src/controller/profile_resolution.rs`:
  startup resolution and pinning of the CA ConfigMap.
- `crates/openab-kubernetes-session/src/controller/generation.rs`: final CA pin
  validation before creation, adoption, and registration.
- `crates/openab-kubernetes-session/tests/`: resource, registration, transport,
  and end-to-end fake ACP tests.
- `Dockerfile.kubernetes-session`: separate broker, controller, worker-base,
  and worker-test image targets without changing default image targets.
- `charts/openab-kubernetes-session/`: default-off controller/RBAC/network
  policy/configuration add-on; it does not own or delete the worker namespace.
- `scripts/test-kubernetes-session-kind.sh`: deterministic two-session
  isolation, replacement, suspension, release, and cleanup verification.

## Code style

- Prefer small typed values with validating constructors over raw strings,
  maps, or boolean mode switches.
- Preserve existing `V1` wire messages, `deny_unknown_fields`, fixed limits,
  sanitized error enums, exact Kubernetes object validation, and feature
  gating.
- Gate process-group and signal code explicitly for the Linux worker runtime;
  unsupported host targets must fail clearly or use test-only adapters rather
  than silently weakening child cleanup.
- Keep transport authentication in HTTP headers and mounted files, never ACP
  JSON. Mark secret headers sensitive and redact `Debug` output.
- Make retry and lifecycle transitions explicit. An error must never select a
  local ACP process or a different worker generation as a fallback.
- Use boring single-purpose functions such as `load_bootstrap`,
  `prepare_workspace`, `build_worker_request`, `await_registration_ack`, and
  `relay_child`; do not introduce a generic runtime framework in this change.
- Tests describe behavior and use deterministic in-memory sockets, local TLS,
  fake processes, and Kubernetes API fixtures rather than implementation
  internals.

## Testing strategy

1. Profile and resource unit tests cover every accepted/rejected URL, CA pin,
   image-pull reference, exact mount/env/annotation, and absence of CA bytes
   from the bootstrap Secret.
2. TLS and request tests prove correct hostname/SNI/private-CA success and
   reject plaintext, untrusted CA, wrong hostname, duplicate headers, query,
   user information, and oversized inputs.
3. Worker state-machine tests prove registration is first, the child starts
   only after ACK, worker-first delayed pairing is bounded to 300 seconds, all
   fatal outcomes are terminal, and no connection or ACP message is retried.
4. Stdio tests prove CRLF/newline handling, exact boundary acceptance, plus-one
   rejection, malformed JSON handling, FIFO ordering, bounded backpressure,
   child EOF, child termination, and no control-envelope leakage.
5. Controller tests prove mutable/missing/deleting CA ConfigMaps fail closed,
   current-revision failure blocks startup, historical-revision failure
   degrades only that revision, and a UID/resourceVersion replacement observed
   after resolution prevents Pod creation or token consumption.
6. The deterministic fake ACP preserves request IDs; implements `initialize`,
   `session/new`, `session/load`, `session/prompt`, and `session/cancel` plus
   the existing OpenAB close/release capability; and exposes only narrow
   workspace read/write probes needed by the isolation test. It never runs a
   shell or arbitrary executable.
7. Kind tests start two logical sessions, observe distinct Pods/PVCs/SAs,
   attempt cross-session filesystem access, verify shared skills are read-only,
   replace one failed Pod without changing logical identity/PVC, then verify
   compute TTL and explicit release cleanup.
8. Existing OpenAB core, default image, and default Helm tests remain green.
   Any pre-existing unrelated failure is reproduced and reported, never
   bypassed or folded into this change.

## Boundaries

- Always:
  - preserve default behavior when `[kubernetes_session]` is absent;
  - keep one active worker Pod and one writable PVC per logical session;
  - use WSS with standard certificate and hostname verification;
  - pin administrator-controlled images and shared read-only inputs;
  - use tests before implementation and atomic conventional commits; and
  - keep worker Kubernetes credentials and inbound Services absent.
- Ask First:
  - change the default OpenAB image or `charts/openab` behavior;
  - add a production-specific agent CLI or credential-injection mechanism;
  - change the version-one wire schema or isolation invariant;
  - add CRDs, SQL, leader election, multi-cluster scheduling, or a generic
    `SessionRuntime`; or
  - introduce any shared writable filesystem, privileged container, sidecar,
    or additional Kubernetes authority.
- Never:
  - let chat content select controller URLs, CA objects, image pull Secrets,
    host paths, arbitrary workspaces, or executable paths;
  - mount a broker HOME/PVC or another session's PVC into a worker;
  - disable TLS verification, expose the relay publicly, or add plaintext
    fallback;
  - reconnect with a consumed or ambiguously consumed token, replay ACP, or
    fall back to a shared local process; or
  - log credentials, private file contents, raw prompts, Kubernetes response
    bodies, or panic payloads.

## Success criteria

- With Kubernetes session mode absent, broker argv, local ACP behavior,
  default images, and the existing Helm chart are unchanged.
- A valid profile deterministically creates a worker Pod with one private PVC,
  per-generation ServiceAccount, resource limits, default-deny networking,
  fixed WSS bootstrap variables, an exact read-only CA mount, and no
  Kubernetes token or inbound Service.
- Invalid relay/CA configuration fails before serving, and drift from the
  resolved CA UID/resourceVersion fails before ACP traffic or token
  consumption. Deployment policy forbids reusing a versioned CA name across
  restarts.
- The supervisor connects and registers once, starts exactly one child only
  after the delayed ACK, relays valid ACP bidirectionally under bounded memory,
  and terminates the child on every terminal path.
- Connection loss produces no reconnect or replay and lets the existing
  controller containment/replacement path preserve logical session identity
  and private PVC state.
- Two concurrent thread fixtures cannot read or mutate one another's HOME,
  Git metadata, workspace, or PVC, while both can read the same pinned skills
  object and reach only explicitly allowed services.
- The separate image targets build reproducibly, the add-on chart renders only
  when explicitly enabled, and the Kind isolation test passes.
- All repository-required format, clippy, test, release-build, image, chart,
  and applicable documentation checks pass without bypasses.

## Resolved decisions

1. The nine assumptions above are approved, including profile-revisioned
   relay/CA configuration and starting the ACP child only after the controller
   ACK.
2. The first contribution ships a generic worker-base image and deterministic
   worker-test image. A production Codex, Claude, or other agent flavour and
   its credential contract require a separately approved slice.
3. The add-on chart references an existing operator-owned immutable CA
   ConfigMap by default. Creating or rotating that platform trust resource is
   outside session lifecycle ownership.
