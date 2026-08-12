# OpenAB Kubernetes session controller

This chart installs the trusted controller side of OpenAB's optional
per-session Kubernetes worker mode. It is disabled by default and is separate
from `charts/openab`, so installing or upgrading the existing OpenAB chart does
not enable session workers or change local ACP behavior.

When enabled, this chart owns only:

- one controller ServiceAccount in the Helm release namespace;
- one controller Deployment and private ClusterIP relay Service in that
  namespace; and
- one narrowly scoped Role and RoleBinding in an existing worker namespace;
- one controller NetworkPolicy in the release namespace; and
- a namespace-wide default deny plus managed-worker platform-egress
  NetworkPolicy in the worker namespace.

It does **not** create the OpenAB broker, worker namespace, session anchor
ConfigMaps, worker Pods, worker PVCs, per-generation Secrets or
ServiceAccounts, worker trust ConfigMaps, or worker profiles. Those objects
have different lifecycles and must not become Helm release children.

## Runtime boundary

The trusted broker Pod and trusted controller Pod are separate. In this mode,
OpenAB still receives and routes IM events, but its per-thread child is an ACP
bridge rather than the agent CLI. The agent CLI runs only in the session's
worker Pod:

```text
IM and ACP message flow

Discord or Slack channel
  | thread A mentions one team bot    | thread B mentions the same bot
  +------------------+----------------+
                     v
        +--------------------------------------------+
        | trusted OpenAB broker Pod                  |
        | thread key -> durable mapping              |
        | thread A -> bridge A; thread B -> bridge B |
        +---------------------+----------------------+
                              | authenticated bidirectional
                              | WSS /v1/bridge
                              v
        +--------------------------------------------+
        | trusted controller Pod                     |
        | authenticated relay + lifecycle reconciler |
        +------------^------------------^------------+
                     |                  |
           outbound WSS /v1/worker from each worker
                     |                  |
        +------------+-----+       +----+-----------------+
        | worker Pod A     |       | worker Pod B         |
        | ACP agent A      |       | ACP agent B          |
        +------------------+       +----------------------+

Kubernetes lifecycle flow

controller Pod -> Kubernetes API -> create / fence / reconcile
                                    | session A resources
                                    | session B resources

Resource boundary

worker Pod A                         worker Pod B
  | private HOME/workspace/PVC         | private HOME/workspace/PVC
  | private cgroup/ServiceAccount      | private cgroup/ServiceAccount
  +------------------+-----------------+
                     | read-only / controlled
                     v
        +--------------------------------------------+
        | explicitly shared platform resources       |
        | immutable skills; approved APIs and caches |
        +--------------------------------------------+
```

Each worker independently opens an outbound `/v1/worker` WSS connection back
to the controller; there is no worker-to-worker connection or inbound Service
on either worker. ACP traffic uses the relay connections. Resource creation,
fencing, and reconciliation use the controller's separate Kubernetes API
connection.

For one configured scope, each OpenAB logical thread key owns one retained
logical session and at most one active worker Pod. Sessions do not share a
writable HOME, workspace, Git metadata, PVC, cgroup, or worker ServiceAccount.
They may mount the same versioned immutable skills ConfigMap read-only and call
explicitly allowed shared services; they never mount a shared writable volume.
The detailed invariants and trade-offs live in the
[Kubernetes session isolation ADR](../../docs/adr/kubernetes-session-isolation.md).

Removing or omitting `[kubernetes_session]` preserves the existing local ACP or
AgentCore runtime. A workspace directive or Git worktree remains useful
workflow organization in that default runtime, but it is not a security
boundary.

## Current MVP target

- One controller replica owns one scope and one dedicated worker namespace.
- The broker keeps its thread-to-session mapping in a scope-partitioned private
  file under `$HOME/.openab/session-runtimes/kubernetes-v1/`; the controller's
  durable lifecycle source of truth is one namespaced ConfigMap anchor per
  retained session. The anchor does not replace or reconstruct the broker
  mapping.
- Durable agent state lives on the session-private PVC. Ephemeral runtime state
  uses per-worker `emptyDir` volumes; neither writable volume allocation is
  shared between sessions. Generation Secrets, ServiceAccounts,
  NetworkPolicies, and Pods are replaceable children of the anchor.
- `worker-base` packages the supervisor only. `worker-test` adds the constrained
  fake ACP used by CI; neither is a supported production agent flavour.
- Compute TTL schedules suspension of an expired `Ready` worker while
  retaining resumable state. The storage-retention deadline is advisory in
  this MVP and never authorizes deletion.

## Deferred production and enterprise work

- Production agent-flavoured images, repository checkout seeding, and
  agent-specific credential delivery.
- A Slack thread-native user release surface. Discord exposes explicit release
  through `/reset`; Slack does not support third-party slash commands in
  threads.
- A production Git/worktree end-to-end suite; the current fake ACP proves only
  its fixed, path-confined workspace marker state.
- A transactional enterprise state service, leader election, multi-controller
  coordination, and multi-cluster recovery. The MVP deliberately does not add
  SQLite, PostgreSQL, or a CRD before the single-cluster operating model is
  proven.
- Automatic destructive storage expiry and a physical disk-reclamation SLA.
  Those require an explicit later policy plus validated StorageClass,
  provisioner, finalizer, backup, and recovery behavior.

## Prerequisites

Before enabling the chart, provision:

1. A dedicated worker namespace that is the complete trust domain for one
   controller scope, with appropriate ResourceQuota, storage policy, and
   `restricted` Pod Security Admission (or equivalent validating policy). Do
   not place another tenant's Pods or unrelated Secrets in this namespace: the
   trusted controller necessarily has Pod-create and Secret-read access there.
2. The `controller` target from `Dockerfile.kubernetes-session`, or a compatible
   image containing `/usr/bin/tini` and
   `/usr/local/bin/openab-kubernetes-session-controller`, able to run as
   UID/GID 1000 with a read-only root filesystem. Worker images are configured
   separately in profiles. Pin all production images by digest.
3. A controller process ConfigMap, a worker profile ConfigMap, a TLS Secret,
   and a bridge authentication Secret in the Helm release namespace.
4. A versioned, immutable relay-CA ConfigMap in the worker namespace. A
   centrally managed skills ConfigMap is optional, but when selected by a
   profile it must also be versioned and immutable.
5. An admission policy that prevents deletion or same-name recreation of the
   pinned CA and skills ConfigMaps. Kubernetes RBAC cannot restrict dynamic
   anchor ConfigMap CRUD by name prefix while also denying mutation of these
   shared objects.
6. A CNI that enforces Kubernetes NetworkPolicy. The chart always renders its
   policies when enabled, but creating policy objects has no effect when the
   cluster's network plugin does not enforce them.
7. Helm, kubectl, and jq on the operator workstation. The commands below use
   jq to add fields to client-generated Kubernetes JSON before creation.

The broker is deployed separately. Session isolation becomes active only when
the controller is running **and** the intended OpenAB agent selects its
`[kubernetes_session]` runtime. Installing this chart alone changes no agent.
Compromise of the trusted controller remains a compromise of its dedicated
worker namespace; admission policy is the defense against privileged,
host-mounted, or host-namespace Pod specifications.

## Repository smoke test

The repository includes a disposable two-node Kind harness for this add-on.
It uses pinned Kind, Kubernetes, and container images, an isolated kubeconfig,
and a private test CA. The harness refuses to reuse an existing cluster and
removes only the cluster and image tags it created.
It requires Docker with a running daemon, Kind, Helm, kubectl, OpenSSL, Git,
and jq.

Check local prerequisites without creating anything:

```console
sh scripts/test-kubernetes-session-kind.sh --check
```

Then, from the repository root, run the complete registration smoke test:

```console
sh scripts/test-kubernetes-session-kind.sh --smoke
```

The smoke test first proves managed DNS access, removes the worker labels and
requires three consecutive DNS denials, then restores the labels and proves
access again. It subsequently drives the real bridge and controller into one
fake ACP worker and verifies its ready anchor, consumed registration Secret,
private ownership chain, lack of Kubernetes credentials, and private-only
Service shape.

Run the complete two-session and lifecycle gate separately:

```console
sh scripts/test-kubernetes-session-kind.sh --isolation
```

To retain a reviewer-facing JSON summary of a successful isolation run, provide
an absolute, non-existing output path. The file records only Kubernetes object
UIDs and asserted outcomes; it excludes session keys, logical session IDs,
attempt IDs, workspace markers, and logs.

```sh
OPENAB_KIND_EVIDENCE_FILE="$PWD/kubernetes-session-isolation-evidence.json" \
  sh scripts/test-kubernetes-session-kind.sh --isolation
```

The evidence reports whether each PVC was bound to a distinct PV object and
whether that PV identity survived replacement, compute suspension, and resume.
Release evidence stops at PVC API-object absence: it deliberately does not
claim that the provisioner physically deleted the backing disk.

That mode proves distinct Pod/PVC/ServiceAccount identities and resource
limits, path-confined private marker state, immutable shared skills, enforced
network boundaries, failed-Pod replacement, compute-TTL suspend/resume,
explicit release, and peer-session non-interference. It uses only the
constrained fake ACP worker: it does not claim a production agent or Git
worktree end-to-end. The release assertion proves absence of the anchor and
PVC Kubernetes API objects, not physical reclamation of a PersistentVolume or
cloud disk. Archived CI evidence is recorded under
[Checkpoint F](../../docs/tasks/kubernetes-session-worker-runtime.md#checkpoint-f).

## Mounted controller contract

The chart maps configurable keys from existing objects onto fixed paths:

| Existing object | Fixed controller path |
| --- | --- |
| `controllerConfig` ConfigMap | `/etc/openab-kubernetes-session/process/controller.toml` |
| `profiles` ConfigMap | `/etc/openab-kubernetes-session/profiles/profiles.toml` |
| `tls` Secret | `/var/run/openab-kubernetes-session/tls/tls.crt` and `tls.key` |
| `authentication` Secret | `/var/run/openab-kubernetes-session/auth/token` |

A minimal controller process document is:

```toml
schema_version = 1
scope = "replace-with-the-broker-scope"
worker_namespace = "openab-sessions"
profiles_file = "/etc/openab-kubernetes-session/profiles/profiles.toml"

[listen]
relay_address = "0.0.0.0:8443"
probe_address = "0.0.0.0:8080"

[tls]
certificate_file = "/var/run/openab-kubernetes-session/tls/tls.crt"
private_key_file = "/var/run/openab-kubernetes-session/tls/tls.key"

[authentication]
bridge_credential_file = "/var/run/openab-kubernetes-session/auth/token"

[relay]
max_connections = 128
queue_capacity = 16
byte_budget_bytes = 134217728
```

The mounted process document and Helm values form one deployment contract:

- `worker_namespace` must exactly equal the chart's `workerNamespace`;
- the relay and probe listeners must remain `0.0.0.0:8443` and
  `0.0.0.0:8080`, matching the Deployment and Service; and
- all mounted paths must remain the fixed paths shown above.

A mismatch fails closed or leaves the controller unready; the chart does not
parse or duplicate the strict process TOML.

## Worker profile contract

The profile ConfigMap is operator-owned. Policy applies across the controller
scope; every named profile selects a versioned revision that operators must
treat as immutable. This illustrative document shows the complete common
shape, but its example digest and ACP executable are placeholders and must not
be deployed unchanged:

```toml
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20

[profiles.acp-strict]
current_version = "2026-08-06"

[profiles.acp-strict.revisions."2026-08-06"]
image = "registry.example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
image_pull_secrets = ["registry-pull"]

[profiles.acp-strict.revisions."2026-08-06".relay]
url = "wss://openab-session-controller.openab-system.svc:8443/v1/worker"
ca_config_map_name = "openab-session-controller-ca-2026-08"

[profiles.acp-strict.revisions."2026-08-06".skills]
config_map_name = "team-skills-2026-08-06"

# Optional stronger sandbox. The controller needs separate get-only
# RuntimeClass RBAC as described below.
[profiles.acp-strict.revisions."2026-08-06".runtime_class]
name = "kata"
expected_handler = "kata-qemu"

[profiles.acp-strict.revisions."2026-08-06".supervisor]
executable = "/usr/local/bin/openab-kubernetes-session-worker"
args = ["serve", "--", "/opt/openab-agent/bin/acp-agent"]

[profiles.acp-strict.revisions."2026-08-06".workspace]
size = "20Gi"
storage_class = "encrypted-rwo"
access_mode = "read_write_once_pod"

[profiles.acp-strict.revisions."2026-08-06".resources.requests]
cpu = "250m"
memory = "256Mi"
ephemeral_storage = "1Gi"

[profiles.acp-strict.revisions."2026-08-06".resources.limits]
cpu = "1"
memory = "2Gi"
ephemeral_storage = "8Gi"

[profiles.acp-strict.revisions."2026-08-06".run_as]
uid = 1000
gid = 1000

[[profiles.acp-strict.revisions."2026-08-06".egress]]
target = "selectors"
namespace_labels = { "kubernetes.io/metadata.name" = "model-services" }
pod_labels = { "app.kubernetes.io/name" = "model-gateway" }

[[profiles.acp-strict.revisions."2026-08-06".egress.ports]]
protocol = "tcp"
port = 443
```

The storage TTL must be greater than or equal to the compute TTL. Both values
are bounded and nonzero. `max_active_workers` limits concurrent worker compute
for the whole scope; namespace CPU, memory, ephemeral-storage, PVC-count, and
storage quotas remain required as independent cost bounds. Skills are mounted
at `/opt/openab/skills` read-only. Each allowed business-service destination
must appear in the trusted profile; neither a chat message nor ACP traffic can
expand it.

### Workspace storage

Prepare a StorageClass before enabling a worker profile. For every logical
session, the controller automatically creates a separately named PVC using the
profile's `size`, `storage_class`, and `access_mode`; a dynamic provisioner is
the recommended way to request its exclusively bound PV. A replacement worker
for the same session remounts that PVC, while another session receives a
different claim. Operators must verify that the provisioner or static PV pool
never aliases distinct session claims to the same writable backing path or
storage identity; distinct Kubernetes objects alone do not prove backend
isolation.

Use `read_write_once_pod` when the CSI driver supports it. Use
`read_write_once` only as a compatibility fallback, including for the Kind
fixture and common K3s `local-path` development clusters. RWO limits a volume
to one node, not one Pod, so the controller's distinct-PVC and mount checks
and the dedicated-namespace sole-writer trust assumption remain part of the
boundary. The MVP does not preflight StorageClass or CSI capabilities;
operators must validate provisioning, RWOP support, topology, node-loss
recovery, and reclaim behavior before enabling a production profile. The
current runtime does not support an `existingClaim`, controller-managed PV
prebinding, selectors, snapshots, or clones. Do not mount one writable PVC into
multiple session workers or divide it with `subPath`.

For dynamically provisioned volumes, the StorageClass selects the reclaim
policy inherited by the resulting PV. For static provisioning, configure the
policy directly on each PV; a PVC cannot override it. Prefer `Delete` when
released session disks should be reclaimed for cost control; use `Retain` only
with an audited cleanup or recovery process. Topology-constrained storage
should normally use `volumeBindingMode: WaitForFirstConsumer`. See the ADR's
[storage provisioning and reclamation contract](../../docs/adr/kubernetes-session-isolation.md#62-storage-provisioning-and-reclamation)
and the Kubernetes documentation for
[dynamic provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/),
[access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes),
and [StorageClass policies](https://kubernetes.io/docs/concepts/storage/storage-classes/).

Treat a published revision as append-only. To change image, command, resources,
trust, skills, or network policy, add a new revision and move
`current_version`; retain every historical revision still referenced by an
anchor. Editing or removing an in-use revision can prevent safe resume or
cleanup.

Create every referenced `image_pull_secrets` object in the worker namespace
before enabling the profile, or remove that field for a public image. The Pod
references those Secrets for image pull only; they are never mounted into the
worker filesystem.

Create the external resources without putting credentials in Helm values or
shell history. For example, use reviewed files and your normal secret manager
integration:

The `bridge-token` file must contain 32–4096 bytes of high-entropy ASCII in the
HTTP `b64token` grammar, with any `=` padding only at the end. It must contain
no spaces, tabs, line ending, or trailing newline. Have the secret manager
write those exact bytes; text-oriented export commands commonly append a
newline that both the controller and bridge reject.

```console
kubectl create namespace openab-system
kubectl create namespace openab-sessions
kubectl label namespace openab-sessions \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=latest
kubectl -n openab-system create configmap openab-kubernetes-session-controller-config \
  --from-file=controller.toml=./controller.toml
kubectl -n openab-system create configmap openab-kubernetes-session-worker-profiles \
  --from-file=profiles.toml=./profiles.toml
kubectl -n openab-system create secret tls openab-kubernetes-session-controller-tls \
  --cert=./tls.crt --key=./tls.key
kubectl -n openab-system create secret generic openab-kubernetes-session-controller-auth \
  --from-file=token=./bridge-token

# Broker identity: no RoleBinding and no automatic Kubernetes API token.
kubectl -n openab-system create serviceaccount team-a-openab-broker \
  --dry-run=client -o json \
  | jq '.automountServiceAccountToken = false' \
  | kubectl apply -f -

# Broker-side trust bundle in the broker/controller namespace.
kubectl -n openab-system create configmap openab-kubernetes-session-bridge-ca-2026-08 \
  --from-file=ca.crt=./controller-ca.crt --dry-run=client -o json \
  | jq '.immutable = true' \
  | kubectl create -f -

# Mandatory worker-side relay trust bundle. The key must be ca.crt.
kubectl -n openab-sessions create configmap openab-session-controller-ca-2026-08 \
  --from-file=ca.crt=./controller-ca.crt --dry-run=client -o json \
  | jq '.immutable = true' \
  | kubectl create -f -

# Optional centrally managed read-only skills selected by the example profile.
kubectl -n openab-sessions create configmap team-skills-2026-08-06 \
  --from-file=./skills --dry-run=client -o json \
  | jq '.immutable = true' \
  | kubectl create -f -
```

These pipelines create each ConfigMap as immutable in its first API write and
fail on a same-name collision. Never reuse the versioned names. Admission must
also prevent deletion and same-name recreation. The profile's CA and skills
names must exactly match these objects.

Then render and install the opt-in controller:

```yaml
# session-values.yaml
networkPolicy:
  controller:
    apiServerCIDRs:
      - 10.96.0.1/32
    apiServerPort: 443
    brokerPeers:
      - namespaceLabels:
          kubernetes.io/metadata.name: openab-system
        podLabels:
          app.kubernetes.io/name: openab
          app.kubernetes.io/instance: openab
          app.kubernetes.io/component: team-a
```

The broker labels must match the actual OpenAB Deployment. Determine the API
destination observed by the target CNI rather than copying the example. The
Service and its EndpointSlices expose the two common candidates:

```console
kubectl -n default get service kubernetes -o jsonpath='{.spec.clusterIP}{"\n"}'
kubectl -n default get endpointslice \
  -l kubernetes.io/service-name=kubernetes \
  -o wide
```

For pre-translation enforcement, configure the Service ClusterIP as `/32` or
`/128` together with its Service port (normally 443). For post-translation
enforcement, configure every observed control-plane endpoint address as `/32`
or `/128` together with the endpoint port (often 6443). Do not combine a
Service address with an endpoint port. The chart rejects subnet and wildcard
destinations; route broader external access through an explicitly approved
egress gateway.

```console
helm upgrade --install openab-session-controller \
  charts/openab-kubernetes-session \
  --namespace openab-system \
  --values ./session-values.yaml \
  --set enabled=true \
  --set fullnameOverride=openab-session-controller \
  --set image.repository=registry.example/openab-session-controller \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

Replace the example digest with the verified controller image digest before
installation. The relay URL for that example is
`wss://openab-session-controller.openab-system.svc:8443/v1/worker`. The TLS
certificate must cover the DNS name used by worker profiles.

When enabled, the chart rejects an unpinned controller image by default. For a
disposable local development cluster only, `image.allowMutableTag=true`
explicitly permits `image.tag` (or the chart appVersion fallback); do not use
that escape hatch in a shared or production cluster.

## Enable one broker scope

First install and validate the controller while all OpenAB agents still use
their existing runtime. Then change only the intended team's OpenAB Deployment
to a digest-pinned `broker` target from `Dockerfile.kubernetes-session`, mount
the controller's bridge credential and private CA read-only, and add:

```toml
[kubernetes_session]
controller_url = "wss://openab-session-controller.openab-system.svc:8443/v1/bridge"
profile = "acp-strict"
scope = "team-a-openab"
credential_file = "/var/run/secrets/openab-session/token"
controller_ca_file = "/var/run/secrets/openab-session/ca.crt"
```

The `scope` must exactly match `scope` in the controller process document.
`profile` must name a profile whose `current_version` points to an existing
revision in the mounted profile document. The credential file must contain the
same value as the controller authentication Secret. The bridge endpoint is
exactly `/v1/bridge`; query strings, alternate paths, plaintext WebSocket, and
URL credentials fail closed.

Run exactly one broker writer for a scope and keep its private HOME durable
across broker Pod replacement. Persist and back up the complete
`$HOME/.openab/session-runtimes/kubernetes-v1/<scope-hash>/thread_map.json`
file. A missing mapping that conflicts with an existing anchor fails closed,
and a malformed mapping prevents strict-mode startup; the anchor does not
contain enough IM routing identity to reconstruct this file. The ConfigMap
anchor and broker mapping are separate required state, not competing sources
of truth. Do not mount broker HOME into workers. When this section is present,
controller or bridge failure returns an error and never falls back to a shared
local agent process.

With the existing `charts/openab` chart, scope this change to one
`agents.<name>` entry. Use that entry's `image`, `configToml` or `configUrl`,
`persistence`, `extraVolumes`, and `extraVolumeMounts`; do not replace the
chart-global image for teams that stay on the default runtime. A projected
read-only volume can combine the broker credential and CA:

```yaml
agents:
  # Disable the example agent inherited from the chart's default values.
  kiro:
    enabled: false
  team-a:
    image: registry.example/openab-session-broker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    configUrl: https://config.example/team-a-openab.toml
    serviceAccountName: team-a-openab-broker
    persistence:
      enabled: true
      existingClaim: team-a-openab-broker-home
    extraVolumeMounts:
      - name: kubernetes-session-bridge
        mountPath: /var/run/secrets/openab-session
        readOnly: true
    extraVolumes:
      - name: kubernetes-session-bridge
        projected:
          sources:
            - secret:
                name: openab-kubernetes-session-controller-auth
                items:
                  - key: token
                    path: token
            - configMap:
                name: openab-kubernetes-session-bridge-ca-2026-08
                items:
                  - key: ca.crt
                    path: ca.crt
```

Apply the reviewed values through the existing release rather than editing the
Deployment by hand:

```console
helm upgrade --install openab charts/openab \
  --namespace openab-system \
  --values ./openab-values.yaml
```

Provision the existing claim and projected objects in the OpenAB release
namespace before rollout. Put the `[kubernetes_session]` section above in that
agent's complete `configToml` or remote config document, preserving its full
Discord/Slack and pool configuration while removing any explicit
`[agent].command` and `[agentcore]` section. Build and push the `broker` target
yourself and replace the example digest: this MVP does not publish a supported
production broker image.

The projected Secret and ConfigMap must be in the broker Pod's namespace. If
the controller uses another namespace, synchronize the same credential through
your secret manager and copy the public CA bundle under a versioned name. When
the controller certificate chains to native public roots, omit
`controller_ca_file` and its ConfigMap. Give the trusted broker no Kubernetes
RBAC; when using the main chart, reference an operator-created
`serviceAccountName` whose ServiceAccount sets
`automountServiceAccountToken: false`.

Restart only the selected broker and open a new Discord or Slack thread for a
first-session check. Other OpenAB Deployments without `[kubernetes_session]`
continue using the existing architecture and create no add-on state. Broker
workspace directives are intentionally rejected in this mode: checkout and
workspace construction belongs to the administrator-controlled worker image
and profile.

Wait for the selected broker rollout, confirm its exact image digest, durable
HOME claim, and read-only projected mount, then open the test thread. A
successful first activation creates one anchor, Pod, and PVC with the expected
session labels in the worker namespace. Confirm that `brokerPeers` matches the
broker's actual namespace and Pod labels before treating the test as complete.

For initial installation and every policy-affecting change, keep the broker's
`[kubernetes_session]` runtime disabled. Suspend active workers before narrowing
policy or changing release, namespace, or selector identity.
Kubernetes exposes no readiness signal for a newly handled NetworkPolicy, so
prove on the target CNI that denied traffic is blocked and the configured DNS,
relay, and API flows work before enabling the runtime. This keeps workers from
racing policy enforcement during installation or upgrade.

## Network boundary

The chart installs three long-lived policies:

1. The controller is ingress- and egress-isolated. TCP 8443 ingress is allowed
   only from managed workers and the exact configured broker peers. Egress is
   limited to the configured Kubernetes API host addresses and TCP port. The
   plaintext probe port 8080 is not exposed to Pods.
2. Every Pod in the dedicated worker namespace is default-denied for ingress
   and egress, including a Pod that is accidentally missing worker labels.
3. Managed workers receive only platform baseline egress: the selected DNS
   endpoints on UDP/TCP 53 and this release's controller on TCP 8443.

The controller separately creates one per-generation NetworkPolicy before each
worker Pod. That policy selects one session and generation and adds only the
service destinations and ports from its immutable trusted profile. Kubernetes
combines policies additively, so the Helm baseline deliberately contains no
model gateway, Git host, artifact service, or other business destination.
Chat content and ACP traffic cannot alter either configuration source.

`brokerPeers` defaults to an empty list and therefore admits no broker until an
operator supplies exact namespace and Pod label maps. CoreDNS defaults to the
`kube-system` namespace and `k8s-app=kube-dns`; replace or extend the selector
list for the target cluster. Exact `/32` or `/128` DNS destinations are also
supported for NodeLocal DNS, but host-network handling varies by CNI.

NetworkPolicy is an additive L3/L4 allow-list, not a complete network sandbox.
Another authorized policy can widen access, node-local traffic has special
semantics, ordinary DNS can be used as an exfiltration channel, and deny
behavior for protocols other than TCP, UDP, and SCTP varies by network plugin.
Keep the worker namespace dedicated, restrict NetworkPolicy mutation with
admission, and use a controlled DNS proxy or CNI L7 policy where query-level
governance is required. Kubernetes also leaves Service address and port
translation relative to policy enforcement implementation-specific, so the
API host CIDR, port, and live connectivity must be verified on the target CNI.
See the
[Kubernetes NetworkPolicy documentation](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

The controller reads process configuration, profiles, TLS identity, and bridge
credentials at startup; mounted-file rotation does not hot-reload them. After
updating any referenced ConfigMap or Secret, explicitly restart and wait for
the replacement controller before retiring the old credential:

```console
kubectl -n openab-system rollout restart deployment/openab-session-controller
kubectl -n openab-system rollout status deployment/openab-session-controller
```

## RBAC and trust boundary

The controller is trusted and receives its ServiceAccount token. Its Role is
limited to lifecycle operations on ConfigMaps, Pods, PVCs, Secrets,
ServiceAccounts, and NetworkPolicies in `workerNamespace`. It receives no
Pod exec, log, attach, or port-forward access and no Namespace, PV, Service,
Deployment, Role, or RoleBinding mutation permission. Generated worker Pods do
not receive Kubernetes API credentials.

The controller sets `blockOwnerDeletion` on generated resources, so the Role
also needs `update` on the `configmaps/finalizers` subresource. This is the
narrow permission checked by Kubernetes's
[OwnerReferencesPermissionEnforcement admission controller](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#ownerreferencespermissionenforcement).

RuntimeClass is cluster-scoped and cannot be authorized by this namespaced
Role. Profiles without `runtime_class` need no extra permission. If an
operator enables gVisor, Kata, or another RuntimeClass, it must separately bind
the controller to an operator-owned ClusterRole granting only `get` on the
approved `node.k8s.io/runtimeclasses`, preferably constrained with
`resourceNames`. The chart deliberately does not create cluster-wide RBAC.

Kubernetes RBAC grants are additive and do not support name-prefix denies; see
the [Kubernetes RBAC reference](https://kubernetes.io/docs/reference/access-authn-authz/rbac/).
Although immutable ConfigMaps prevent data changes, they can still be deleted
and recreated, so versioned names plus external admission protection are a
deployment prerequisite; see
[Immutable ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/#immutable-configmaps).

## Lifecycle

State ownership is deliberately split rather than hidden in SQLite:

| State | MVP owner |
| --- | --- |
| IM thread to logical-session mapping | selected broker's private, scope-partitioned HOME |
| lifecycle phase, deadlines, profile revision, and fences | one ConfigMap anchor in the worker namespace |
| durable workspace, HOME, ACP state, and files; any future worker-flavour checkout/Git metadata | one session-private PVC |
| ephemeral `/tmp`, `/var/tmp`, and `/run/openab` state | per-worker `emptyDir` volumes removed with the Pod |
| bootstrap credential and execution identity | per-generation Secret and ServiceAccount |
| centrally managed CA and skills | operator-owned, versioned immutable ConfigMaps |

The anchor contains lifecycle metadata, not prompts, source code, model output,
or credentials. Only the team/agent scope that enables this runtime creates and
operates these objects. The single-replica controller plus ConfigMap anchors is
the quick single-cluster solution; HA database state and multi-controller
coordination remain deferred enterprise work.

Lifecycle operations have different effects:

- `/cancel` cancels only the current ACP turn.
- Idle `session/close` suspends compute: the worker Pod and generation
  resources are removed, while the anchor and PVC remain resumable. The
  controller applies `compute_idle_seconds` only to expired `Ready` sessions on
  its 30-second maintenance scan; it does not interrupt `Busy` work or promise
  an exact wall-clock cutoff. The broker's `[pool].session_ttl_hours`
  independently requests the same non-destructive close on its own schedule.
- `/reset` is the explicit destructive operation in this runtime. OpenAB first
  fences and cancels the active session, invokes its private release extension,
  and removes the broker mapping only after the controller acknowledges
  authoritative Kubernetes API-object absence. A suspended or orphaned session
  must first be resumed by sending a new message before `/reset` can request
  fenced release. There is no resume-only API: that message starts a real ACP
  turn, so use a harmless prompt and wait for completion or `/cancel` it before
  reset.
- `storage_retention_seconds` records an advisory deadline. Expiry reports
  retained state but does not delete it in the MVP; a human-authorized `/reset`
  remains necessary.

`[pool].max_sessions`, profile-policy `max_active_workers`, and namespace
`ResourceQuota` are three independent gates. Align the first two deliberately;
the namespace quota remains the cluster-enforced hard backstop.

A successful release proves that the session anchor and PVC Kubernetes API
objects are absent. It does not prove that the backing PersistentVolume or
cloud disk has been physically reclaimed. StorageClass reclaim policy, the CSI
provisioner, protection finalizers, and the storage platform determine whether
and when that happens. See the ADR's
[storage claim boundary](../../docs/adr/kubernetes-session-isolation.md#6-lifecycle-and-cost).

`helm uninstall` removes only the controller and static policy resources listed
above. It does not delete session anchors, retained PVCs, worker trust objects,
or the worker namespace. Session suspension and release remain explicit
controller lifecycle operations so Helm cannot accidentally destroy retained
work.

Before uninstalling, follow the controlled rollback sequence below and verify
that no managed worker operation is still in progress. The operator can
inventory controller-owned objects without reading their data:

```console
kubectl -n openab-sessions get configmap,pod,persistentvolumeclaim,secret,serviceaccount,networkpolicy \
  -l app.kubernetes.io/managed-by=openab-session-controller
```

This mixed-resource listing is operational inventory, not a transactionally
atomic Kubernetes snapshot and not release authority.

If retained anchors or PVCs remain after uninstall, later lifecycle cleanup
requires reinstalling a controller with the same scope and trust inputs, or a
separate audited operator procedure. Removing the Deployment and its RBAC
while provisioning or release is in progress can leave chargeable resources;
the chart intentionally has no destructive pre-delete hook.

To disable or roll back the add-on safely:

1. Enter a maintenance window that blocks ordinary user ingress, but leave the
   single broker, controller, and controller RBAC running for controlled
   lifecycle operations.
2. For every session to destroy, resume it with a harmless ACP turn when
   necessary, wait or `/cancel`, and then `/reset` it.
3. After destructive releases finish, gracefully stop the broker so every
   session being retained follows non-destructive `session/close`. Verify each
   retained anchor is `Suspended` and has no active Pod, bootstrap Secret,
   generation ServiceAccount, or per-generation NetworkPolicy.
4. Inventory the worker namespace and verify the intended anchor/PVC API
   objects are absent. Separately verify backing-volume reclamation with the
   storage provider when that is an operational requirement.
5. Remove `[kubernetes_session]` or redeploy the default broker image only when
   intentionally returning that bot to the shared local runtime. This is a
   security-contract change, not a transparent fallback.
6. Uninstall the chart, or set `enabled=false`, only after cleanup is complete.

If retained anchors or PVCs are kept, record the exact scope, profile revisions,
CA objects, and compatible TLS/authentication procedure needed to reinstall a
controller. Helm disablement is not a substitute for a retained-state
migration or deletion procedure.
