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
4. Versioned, immutable relay-CA and centrally managed skills ConfigMaps in the
   worker namespace. Their names are selected by an immutable worker profile
   revision.
5. An admission policy that prevents deletion or same-name recreation of the
   pinned CA and skills ConfigMaps. Kubernetes RBAC cannot restrict dynamic
   anchor ConfigMap CRUD by name prefix while also denying mutation of these
   shared objects.
6. A CNI that enforces Kubernetes NetworkPolicy. The chart always renders its
   policies when enabled, but creating policy objects has no effect when the
   cluster's network plugin does not enforce them.

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
Service shape. Cross-session writable-state and ingress isolation belong to
the separate two-session isolation test rather than this bootstrap gate.

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

Create the external resources without putting credentials in Helm values or
shell history. For example, use reviewed files and your normal secret manager
integration:

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
```

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
          app.kubernetes.io/component: kiro
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
  --set-string image.digest=sha256:REPLACE_WITH_64_LOWERCASE_HEX
```

The relay URL for that example is
`wss://openab-session-controller.openab-system.svc:8443/v1/worker`. The TLS
certificate must cover the DNS name used by worker profiles.

Install the chart or change policy-affecting values while the broker's
`[kubernetes_session]` runtime remains disabled. Suspend active workers before
narrowing policy or changing release, namespace, or selector identity.
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

`helm uninstall` removes only the controller and static policy resources listed
above. It does not delete session anchors, retained PVCs, worker trust objects,
or the worker namespace. Session suspension and release remain explicit
controller lifecycle operations so Helm cannot accidentally destroy retained
work.

Before uninstalling, stop the broker from opening new Kubernetes sessions,
suspend or explicitly release each session according to the desired retention
policy, and verify that no managed worker operation is still in progress. The
operator can inventory controller-owned objects without reading their data:

```console
kubectl -n openab-sessions get configmap,pod,persistentvolumeclaim,secret,serviceaccount,networkpolicy \
  -l app.kubernetes.io/managed-by=openab-session-controller
```

If retained anchors or PVCs remain after uninstall, later lifecycle cleanup
requires reinstalling a controller with the same scope and trust inputs, or a
separate audited operator procedure. Removing the Deployment and its RBAC
while provisioning or release is in progress can leave chargeable resources;
the chart intentionally has no destructive pre-delete hook.
