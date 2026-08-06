#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "${0%/*}" && pwd)
TARGET="$SCRIPT_DIR/test-kubernetes-session-kind.sh"
WORKFLOW="$SCRIPT_DIR/../.github/workflows/kubernetes-session-images.yml"
PROFILE_FIXTURE="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/profiles.toml.in"
SKILLS_FIXTURE="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/shared-skill.md"
WORKSPACE_FIXTURE="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/workspace-isolation.ndjson"
ENDPOINT_FILTER="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/api-server-endpoints.jq"
ENDPOINT_FIXTURE="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/api-server-endpoints.json"
TEMPORARY_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/openab-session-kind-contract.XXXXXX")

fail() {
    printf '%s\n' "kubernetes-session Kind contract test: $*" >&2
    exit 1
}

cleanup() {
    rm -rf "$TEMPORARY_ROOT"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

write_command() {
    destination=$1
    status=$2
    {
        printf '%s\n' '#!/bin/sh'
        printf '%s\n' "exit $status"
    } > "$destination"
    chmod +x "$destination"
}

prepare_path() {
    case_name=$1
    missing=${2:-}
    fake_bin="$TEMPORARY_ROOT/$case_name/bin"
    mkdir -p "$fake_bin"
    for command_name in docker kind helm kubectl openssl git jq; do
        if [ "$command_name" != "$missing" ]; then
            write_command "$fake_bin/$command_name" 0
        fi
    done
    printf '%s\n' "$fake_bin"
}

assert_failure() {
    case_name=$1
    expected=$2
    fake_bin=$3
    stdout_file="$TEMPORARY_ROOT/$case_name.stdout"
    stderr_file="$TEMPORARY_ROOT/$case_name.stderr"

    if PATH="$fake_bin" "$TARGET" --check >"$stdout_file" 2>"$stderr_file"; then
        fail "$case_name unexpectedly succeeded"
    fi
    [ ! -s "$stdout_file" ] || fail "$case_name wrote unexpected stdout"
    grep -Fqx "kubernetes-session Kind test: $expected" "$stderr_file" || {
        sed -n '1,20p' "$stderr_file" >&2
        fail "$case_name did not emit the exact failure"
    }
}

assert_endpoint_filter_failure() {
    case_name=$1
    mutation=$2
    expected=$3
    fixture="$TEMPORARY_ROOT/$case_name.json"
    stdout_file="$TEMPORARY_ROOT/$case_name.stdout"
    stderr_file="$TEMPORARY_ROOT/$case_name.stderr"

    jq "$mutation" "$ENDPOINT_FIXTURE" > "$fixture"
    if jq -r -f "$ENDPOINT_FILTER" "$fixture" \
        > "$stdout_file" 2> "$stderr_file"; then
        fail "$case_name unexpectedly succeeded"
    fi
    [ ! -s "$stdout_file" ] || fail "$case_name wrote unexpected stdout"
    grep -Fq "$expected" "$stderr_file" || {
        sed -n '1,20p' "$stderr_file" >&2
        fail "$case_name did not fail with the expected parser error"
    }
}

[ -f "$TARGET" ] || fail "missing scripts/test-kubernetes-session-kind.sh"
[ -f "$WORKFLOW" ] || fail "missing Kubernetes Session Images workflow"
[ -f "$PROFILE_FIXTURE" ] || fail "missing Kind worker profile fixture"
[ -f "$SKILLS_FIXTURE" ] || fail "missing immutable shared skills fixture"
[ -f "$WORKSPACE_FIXTURE" ] || fail "missing workspace isolation fixture"
[ -f "$ENDPOINT_FILTER" ] || fail "missing API EndpointSlice jq filter"
[ -f "$ENDPOINT_FIXTURE" ] || fail "missing API EndpointSlice JSON fixture"

endpoint_output=$(jq -r -f "$ENDPOINT_FILTER" "$ENDPOINT_FIXTURE")
expected_endpoint_output='endpoint=172.18.0.2:6443'
[ "$endpoint_output" = "$expected_endpoint_output" ] || {
    printf '%s\n' "$endpoint_output" >&2
    fail "the jq parser did not preserve only correlated API endpoint tuples"
}
printf '%s\n' '{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","items":[]}' \
    > "$TEMPORARY_ROOT/empty-endpoints.json"
empty_endpoint_output=$(jq -r -f \
    "$ENDPOINT_FILTER" "$TEMPORARY_ROOT/empty-endpoints.json")
[ -z "$empty_endpoint_output" ] || {
    fail "an empty EndpointSlice list must remain retryable"
}
jq '.items[1].endpoints[0].addresses[0] = "172.18.0.3"' \
    "$ENDPOINT_FIXTURE" > "$TEMPORARY_ROOT/conflicting-endpoints.json"
if jq -r -f "$ENDPOINT_FILTER" \
    "$TEMPORARY_ROOT/conflicting-endpoints.json" \
    > "$TEMPORARY_ROOT/conflicting-endpoints.stdout" \
    2> "$TEMPORARY_ROOT/conflicting-endpoints.stderr"; then
    fail "different API EndpointSlice tuples unexpectedly agreed"
fi
[ ! -s "$TEMPORARY_ROOT/conflicting-endpoints.stdout" ] || {
    fail "conflicting API EndpointSlice tuples wrote unexpected stdout"
}
grep -Fq 'Kubernetes API EndpointSlice tuples did not agree' \
    "$TEMPORARY_ROOT/conflicting-endpoints.stderr" || {
    fail "conflicting API EndpointSlice tuples did not fail closed"
}
assert_endpoint_filter_failure protocol-type \
    '.items[0].ports[0].protocol = false' \
    'EndpointSlice port.protocol must be a string or null'
assert_endpoint_filter_failure ready-type \
    '.items[0].endpoints[0].conditions.ready = "false"' \
    'EndpointSlice endpoint.conditions.ready must be boolean or null'
assert_endpoint_filter_failure nil-addresses \
    '.items[0].endpoints[0].addresses = null' \
    'ready IPv4 EndpointSlice addresses must be a non-empty array'
assert_endpoint_filter_failure nil-port \
    '.items[0].ports[0].port = null' \
    'Kubernetes API https/TCP EndpointSlice port must be a number'
assert_endpoint_filter_failure invalid-ipv4 \
    '.items[0].endpoints[0].addresses[0] = "999.999.999.999"' \
    'ready IPv4 EndpointSlice address must be canonical IPv4'
assert_endpoint_filter_failure invalid-port \
    '.items[0].ports[0].port = 70000' \
    'Kubernetes API https/TCP EndpointSlice port must be an integer from 1 to 65535'

grep -Fq 'cidr = "192.0.2.1/32"' "$PROFILE_FIXTURE" || {
    fail "Kind profile must use a non-relay documentation egress target"
}
if grep -Fq 'port = 8443' "$PROFILE_FIXTURE"; then
    fail "Kind profile must leave relay egress to the static chart policy"
fi
grep -Fq '[profiles.kind-smoke.revisions.v1.skills]' "$PROFILE_FIXTURE" || {
    fail "Kind profile must pin centrally managed skills"
}
grep -Fq 'config_map_name = "openab-kind-smoke-skills-v1"' \
    "$PROFILE_FIXTURE" || {
    fail "Kind profile must name the immutable shared skills ConfigMap"
}
grep -Fqx 'OPENAB_KIND_SHARED_SKILL_V1' "$SKILLS_FIXTURE" || {
    fail "Kind shared skills fixture must expose its fixed version marker"
}

workspace_fixture_bytes=$(wc -c < "$WORKSPACE_FIXTURE")
[ "$workspace_fixture_bytes" -gt 0 ] || fail "workspace isolation fixture is empty"
[ "$workspace_fixture_bytes" -le 4096 ] || {
    fail "workspace isolation fixture exceeds its bounded input size"
}
jq -s -e '
    length == 6
    and ([.[].id] | sort == [101, 102, 103, 201, 202, 203])
    and all(.[];
        type == "object"
        and (keys == ["id", "jsonrpc", "method", "params"])
        and .jsonrpc == "2.0"
        and (.id | type) == "number"
        and .params.sessionId == "openab-fake-session-v1"
        and (
            if .method == "_openab/test/workspace/read" then
                (.params | (
                    type == "object"
                    and keys == ["sessionId"]
                ))
            elif .method == "_openab/test/workspace/write" then
                (.params | (
                    type == "object"
                    and keys == ["content", "sessionId"]
                    and (.content | (
                        type == "string"
                        and utf8bytelength <= 4096
                    ))
                ))
            else
                false
            end
        )
    )
    and (
        [.[] | [.id, .method, (.params.content // null)]]
        | sort_by(.[0])
    ) == [
        [101, "_openab/test/workspace/write", "OPENAB_KIND_WORKSPACE_A_V1"],
        [102, "_openab/test/workspace/read", null],
        [103, "_openab/test/workspace/write", "OPENAB_KIND_WORKSPACE_A_V2"],
        [201, "_openab/test/workspace/read", null],
        [202, "_openab/test/workspace/write", "OPENAB_KIND_WORKSPACE_B_V1"],
        [203, "_openab/test/workspace/read", null]
    ]
' "$WORKSPACE_FIXTURE" >/dev/null || {
    fail "workspace isolation fixture is not a closed deterministic probe sequence"
}

unknown_stdout="$TEMPORARY_ROOT/unknown.stdout"
unknown_stderr="$TEMPORARY_ROOT/unknown.stderr"
if "$TARGET" --unknown >"$unknown_stdout" 2>"$unknown_stderr"; then
    fail "unknown mode unexpectedly succeeded"
fi
[ ! -s "$unknown_stdout" ] || fail "unknown mode wrote unexpected stdout"
grep -Fqx "kubernetes-session Kind test: usage: $TARGET [--check|--smoke|--isolation]" \
    "$unknown_stderr" || fail "unknown mode did not emit the exact usage failure"

for missing in docker kind helm kubectl openssl git jq; do
    fake_bin=$(prepare_path "missing-$missing" "$missing")
    assert_failure "missing-$missing" "$missing is required for Kind smoke tests" "$fake_bin"
done

daemon_bin=$(prepare_path docker-daemon)
write_command "$daemon_bin/docker" 1
assert_failure docker-daemon "a running Docker daemon is required for Kind smoke tests" "$daemon_bin"

success_bin=$(prepare_path success)
success_output=$(PATH="$success_bin" "$TARGET" --check)
[ "$success_output" = "kubernetes-session Kind test: prerequisites available" ] || {
    fail "successful prerequisite check returned unexpected output"
}

grep -Fq 'NetworkPolicy CNI did not enforce the worker namespace deny policy' "$TARGET" || {
    fail "the live CNI failure must remain explicit"
}
grep -Fq 'refusing to reuse pre-existing Kind cluster' "$TARGET" || {
    fail "the live harness must reject a pre-existing cluster"
}
grep -Fq 'if [ "$CLUSTER_OWNED" -eq 1 ]; then' "$TARGET" || {
    fail "cluster cleanup must remain ownership-gated"
}
grep -Fq 'run_bounded 30 "$TEMPORARY_ROOT/cleanup-kind"' "$TARGET" || {
    fail "owned cluster cleanup must remain bounded"
}
grep -Fq 'openab-cni-probe-started' "$TARGET" || {
    fail "the live CNI deny gate must distinguish remote DNS from exec failure"
}
grep -Fq 'kill -9 "$termination_pid"' "$TARGET" || {
    fail "bounded commands must escalate from TERM to KILL"
}
grep -Fq '.projected.sources[*]}{.secret.name}' "$TARGET" || {
    fail "worker credential checks must cover projected Secret sources"
}
grep -Fq "trap 'exit 143' TERM" "$TARGET" || {
    fail "the live harness must preserve the TERM exit status"
}
grep -Fq 'for node_name in $node_names; do' "$TARGET" || {
    fail "loaded image aliases must be installed on every Kind node"
}
grep -Fq 'ctr --namespace=k8s.io images list' "$TARGET" || {
    fail "loaded image digests must come from containerd target descriptors"
}
grep -Fq '$1 == ref { print $3 }' "$TARGET" || {
    fail "loaded image aliases must use the containerd target digest column"
}
grep -Fq 'images tag --local --force' "$TARGET" || {
    fail "loaded images must receive node-local exact digest aliases"
}
grep -Fq 'crictl inspecti "$digest_reference"' "$TARGET" || {
    fail "loaded image digest aliases must be verified through CRI"
}
grep -Fq 'loaded $output_name image digest differs across Kind nodes' "$TARGET" || {
    fail "loaded image target digests must agree across Kind nodes"
}
if grep -Fq 'NODE_NAME=$(kind get nodes' "$TARGET"; then
    fail "loaded image verification must not inspect only one Kind node"
fi
grep -Fq 'single_unique_word()' "$TARGET" || {
    fail "EndpointSlice values must be deduplicated before cardinality checks"
}
grep -Fq 'fail "$description values did not agree"' "$TARGET" || {
    fail "different EndpointSlice values must fail closed"
}
grep -Fq 'API_SERVER_ENDPOINT=$(single_unique_word' "$TARGET" || {
    fail "the API endpoint tuple must use the unique EndpointSlice value"
}
grep -Fq 'API_SERVER_IP=${API_SERVER_ENDPOINT%:*}' "$TARGET" || {
    fail "the API server address must come from the selected tuple"
}
grep -Fq 'API_SERVER_PORT=${API_SERVER_ENDPOINT##*:}' "$TARGET" || {
    fail "the API server port must come from the selected tuple"
}
grep -Fq '      - $api_server_ip/32' "$TARGET" || {
    fail "the controller API egress must remain restricted to one exact IPv4 host"
}
grep -Fq 'wait_for_api_server_endpoint()' "$TARGET" || {
    fail "the API EndpointSlice discovery must tolerate bounded publication delay"
}
grep -Fq 'poll_api_server_endpoint()' "$TARGET" || {
    fail "the API EndpointSlice discovery must isolate its polling loop"
}
grep -Fq 'run_bounded 60 "$TEMPORARY_ROOT/api-endpoint"' "$TARGET" || {
    fail "the API EndpointSlice discovery must use the existing bounded runner"
}
grep -Fq 'kubectl --request-timeout=5s -n default get endpointslice' "$TARGET" || {
    fail "each API EndpointSlice query must have a request timeout"
}
grep -Fq '\($address):\($port)' "$ENDPOINT_FILTER" || {
    fail "the API EndpointSlice parser must preserve address-port correlation"
}
grep -Fq 'Kubernetes API endpoint was unavailable after 60s' "$TARGET" || {
    fail "the API EndpointSlice timeout must remain explicit"
}
grep -Fq 'diagnostic-api-endpointslices' "$TARGET" || {
    fail "EndpointSlice failures must retain live object diagnostics"
}
grep -Fq 'jq -r -f "$API_SERVER_ENDPOINT_FILTER"' "$TARGET" || {
    fail "EndpointSlice discovery must use the fixture-tested jq parser"
}
if grep -Fq '.items[*].endpoints[*].addresses[*]' "$TARGET" || \
    grep -Fq '.items[*].ports[*]' "$TARGET"; then
    fail "EndpointSlice discovery must not return to kubectl JSONPath parsing"
fi

grep -Fq 'start_bridge()' "$TARGET" || {
    fail "the live harness must start bridges through one reusable function"
}
grep -Fq 'STARTED_BRIDGE_PID=$!' "$TARGET" || {
    fail "bridge startup must return its exact background process without a subshell"
}
grep -Fq 'BRIDGE_A_PID=' "$TARGET" || {
    fail "the live harness must reserve an explicit process slot for bridge A"
}
grep -Fq 'BRIDGE_B_PID=' "$TARGET" || {
    fail "the live harness must reserve an explicit process slot for bridge B"
}
grep -Fq 'process_id=$4' "$TARGET" || {
    fail "bridge output waits must inspect the requested bridge process"
}
grep -Fq 'error_file=$5' "$TARGET" || {
    fail "bridge output waits must report the requested bridge stderr"
}
grep -Fq 'exec 4>&-' "$TARGET" || {
    fail "cleanup must be able to close bridge B's writer descriptor"
}
grep -Fq 'terminate_process "$BRIDGE_B_PID"' "$TARGET" || {
    fail "cleanup must terminate bridge B independently"
}
grep -Fq 'terminate_process "$BRIDGE_A_PID"' "$TARGET" || {
    fail "cleanup must terminate bridge A independently"
}
grep -Fq 'openab-kind-smoke-skills-v1' "$TARGET" || {
    fail "the live harness must create and verify pinned shared skills"
}
grep -Fq 'worker skills ConfigMap is mutable' "$TARGET" || {
    fail "the live harness must reject mutable shared skills"
}
grep -Fq 'worker Pod does not mount the pinned skills ConfigMap' "$TARGET" || {
    fail "the live harness must verify the exact shared skills reference"
}
grep -Fq 'worker skills mount is writable' "$TARGET" || {
    fail "the live harness must require an explicitly read-only skills mount"
}
grep -Fq 'worker Pod skills UID pin does not match the immutable ConfigMap' \
    "$TARGET" || {
    fail "the live harness must verify the exact shared skills UID pin"
}
grep -Fq 'worker Pod skills resource-version pin does not match the immutable ConfigMap' \
    "$TARGET" || {
    fail "the live harness must verify the exact shared skills resource-version pin"
}
grep -Fq 'openab-skills-write-probe-started' "$TARGET" || {
    fail "the live skills write probe must distinguish execution from denial"
}
grep -Fq 'worker could write to the shared skills mount' "$TARGET" || {
    fail "the live harness must actively reject writable shared skills"
}
grep -Fq -- '--check|--smoke|--isolation)' "$TARGET" || {
    fail "the live harness must expose a distinct isolation mode"
}
grep -Fq 'discord:kind-smoke:thread-2' "$TARGET" || {
    fail "the isolation mode must activate a second logical thread"
}
grep -Fq '00000000-0000-0000-0000-000000000065' "$TARGET" || {
    fail "the second logical thread must use an independent activation attempt"
}
grep -Fq 'session A bridge did not remain active while session B started' \
    "$TARGET" || {
    fail "the isolation mode must prove both bridge lanes remain concurrent"
}
grep -Fq 'session B bridge did not remain active after session creation' \
    "$TARGET" || {
    fail "the isolation mode must prove bridge B remains live at the checkpoint"
}
grep -Fq 'session A worker Pod was replaced while session B started' \
    "$TARGET" || {
    fail "the isolation mode must keep the first worker generation stable"
}
grep -Fq 'exec 4> "$BRIDGE_B_FIFO"' "$TARGET" || {
    fail "the isolation mode must keep bridge B input open independently"
}
grep -Fq 'assert_worker_shared_skills "$WORKER_POD_B"' "$TARGET" || {
    fail "both isolated workers must consume the pinned read-only skills"
}
grep -Fq 'two sessions unexpectedly share a worker Pod' "$TARGET" || {
    fail "the isolation mode must reject a shared worker Pod"
}
grep -Fq 'two sessions unexpectedly share a workspace PVC' "$TARGET" || {
    fail "the isolation mode must reject a shared workspace PVC"
}
grep -Fq 'two sessions unexpectedly share a ServiceAccount' "$TARGET" || {
    fail "the isolation mode must reject a shared worker identity"
}
grep -Fq 'two sessions unexpectedly reuse a registration Secret name' \
    "$TARGET" || {
    fail "the isolation mode must keep bootstrap credentials session-private"
}
grep -Fq 'worker Pod B does not use its anchor-owned ServiceAccount' \
    "$TARGET" || {
    fail "the isolation mode must bind worker B to its private ServiceAccount"
}
grep -Fq 'fail "$isolation_pod_description mounts $isolation_peer_description workspace PVC"' \
    "$TARGET" || {
    fail "the isolation mode must reject a peer workspace mount"
}
grep -Fq 'fail "$isolation_pod_description mounts a PVC outside its private workspace"' \
    "$TARGET" || {
    fail "the isolation mode must reject every additional PVC mount"
}
grep -Fq 'fail "$isolation_pod_description does not mount its private workspace read-write at /session"' \
    "$TARGET" || {
    fail "the isolation mode must mount each private workspace into its worker"
}
grep -Fq 'fail "$isolation_pod_description resource requests or limits drifted from the profile"' \
    "$TARGET" || {
    fail "the isolation mode must verify the second worker resource cgroup contract"
}
grep -Fq 'fixture_request()' "$TARGET" || {
    fail "workspace requests must be selected by exact JSON-RPC ID"
}
grep -Fq 'wait_for_rpc_response()' "$TARGET" || {
    fail "workspace probes must wait for a parsed JSON-RPC response"
}
grep -Fq 'assert_rpc_empty_result()' "$TARGET" || {
    fail "workspace writes must require an exact empty JSON-RPC result"
}
grep -Fq 'assert_rpc_error()' "$TARGET" || {
    fail "workspace visibility denial must require an exact JSON-RPC error"
}
grep -Fq 'assert_rpc_content_result()' "$TARGET" || {
    fail "workspace reads must require an exact JSON-RPC marker result"
}
grep -Fq 'session B unexpectedly observed session A workspace state before its own write' \
    "$TARGET" || {
    fail "the isolation mode must prove the peer marker starts invisible"
}
grep -Fq 'session B workspace write changed session A state' "$TARGET" || {
    fail "the isolation mode must prove B cannot overwrite A"
}
grep -Fq 'session A workspace overwrite changed session B state' "$TARGET" || {
    fail "the isolation mode must prove A cannot overwrite B"
}
grep -Fq 'kubernetes-session Kind test: isolation checks passed' "$TARGET" || {
    fail "the isolation mode must have its own completion signal"
}
grep -Fq 'run: sh scripts/test-kubernetes-session-kind.sh --isolation' \
    "$WORKFLOW" || {
    fail "the image workflow must execute the two-session isolation gate"
}

printf '%s\n' 'kubernetes-session Kind contract test: all checks passed'
