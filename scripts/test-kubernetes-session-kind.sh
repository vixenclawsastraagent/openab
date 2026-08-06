#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "${0%/*}" && pwd)
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
FIXTURE_ROOT="$REPOSITORY_ROOT/tests/fixtures/kubernetes-session-kind"
CHART="$REPOSITORY_ROOT/charts/openab-kubernetes-session"
API_SERVER_ENDPOINT_FILTER="$FIXTURE_ROOT/api-server-endpoints.jq"
MODE=${1:-}

KIND_NODE_IMAGE='kindest/node:v1.32.11@sha256:5fc52d52a7b9574015299724bd68f183702956aa4a2116ae75a63cb574b35af8'
SYSTEM_NAMESPACE='openab-system'
WORKER_NAMESPACE='openab-sessions'
RELEASE_NAME='openab-session-controller'
CONTROLLER_NAME='openab-session-controller'
BROKER_POD='openab-kind-smoke-broker'
PROBE_POD='openab-kind-network-probe'

fail() {
    printf '%s\n' "kubernetes-session Kind test: $*" >&2
    exit 1
}

require_command() {
    command_name=$1
    command -v "$command_name" >/dev/null 2>&1 || {
        fail "$command_name is required for Kind smoke tests"
    }
}

require_prerequisites() {
    for command_name in docker kind helm kubectl openssl git jq; do
        require_command "$command_name"
    done
    docker info >/dev/null 2>&1 || {
        fail "a running Docker daemon is required for Kind smoke tests"
    }
}

case "$MODE" in
    --check|--smoke|--isolation)
        ;;
    *)
        fail "usage: $0 [--check|--smoke|--isolation]"
        ;;
esac

require_prerequisites

if [ "$MODE" = '--check' ]; then
    printf '%s\n' 'kubernetes-session Kind test: prerequisites available'
    exit 0
fi

for fixture in \
    kind.yaml \
    controller.toml \
    profiles.toml.in \
    shared-skill.md \
    api-server-endpoints.jq \
    smoke.ndjson \
    workspace-isolation.ndjson \
    session-lifecycle.ndjson \
    broker-pod.yaml.in \
    network-probe-pod.yaml.in; do
    [ -f "$FIXTURE_ROOT/$fixture" ] || fail "missing fixture $fixture"
done

WORKSPACE_ISOLATION_FIXTURE="$FIXTURE_ROOT/workspace-isolation.ndjson"
SESSION_LIFECYCLE_FIXTURE="$FIXTURE_ROOT/session-lifecycle.ndjson"

TEMPORARY_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/openab-session-kind.XXXXXX")
KUBECONFIG="$TEMPORARY_ROOT/kubeconfig"
export KUBECONFIG

RUN_TAG="smoke-$$"
CLUSTER_NAME="openab-session-smoke-$$"
BROKER_REPOSITORY='openab-session-broker-smoke'
CONTROLLER_REPOSITORY='openab-session-controller-smoke'
WORKER_REPOSITORY='openab-session-worker-test-smoke'
BROKER_IMAGE="$BROKER_REPOSITORY:$RUN_TAG"
CONTROLLER_IMAGE="$CONTROLLER_REPOSITORY:$RUN_TAG"
WORKER_IMAGE="$WORKER_REPOSITORY:$RUN_TAG"

CLUSTER_OWNED=0
CLUSTER_CREATED=0
IMAGES_OWNED=0
BRIDGE_A_PID=''
BRIDGE_A_WRITER_OPEN=0
BRIDGE_B_PID=''
BRIDGE_B_WRITER_OPEN=0
STARTED_BRIDGE_PID=''
SHARED_SKILLS_UID=''
SHARED_SKILLS_RESOURCE_VERSION=''
BOUNDED_PID=''
COMPLETED=0

diagnostics() {
    [ "$CLUSTER_CREATED" -eq 1 ] || return 0
    printf '%s\n' 'kubernetes-session Kind test: cluster diagnostics follow' >&2
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-pods" \
        kubectl --request-timeout=5s get pods -A -o wide || true
    sed -n '1,200p' "$TEMPORARY_ROOT/diagnostic-pods" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-policies" \
        kubectl --request-timeout=5s get networkpolicies -A || true
    sed -n '1,200p' "$TEMPORARY_ROOT/diagnostic-policies" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-pvcs" \
        kubectl --request-timeout=5s get persistentvolumeclaims -A || true
    sed -n '1,200p' "$TEMPORARY_ROOT/diagnostic-pvcs" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-api-service" \
        kubectl --request-timeout=5s -n default \
        get service kubernetes -o yaml || true
    sed -n '1,200p' "$TEMPORARY_ROOT/diagnostic-api-service" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-api-endpointslices" \
        kubectl --request-timeout=5s -n default get endpointslice \
        -l kubernetes.io/service-name=kubernetes -o yaml || true
    sed -n '1,240p' \
        "$TEMPORARY_ROOT/diagnostic-api-endpointslices" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-api-endpoints" \
        kubectl --request-timeout=5s -n default \
        get endpoints kubernetes -o yaml || true
    sed -n '1,200p' "$TEMPORARY_ROOT/diagnostic-api-endpoints" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-controller" \
        kubectl --request-timeout=5s -n "$SYSTEM_NAMESPACE" \
        logs deployment/"$CONTROLLER_NAME" --tail=200 || true
    sed -n '1,240p' "$TEMPORARY_ROOT/diagnostic-controller" >&2 || true
    run_bounded 10 "$TEMPORARY_ROOT/diagnostic-worker-names" \
        kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pods \
        -l 'openab.dev/resource=worker-pod' -o name || true
    for pod_name in $(sed -n '1,20p' "$TEMPORARY_ROOT/diagnostic-worker-names"); do
        diagnostic_name=$(printf '%s' "$pod_name" | tr '/.' '___')
        run_bounded 10 "$TEMPORARY_ROOT/diagnostic-worker-$diagnostic_name" \
            kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" \
            logs "$pod_name" --tail=200 || true
        sed -n '1,240p' \
            "$TEMPORARY_ROOT/diagnostic-worker-$diagnostic_name" >&2 || true
    done
}

cleanup() {
    status=$?
    cleanup_failed=0
    trap - EXIT HUP INT TERM

    if [ -n "$BOUNDED_PID" ]; then
        terminate_process "$BOUNDED_PID"
        BOUNDED_PID=''
    fi
    if [ "$COMPLETED" -ne 1 ]; then
        diagnostics
    fi
    if [ "$BRIDGE_B_WRITER_OPEN" -eq 1 ]; then
        exec 4>&-
        BRIDGE_B_WRITER_OPEN=0
    fi
    if [ "$BRIDGE_A_WRITER_OPEN" -eq 1 ]; then
        exec 3>&-
        BRIDGE_A_WRITER_OPEN=0
    fi
    if [ -n "$STARTED_BRIDGE_PID" ]; then
        terminate_process "$STARTED_BRIDGE_PID"
        STARTED_BRIDGE_PID=''
    fi
    if [ -n "$BRIDGE_B_PID" ]; then
        terminate_process "$BRIDGE_B_PID"
        BRIDGE_B_PID=''
    fi
    if [ -n "$BRIDGE_A_PID" ]; then
        terminate_process "$BRIDGE_A_PID"
        BRIDGE_A_PID=''
    fi
    if [ "$CLUSTER_OWNED" -eq 1 ]; then
        if run_bounded 30 "$TEMPORARY_ROOT/cleanup-kind" \
            kind delete cluster --name "$CLUSTER_NAME"; then
            :
        else
            cleanup_failed=1
            printf '%s\n' \
                'kubernetes-session Kind test: failed to delete the owned Kind cluster' >&2
            sed -n '1,40p' "$TEMPORARY_ROOT/cleanup-kind" >&2 || true
        fi
    fi
    if [ "$IMAGES_OWNED" -eq 1 ]; then
        if run_bounded 20 "$TEMPORARY_ROOT/cleanup-images" \
            docker image rm -f \
            "$BROKER_IMAGE" \
            "$CONTROLLER_IMAGE" \
            "$WORKER_IMAGE"; then
            :
        else
            cleanup_failed=1
            printf '%s\n' \
                'kubernetes-session Kind test: failed to remove owned image tags' >&2
            sed -n '1,40p' "$TEMPORARY_ROOT/cleanup-images" >&2 || true
        fi
    fi
    if ! rm -rf "$TEMPORARY_ROOT"; then
        cleanup_failed=1
        printf '%s\n' \
            'kubernetes-session Kind test: failed to remove its temporary directory' >&2
    fi
    if [ "$status" -eq 0 ] && [ "$cleanup_failed" -ne 0 ]; then
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

terminate_process() {
    termination_pid=$1
    kill "$termination_pid" >/dev/null 2>&1 || true
    termination_elapsed=0
    while kill -0 "$termination_pid" >/dev/null 2>&1 && \
        [ "$termination_elapsed" -lt 2 ]; do
        sleep 1
        termination_elapsed=$((termination_elapsed + 1))
    done
    if kill -0 "$termination_pid" >/dev/null 2>&1; then
        kill -9 "$termination_pid" >/dev/null 2>&1 || true
        termination_elapsed=0
        while kill -0 "$termination_pid" >/dev/null 2>&1 && \
            [ "$termination_elapsed" -lt 2 ]; do
            sleep 1
            termination_elapsed=$((termination_elapsed + 1))
        done
    fi
    if ! kill -0 "$termination_pid" >/dev/null 2>&1; then
        wait "$termination_pid" >/dev/null 2>&1 || true
    fi
}

run_bounded() {
    seconds=$1
    output_file=$2
    shift 2

    "$@" >"$output_file" 2>&1 &
    BOUNDED_PID=$!
    elapsed=0
    while kill -0 "$BOUNDED_PID" >/dev/null 2>&1; do
        if [ "$elapsed" -ge "$seconds" ]; then
            terminate_process "$BOUNDED_PID"
            BOUNDED_PID=''
            return 124
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    if wait "$BOUNDED_PID"; then
        BOUNDED_PID=''
        return 0
    else
        command_status=$?
        BOUNDED_PID=''
        return "$command_status"
    fi
}

wait_for_no_controller_pods() {
    elapsed=0
    while [ "$elapsed" -lt 120 ]; do
        pods=$(kubectl -n "$SYSTEM_NAMESPACE" get pods \
            -l 'app.kubernetes.io/name=openab-kubernetes-session,app.kubernetes.io/instance=openab-session-controller,app.kubernetes.io/component=controller' \
            -o name)
        [ -z "$pods" ] && return 0
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "controller Pods did not stop before the NetworkPolicy gate"
}

wait_for_output() {
    description=$1
    pattern=$2
    output_file=$3
    process_id=$4
    error_file=$5
    seconds=$6
    elapsed=0

    while [ "$elapsed" -lt "$seconds" ]; do
        if grep -Fq "$pattern" "$output_file"; then
            return 0
        fi
        if ! kill -0 "$process_id" >/dev/null 2>&1; then
            sed -n '1,80p' "$error_file" >&2 || true
            fail "$description terminated before producing its result"
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    sed -n '1,80p' "$error_file" >&2 || true
    fail "$description did not complete within ${seconds}s"
}

fixture_request() {
    fixture_request_id=$1
    fixture_request_value=$(jq -s -c --argjson request_id "$fixture_request_id" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )]
        | if length == 1 then
            .[0]
        else
            error("workspace fixture request ID must be unique")
        end
    ' "$WORKSPACE_ISOLATION_FIXTURE") || {
        fail "workspace fixture request ID $fixture_request_id is not unique"
    }
    printf '%s\n' "$fixture_request_value"
}

lifecycle_request() {
    lifecycle_request_id=$1
    lifecycle_request_value=$(jq -s -c --argjson request_id "$lifecycle_request_id" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )]
        | if length == 1 then
            .[0]
        else
            error("lifecycle fixture request ID must be unique")
        end
    ' "$SESSION_LIFECYCLE_FIXTURE") || {
        fail "lifecycle fixture request ID $lifecycle_request_id is not unique"
    }
    printf '%s\n' "$lifecycle_request_value"
}

send_bridge_request() {
    bridge_channel=$1
    bridge_request_description=$2
    bridge_request_line=$3
    case "$bridge_channel" in
        a)
            if (trap '' PIPE; printf '%s\n' "$bridge_request_line" >&3) \
                2>/dev/null; then
                return 0
            fi
            ;;
        b)
            if (trap '' PIPE; printf '%s\n' "$bridge_request_line" >&4) \
                2>/dev/null; then
                return 0
            fi
            ;;
        *)
            fail "unknown bridge request channel $bridge_channel"
            ;;
    esac
    fail "$bridge_request_description input closed before request was accepted"
}

wait_for_rpc_response() {
    rpc_wait_description=$1
    rpc_wait_request_id=$2
    rpc_wait_output_file=$3
    rpc_wait_process_id=$4
    rpc_wait_error_file=$5
    rpc_wait_seconds=$6
    rpc_wait_elapsed=0

    while [ "$rpc_wait_elapsed" -lt "$rpc_wait_seconds" ]; do
        if jq -s -e --argjson request_id "$rpc_wait_request_id" '
            any(.[];
                type == "object"
                and .id == $request_id
            )
        ' "$rpc_wait_output_file" >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$rpc_wait_process_id" >/dev/null 2>&1; then
            sed -n '1,80p' "$rpc_wait_error_file" >&2 || true
            fail "$rpc_wait_description terminated before producing its response"
        fi
        sleep 1
        rpc_wait_elapsed=$((rpc_wait_elapsed + 1))
    done
    sed -n '1,80p' "$rpc_wait_error_file" >&2 || true
    fail "$rpc_wait_description did not complete within ${rpc_wait_seconds}s"
}

assert_rpc_empty_result() {
    rpc_result_description=$1
    rpc_result_output_file=$2
    rpc_result_request_id=$3
    if jq -s -e --argjson request_id "$rpc_result_request_id" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )] == [{
            "jsonrpc": "2.0",
            "id": $request_id,
            "result": {}
        }]
    ' "$rpc_result_output_file" >/dev/null; then
        :
    else
        fail "$rpc_result_description"
    fi
}

assert_rpc_prompt_result() {
    rpc_prompt_description=$1
    rpc_prompt_output_file=$2
    rpc_prompt_request_id=$3
    if jq -s -e --argjson request_id "$rpc_prompt_request_id" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )] == [{
            "jsonrpc": "2.0",
            "id": $request_id,
            "result": {"stopReason": "end_turn"}
        }]
    ' "$rpc_prompt_output_file" >/dev/null; then
        :
    else
        fail "$rpc_prompt_description"
    fi
}

refresh_session_b() {
    peer_refresh_request_id=$1
    peer_refresh_description=$2
    peer_refresh_request=$(lifecycle_request "$peer_refresh_request_id")
    send_bridge_request b "$peer_refresh_description" "$peer_refresh_request"
    wait_for_rpc_response "$peer_refresh_description" \
        "$peer_refresh_request_id" \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    assert_rpc_prompt_result \
        "$peer_refresh_description returned an unexpected result" \
        "$BRIDGE_B_STDOUT" "$peer_refresh_request_id"
}

assert_rpc_error() {
    rpc_error_description=$1
    rpc_error_output_file=$2
    rpc_error_request_id=$3
    rpc_error_code=$4
    rpc_error_message=$5
    if jq -s -e \
        --argjson request_id "$rpc_error_request_id" \
        --argjson expected_code "$rpc_error_code" \
        --arg expected_message "$rpc_error_message" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )] == [{
            "jsonrpc": "2.0",
            "id": $request_id,
            "error": {
                "code": $expected_code,
                "message": $expected_message
            }
        }]
    ' "$rpc_error_output_file" >/dev/null; then
        :
    else
        fail "$rpc_error_description"
    fi
}

assert_rpc_content_result() {
    rpc_content_description=$1
    rpc_content_output_file=$2
    rpc_content_request_id=$3
    rpc_content_expected=$4
    if jq -s -e \
        --argjson request_id "$rpc_content_request_id" \
        --arg expected_content "$rpc_content_expected" '
        [.[] | select(
            type == "object"
            and .id == $request_id
        )] == [{
            "jsonrpc": "2.0",
            "id": $request_id,
            "result": {"content": $expected_content}
        }]
    ' "$rpc_content_output_file" >/dev/null; then
        :
    else
        fail "$rpc_content_description"
    fi
}

start_bridge() {
    start_bridge_fifo=$1
    start_bridge_stdout=$2
    start_bridge_stderr=$3
    start_bridge_session_key=$4
    start_bridge_attempt_id=$5
    start_bridge_mapping_expectation=$6

    case "$start_bridge_mapping_expectation" in
        absent|present)
            ;;
        *)
            fail "invalid bridge mapping expectation $start_bridge_mapping_expectation"
            ;;
    esac

    kubectl -n "$SYSTEM_NAMESPACE" exec -i "$BROKER_POD" -- \
        env \
        OPENAB_SESSION_KEY="$start_bridge_session_key" \
        OPENAB_SESSION_ATTEMPT_ID="$start_bridge_attempt_id" \
        OPENAB_SESSION_MAPPING_EXPECTATION="$start_bridge_mapping_expectation" \
        /usr/local/bin/openab-kubernetes-session bridge \
        --controller-url \
        wss://openab-session-controller.openab-system.svc:8443/v1/bridge \
        --profile kind-smoke \
        --scope kind-smoke \
        --credential-file /var/run/openab-kind-smoke/auth/token \
        --controller-ca-file /var/run/openab-kind-smoke/ca/ca.crt \
        < "$start_bridge_fifo" \
        > "$start_bridge_stdout" \
        2> "$start_bridge_stderr" \
        3>&- 4>&- &
    STARTED_BRIDGE_PID=$!
}

single_resource_name() {
    resource_type=$1
    selector=$2
    description=$3
    resource_names=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        -l "$selector" -o name)
    set -- $resource_names
    [ "$#" -eq 1 ] || fail "$description count was not exactly one"
    printf '%s\n' "$1"
}

kubernetes_resource_uid() {
    uid_resource_type=$1
    uid_resource_name=$2
    uid_description=$3
    uid_snapshot="$TEMPORARY_ROOT/uid-$uid_resource_name.json"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get \
        "$uid_resource_type" "$uid_resource_name" -o json > "$uid_snapshot"
    jq -er '.metadata.uid | select(type == "string" and length > 0)' \
        "$uid_snapshot" || fail "$uid_description has no Kubernetes UID"
}

other_resource_name() {
    resource_type=$1
    selector=$2
    excluded_name=$3
    description=$4
    resource_names=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        -l "$selector" -o name)
    set -- $resource_names
    [ "$#" -eq 2 ] || fail "$description count was not exactly two"
    other_name=''
    for candidate in $resource_names; do
        if [ "$candidate" != "$excluded_name" ]; then
            [ -z "$other_name" ] || fail "$description had more than one peer"
            other_name=$candidate
        fi
    done
    [ -n "$other_name" ] || fail "$description did not contain an independent peer"
    printf '%s\n' "$other_name"
}

single_owned_resource_name() {
    resource_type=$1
    selector=$2
    controlling_owner_uid=$3
    description=$4
    resource_snapshot="$TEMPORARY_ROOT/owned-$resource_type.json"
    kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        -l "$selector" -o json > "$resource_snapshot"
    resource_names=$(jq -r --arg owner_uid "$controlling_owner_uid" '
        .items[]
        | select(any(
            .metadata.ownerReferences[]?;
            .uid == $owner_uid and .controller == true
        ))
        | .metadata.name
    ' "$resource_snapshot")
    set -- $resource_names
    [ "$#" -eq 1 ] || fail "$description count was not exactly one"
    printf '%s\n' "$1"
}

single_word() {
    values=$1
    description=$2
    set -- $values
    [ "$#" -eq 1 ] || fail "$description count was not exactly one"
    printf '%s\n' "$1"
}

single_unique_word() {
    values=$1
    description=$2
    unique=''
    for candidate in $values; do
        if [ -z "$unique" ]; then
            unique=$candidate
        elif [ "$candidate" != "$unique" ]; then
            printf '%s\n' "$description values: $values" >&2
            fail "$description values did not agree"
        fi
    done
    [ -n "$unique" ] || fail "$description was unavailable"
    printf '%s\n' "$unique"
}

capture_anchor_snapshot() {
    anchor_snapshot_name=$1
    anchor_snapshot_destination=$2
    anchor_snapshot_raw="$anchor_snapshot_destination.raw"
    anchor_snapshot_error="$anchor_snapshot_destination.stderr"

    if ! kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get configmap \
        "$anchor_snapshot_name" -o json \
        > "$anchor_snapshot_raw" 2> "$anchor_snapshot_error"; then
        return 1
    fi
    if jq -e --arg expected_name "$anchor_snapshot_name" '
        . as $config_map
        | ($config_map.data["anchor.json"] | fromjson) as $state
        | if (
            $config_map.metadata.name == $expected_name
            and ($config_map.metadata.uid | (
                type == "string" and length > 0
            ))
            and ($config_map.metadata.resourceVersion | (
                type == "string" and length > 0
            ))
            and ($state | type == "object")
            and ($state.sessionId | (
                type == "string" and test("^[0-9a-f]{64}$")
            ))
            and ($state.scopeId | (
                type == "string" and test("^[0-9a-f]{64}$")
            ))
            and ($state.incarnationId | (
                type == "string" and length > 0
            ))
            and ($state.fence.generation | (
                type == "number" and . >= 1 and floor == .
            ))
            and ($state.fence.attemptId | (
                type == "string" and length > 0
            ))
            and ([
                "provisioning", "ready", "busy", "suspending",
                "suspended", "deleting", "blocked"
            ] | index($state.phase) != null)
            and (
                ($state.podUid == null)
                or ($state.podUid | (type == "string" and length > 0))
            )
          ) then
            {
                name: $config_map.metadata.name,
                uid: $config_map.metadata.uid,
                state: $state
            }
          else
            error("malformed session anchor snapshot")
          end
    ' "$anchor_snapshot_raw" > "$anchor_snapshot_destination"; then
        return 0
    fi
    return 2
}

wait_for_anchor_state() {
    anchor_wait_name=$1
    anchor_wait_uid=$2
    anchor_wait_phase=$3
    anchor_wait_pod_uid=$4
    anchor_wait_destination=$5
    anchor_wait_seconds=$6
    anchor_wait_deadline=$(($(date +%s) + anchor_wait_seconds))

    while [ "$(date +%s)" -lt "$anchor_wait_deadline" ]; do
        if capture_anchor_snapshot \
            "$anchor_wait_name" "$anchor_wait_destination"; then
            observed_anchor_uid=$(jq -r '.uid' "$anchor_wait_destination")
            [ "$observed_anchor_uid" = "$anchor_wait_uid" ] || {
                fail "session anchor name was replaced with another UID"
            }
            if jq -e \
                --arg phase "$anchor_wait_phase" \
                --arg pod_uid "$anchor_wait_pod_uid" '
                .state.phase == $phase
                and (
                    if $pod_uid == "" then
                        .state.podUid == null
                    else
                        .state.podUid == $pod_uid
                    end
                )
            ' "$anchor_wait_destination" >/dev/null; then
                return 0
            fi
        else
            anchor_capture_status=$?
            if [ "$anchor_capture_status" -eq 2 ]; then
                fail "session anchor snapshot was malformed"
            fi
        fi
        sleep 1
    done
    sed -n '1,20p' "$anchor_wait_destination.stderr" >&2 || true
    fail "session anchor did not reach $anchor_wait_phase within ${anchor_wait_seconds}s"
}

assert_anchor_stable_identity() {
    stable_anchor_current=$1
    stable_anchor_baseline=$2
    stable_anchor_description=$3
    if jq -e --slurpfile baseline "$stable_anchor_baseline" '
        def stable_identity:
            {
                uid,
                sessionId: .state.sessionId,
                scopeId: .state.scopeId,
                profile: .state.profile,
                incarnationId: .state.incarnationId,
                generation: .state.fence.generation,
                attemptId: .state.fence.attemptId,
                phase: .state.phase,
                podUid: .state.podUid
            };
        stable_identity == ($baseline[0] | stable_identity)
    ' "$stable_anchor_current" >/dev/null; then
        return 0
    fi
    fail "$stable_anchor_description"
}

wait_for_exact_uid_absent() {
    absence_resource_type=$1
    absence_uid=$2
    absence_case=$3
    absence_seconds=$4
    absence_snapshot="$TEMPORARY_ROOT/absence-$absence_case.json"
    absence_deadline=$(($(date +%s) + absence_seconds))

    while [ "$(date +%s)" -lt "$absence_deadline" ]; do
        if kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get \
            "$absence_resource_type" -o json > "$absence_snapshot"; then
            jq -e '
                type == "object"
                and (.items | type == "array")
            ' "$absence_snapshot" >/dev/null || {
                fail "$absence_case Kubernetes list was malformed"
            }
            if jq -e --arg uid "$absence_uid" '
                all(.items[]; (.metadata.uid // "") != $uid)
            ' "$absence_snapshot" >/dev/null; then
                return 0
            fi
        fi
        sleep 1
    done
    fail "$absence_case exact Kubernetes UID remained after ${absence_seconds}s"
}

snapshot_session_children() {
    children_session_id=$1
    children_destination=$2
    children_raw="$children_destination.raw"

    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get \
        pods,persistentvolumeclaims,secrets,serviceaccounts,networkpolicies.networking.k8s.io \
        -o json > "$children_raw"
    jq -e --arg session_id "$children_session_id" '
        if (type == "object" and (.items | type == "array")) then
            [
                .items[]
                | select(
                    (.metadata.annotations["openab.dev/session-id"] // "")
                    == $session_id
                )
                | {
                    kind,
                    name: .metadata.name,
                    uid: .metadata.uid,
                    resource: (
                        .metadata.labels["openab.dev/resource"] // ""
                    )
                }
            ] | sort_by([.kind, .name, .uid])
        else
            error("malformed session child inventory")
        end
    ' "$children_raw" > "$children_destination" || {
        fail "session child inventory was malformed"
    }
}

assert_retained_session_children() {
    retained_session_id=$1
    retained_destination=$2
    retained_pvc_name=$3
    retained_pvc_uid=$4
    retained_description=$5

    snapshot_session_children "$retained_session_id" "$retained_destination"
    jq -e \
        --arg pvc_name "$retained_pvc_name" \
        --arg pvc_uid "$retained_pvc_uid" '
        . == [{
            kind: "PersistentVolumeClaim",
            name: $pvc_name,
            uid: $pvc_uid,
            resource: "workspace-pvc"
        }]
    ' "$retained_destination" >/dev/null || fail "$retained_description"
}

assert_active_session_children() {
    active_session_id=$1
    active_destination=$2
    active_network_policy_name=$3
    active_network_policy_uid=$4
    active_pvc_name=$5
    active_pvc_uid=$6
    active_pod_name=$7
    active_pod_uid=$8
    active_service_account_name=$9
    shift 9
    active_service_account_uid=$1
    active_description=$2

    snapshot_session_children "$active_session_id" "$active_destination"
    jq -e \
        --arg network_policy_name "$active_network_policy_name" \
        --arg network_policy_uid "$active_network_policy_uid" \
        --arg pvc_name "$active_pvc_name" \
        --arg pvc_uid "$active_pvc_uid" \
        --arg pod_name "$active_pod_name" \
        --arg pod_uid "$active_pod_uid" \
        --arg service_account_name "$active_service_account_name" \
        --arg service_account_uid "$active_service_account_uid" '
        . == [
            {
                kind: "NetworkPolicy",
                name: $network_policy_name,
                uid: $network_policy_uid,
                resource: "worker-network-policy"
            },
            {
                kind: "PersistentVolumeClaim",
                name: $pvc_name,
                uid: $pvc_uid,
                resource: "workspace-pvc"
            },
            {
                kind: "Pod",
                name: $pod_name,
                uid: $pod_uid,
                resource: "worker-pod"
            },
            {
                kind: "ServiceAccount",
                name: $service_account_name,
                uid: $service_account_uid,
                resource: "worker-service-account"
            }
        ]
    ' "$active_destination" >/dev/null || fail "$active_description"
}

assert_session_children_unchanged() {
    children_current=$1
    children_baseline=$2
    children_description=$3
    jq -e --slurpfile baseline "$children_baseline" \
        '. == $baseline[0]' "$children_current" >/dev/null || {
        fail "$children_description"
    }
}

wait_for_process_exit() {
    process_wait_pid=$1
    process_wait_seconds=$2
    process_wait_deadline=$(($(date +%s) + process_wait_seconds))

    while [ "$(date +%s)" -lt "$process_wait_deadline" ]; do
        if ! kill -0 "$process_wait_pid" >/dev/null 2>&1; then
            if wait "$process_wait_pid"; then
                BRIDGE_EXIT_STATUS=0
            else
                BRIDGE_EXIT_STATUS=$?
            fi
            return 0
        fi
        sleep 1
    done
    return 1
}

poll_api_server_endpoint() {
    endpoint_json="$TEMPORARY_ROOT/api-endpoint-slices.json"
    while :; do
        # Keep address and port correlated by parsing one complete API snapshot.
        if ! kubectl --request-timeout=5s -n default get endpointslice \
            -l kubernetes.io/service-name=kubernetes \
            -o json > "$endpoint_json"; then
            return 1
        fi
        if ! endpoint_snapshot=$(jq -r -f "$API_SERVER_ENDPOINT_FILTER" \
            "$endpoint_json"); then
            return 1
        fi
        if [ -n "$endpoint_snapshot" ]; then
            printf '%s\n' "$endpoint_snapshot"
            return 0
        fi
        sleep 1
    done
}

wait_for_api_server_endpoint() {
    endpoint_output="$TEMPORARY_ROOT/api-endpoint"
    if run_bounded 60 "$TEMPORARY_ROOT/api-endpoint" \
        poll_api_server_endpoint; then
        :
    else
        endpoint_status=$?
        sed -n '1,40p' "$endpoint_output" >&2 || true
        if [ "$endpoint_status" -eq 124 ]; then
            fail "Kubernetes API endpoint was unavailable after 60s"
        fi
        fail "Kubernetes API EndpointSlice discovery failed"
    fi
    API_SERVER_ENDPOINTS=$(sed -n 's/^endpoint=//p' "$endpoint_output")
    API_SERVER_ENDPOINT=$(single_unique_word \
        "$API_SERVER_ENDPOINTS" 'Kubernetes API endpoint')
    API_SERVER_IP=${API_SERVER_ENDPOINT%:*}
    API_SERVER_PORT=${API_SERVER_ENDPOINT##*:}
    [ "$API_SERVER_IP:$API_SERVER_PORT" = "$API_SERVER_ENDPOINT" ] || {
        fail "Kubernetes API endpoint tuple was malformed"
    }
}

assert_anchor_owner() {
    resource_type=$1
    resource_name=$2
    expected_anchor_name=$3
    expected_anchor_uid=$4
    description=$5
    owner_uids=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        "$resource_name" -o jsonpath='{.metadata.ownerReferences[*].uid}')
    [ "$owner_uids" = "$expected_anchor_uid" ] || {
        fail "$description is not owned only by the session anchor"
    }
    owner_name=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        "$resource_name" -o jsonpath='{.metadata.ownerReferences[0].name}')
    owner_kind=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        "$resource_name" -o jsonpath='{.metadata.ownerReferences[0].kind}')
    owner_controller=$(kubectl -n "$WORKER_NAMESPACE" get "$resource_type" \
        "$resource_name" -o jsonpath='{.metadata.ownerReferences[0].controller}')
    [ "$owner_name" = "$expected_anchor_name" ] || {
        fail "$description names an unexpected owner"
    }
    [ "$owner_kind" = 'ConfigMap' ] || fail "$description owner is not a ConfigMap"
    [ "$owner_controller" = 'true' ] || fail "$description owner is not controlling"
}

assert_worker_pod_isolation_contract() {
    isolation_worker_pod=$1
    isolation_private_pvc=$2
    isolation_peer_pvc=$3
    isolation_pod_description=$4
    isolation_peer_description=$5
    isolation_pod_snapshot="$TEMPORARY_ROOT/$isolation_worker_pod-isolation.json"
    kubectl -n "$WORKER_NAMESPACE" get pod "$isolation_worker_pod" \
        -o json > "$isolation_pod_snapshot"

    isolation_session_claim=$(jq -r '
        .spec.volumes[]
        | select(.name == "session")
        | .persistentVolumeClaim.claimName
    ' "$isolation_pod_snapshot")
    [ "$isolation_session_claim" = "$isolation_private_pvc" ] || {
        fail "$isolation_pod_description does not mount its private workspace PVC"
    }
    if jq -e '
        ([
            .spec.containers[0].volumeMounts[]?
            | select(.name == "session")
        ] | length == 1)
        and any(
            .spec.containers[0].volumeMounts[]?;
            .name == "session"
            and .mountPath == "/session"
            and ((.readOnly // false) == false)
        )
    ' "$isolation_pod_snapshot" >/dev/null; then
        :
    else
        fail "$isolation_pod_description does not mount its private workspace read-write at /session"
    fi
    if jq -e --arg peer "$isolation_peer_pvc" '
        [.spec.volumes[] | .persistentVolumeClaim.claimName // empty]
        | index($peer) != null
    ' "$isolation_pod_snapshot" >/dev/null; then
        fail "$isolation_pod_description mounts $isolation_peer_description workspace PVC"
    fi
    if jq -e --arg private "$isolation_private_pvc" '
        [.spec.volumes[] | .persistentVolumeClaim.claimName // empty]
        == [$private]
    ' "$isolation_pod_snapshot" >/dev/null; then
        :
    else
        fail "$isolation_pod_description mounts a PVC outside its private workspace"
    fi

    if jq -e '
        (.spec.containers | length == 1)
        and (.spec.containers[0].resources.requests.cpu == "10m")
        and (.spec.containers[0].resources.requests.memory == "64Mi")
        and (.spec.containers[0].resources.requests["ephemeral-storage"] == "64Mi")
        and (.spec.containers[0].resources.limits.cpu == "250m")
        and (.spec.containers[0].resources.limits.memory == "256Mi")
        and (.spec.containers[0].resources.limits["ephemeral-storage"] == "256Mi")
    ' "$isolation_pod_snapshot" >/dev/null; then
        :
    else
        fail "$isolation_pod_description resource requests or limits drifted from the profile"
    fi

    if jq -e '
        (.spec.automountServiceAccountToken == false)
        and ((.spec.hostNetwork // false) == false)
        and ((.spec.hostPID // false) == false)
        and ((.spec.hostIPC // false) == false)
        and ((.spec.shareProcessNamespace // false) == false)
        and ([.spec.volumes[] | select(.hostPath != null)] | length == 0)
        and ([
            .spec.volumes[]?.projected.sources[]?.serviceAccountToken
        ] | length == 0)
    ' "$isolation_pod_snapshot" >/dev/null; then
        :
    else
        fail "$isolation_pod_description weakens the worker isolation boundary"
    fi
}

assert_worker_network_policy_contract() {
    network_policy_case=$1
    network_policy_name=$2
    network_policy_worker_pod=$3
    network_policy_peer_pod=$4
    network_policy_description=$5
    network_policy_snapshot="$TEMPORARY_ROOT/network-policy-$network_policy_case.json"
    network_policy_worker_snapshot="$TEMPORARY_ROOT/network-policy-$network_policy_case-worker.json"
    network_policy_peer_snapshot="$TEMPORARY_ROOT/network-policy-$network_policy_case-peer.json"

    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get networkpolicy \
        "$network_policy_name" -o json > "$network_policy_snapshot"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pod \
        "$network_policy_worker_pod" -o json > "$network_policy_worker_snapshot"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pod \
        "$network_policy_peer_pod" -o json > "$network_policy_peer_snapshot"

    if jq -e \
        --slurpfile worker "$network_policy_worker_snapshot" '
        def selected_by($labels; $selector):
            (($selector.matchExpressions // []) | length == 0)
            and all(
                ($selector.matchLabels // {}) | to_entries[];
                $labels[.key] == .value
            );
        ($worker[0].metadata.labels // {}) as $worker_labels
        | ($worker_labels["openab.dev/session"] // "") as $session
        | ($worker_labels["openab.dev/generation"] // "") as $generation
        | ($session | (type == "string" and test("^[0-9a-f]{40}$")))
        and ($generation | (type == "string" and test("^[1-9][0-9]*$")))
        and ({
            podSelector: .spec.podSelector,
            policyTypes: (.spec.policyTypes | sort),
            ingress: (.spec.ingress // []),
            egress: .spec.egress
        } == {
            podSelector: {
                matchLabels: {
                    "openab.dev/session": $session,
                    "openab.dev/generation": $generation,
                    "openab.dev/resource": "worker-pod"
                }
            },
            policyTypes: ["Egress", "Ingress"],
            ingress: [],
            egress: [{
                to: [{ipBlock: {cidr: "192.0.2.1/32"}}],
                ports: [{port: 443, protocol: "TCP"}]
            }]
        })
        and selected_by($worker_labels; .spec.podSelector)
    ' "$network_policy_snapshot" >/dev/null; then
        :
    else
        fail "$network_policy_description dynamic NetworkPolicy does not exactly match its private worker and profile egress"
    fi

    if jq -e \
        --slurpfile peer "$network_policy_peer_snapshot" '
        def selected_by($labels; $selector):
            (($selector.matchExpressions // []) | length == 0)
            and all(
                ($selector.matchLabels // {}) | to_entries[];
                $labels[.key] == .value
            );
        selected_by(
            ($peer[0].metadata.labels // {});
            .spec.podSelector
        ) | not
    ' "$network_policy_snapshot" >/dev/null; then
        :
    else
        fail "$network_policy_description dynamic NetworkPolicy unexpectedly selects the peer worker"
    fi
}

assert_worker_relay_network_policy_contract() {
    relay_worker_a=$1
    relay_worker_b=$2
    relay_policy_snapshot="$TEMPORARY_ROOT/worker-relay-network-policy.json"
    relay_worker_a_snapshot="$TEMPORARY_ROOT/worker-relay-network-policy-a.json"
    relay_worker_b_snapshot="$TEMPORARY_ROOT/worker-relay-network-policy-b.json"

    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get networkpolicy \
        "$CONTROLLER_NAME-worker-net" -o json > "$relay_policy_snapshot"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pod \
        "$relay_worker_a" -o json > "$relay_worker_a_snapshot"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pod \
        "$relay_worker_b" -o json > "$relay_worker_b_snapshot"

    if jq -e \
        --arg system_namespace "$SYSTEM_NAMESPACE" \
        --arg release_name "$RELEASE_NAME" \
        --slurpfile worker_a "$relay_worker_a_snapshot" \
        --slurpfile worker_b "$relay_worker_b_snapshot" '
        def selected_by($labels; $selector):
            (($selector.matchExpressions // []) | length == 0)
            and all(
                ($selector.matchLabels // {}) | to_entries[];
                $labels[.key] == .value
            );
        ({
            podSelector: .spec.podSelector,
            policyTypes: (.spec.policyTypes | sort),
            ingress: (.spec.ingress // []),
            egress: .spec.egress
        } == {
            podSelector: {matchLabels: {
                "app.kubernetes.io/managed-by": "openab-session-controller",
                "openab.dev/resource": "worker-pod"
            }},
            policyTypes: ["Egress"],
            ingress: [],
            egress: [
                {
                    to: [{
                        namespaceSelector: {matchLabels: {
                            "kubernetes.io/metadata.name": "kube-system"
                        }},
                        podSelector: {matchLabels: {"k8s-app": "kube-dns"}}
                    }],
                    ports: [
                        {port: 53, protocol: "UDP"},
                        {port: 53, protocol: "TCP"}
                    ]
                },
                {
                    to: [{
                        namespaceSelector: {matchLabels: {
                            "kubernetes.io/metadata.name": $system_namespace
                        }},
                        podSelector: {matchLabels: {
                            "app.kubernetes.io/name": "openab-kubernetes-session",
                            "app.kubernetes.io/instance": $release_name,
                            "app.kubernetes.io/component": "controller"
                        }}
                    }],
                    ports: [{port: 8443, protocol: "TCP"}]
                }
            ]
        })
        and selected_by(
            ($worker_a[0].metadata.labels // {});
            .spec.podSelector
        )
        and selected_by(
            ($worker_b[0].metadata.labels // {});
            .spec.podSelector
        )
    ' "$relay_policy_snapshot" >/dev/null; then
        :
    else
        fail "worker relay NetworkPolicy contains an unexpected lane"
    fi
}

assert_shared_skills_config_map() {
    skills_config_map_snapshot="$TEMPORARY_ROOT/shared-skills-config-map.json"
    kubectl -n "$WORKER_NAMESPACE" get configmap \
        openab-kind-smoke-skills-v1 -o json > "$skills_config_map_snapshot"
    skills_immutable=$(jq -r '.immutable // false' "$skills_config_map_snapshot")
    [ "$skills_immutable" = 'true' ] || fail "worker skills ConfigMap is mutable"
    skills_owners=$(jq -r '.metadata.ownerReferences[]?.uid' \
        "$skills_config_map_snapshot")
    [ -z "$skills_owners" ] || fail "worker skills ConfigMap has a session owner"
    skills_marker=$(jq -r '.data["SKILL.md"] // ""' "$skills_config_map_snapshot")
    [ "$skills_marker" = 'OPENAB_KIND_SHARED_SKILL_V1' ] || {
        fail "worker skills ConfigMap has unexpected content"
    }
    SHARED_SKILLS_UID=$(jq -r '.metadata.uid // ""' "$skills_config_map_snapshot")
    [ -n "$SHARED_SKILLS_UID" ] || fail "worker skills ConfigMap has no UID"
    SHARED_SKILLS_RESOURCE_VERSION=$(jq -r \
        '.metadata.resourceVersion // ""' "$skills_config_map_snapshot")
    [ -n "$SHARED_SKILLS_RESOURCE_VERSION" ] || {
        fail "worker skills ConfigMap has no resource version"
    }
}

assert_worker_shared_skills() {
    skills_worker_pod=$1
    skills_pod_snapshot="$TEMPORARY_ROOT/$skills_worker_pod-shared-skills.json"
    kubectl -n "$WORKER_NAMESPACE" get pod "$skills_worker_pod" \
        -o json > "$skills_pod_snapshot"
    skills_config_map=$(jq -r \
        '.spec.volumes[] | select(.name == "skills") | .configMap.name' \
        "$skills_pod_snapshot")
    [ "$skills_config_map" = 'openab-kind-smoke-skills-v1' ] || {
        fail "worker Pod does not mount the pinned skills ConfigMap"
    }
    skills_mount_path=$(jq -r \
        '.spec.containers[0].volumeMounts[] | select(.name == "skills") | .mountPath' \
        "$skills_pod_snapshot")
    [ "$skills_mount_path" = '/opt/openab/skills' ] || {
        fail "worker skills mount uses an unexpected path"
    }
    skills_read_only=$(jq -r \
        '.spec.containers[0].volumeMounts[] | select(.name == "skills") | .readOnly' \
        "$skills_pod_snapshot")
    [ "$skills_read_only" = 'true' ] || fail "worker skills mount is writable"
    skills_pinned_name=$(jq -r \
        '.metadata.annotations["openab.dev/skills-config-map-name"] // ""' \
        "$skills_pod_snapshot")
    [ "$skills_pinned_name" = "$skills_config_map" ] || {
        fail "worker Pod skills name pin does not match its mounted ConfigMap"
    }
    skills_pinned_uid=$(jq -r \
        '.metadata.annotations["openab.dev/skills-config-map-uid"] // ""' \
        "$skills_pod_snapshot")
    [ "$skills_pinned_uid" = "$SHARED_SKILLS_UID" ] || {
        fail "worker Pod skills UID pin does not match the immutable ConfigMap"
    }
    skills_pinned_resource_version=$(jq -r \
        '.metadata.annotations["openab.dev/skills-config-map-resource-version"] // ""' \
        "$skills_pod_snapshot")
    [ "$skills_pinned_resource_version" = "$SHARED_SKILLS_RESOURCE_VERSION" ] || {
        fail "worker Pod skills resource-version pin does not match the immutable ConfigMap"
    }

    skills_read_marker=$(kubectl -n "$WORKER_NAMESPACE" exec \
        "$skills_worker_pod" -- /bin/sh -c \
        'IFS= read -r marker < /opt/openab/skills/SKILL.md && printf "%s\n" "$marker"')
    [ "$skills_read_marker" = 'OPENAB_KIND_SHARED_SKILL_V1' ] || {
        fail "worker could not read the pinned shared skill"
    }

    skills_write_output="$TEMPORARY_ROOT/$skills_worker_pod-skills-write"
    if kubectl -n "$WORKER_NAMESPACE" exec "$skills_worker_pod" -- \
        /bin/sh -c '
            printf "%s\n" openab-skills-write-probe-started
            if printf "%s\n" unexpected 2>/dev/null \
                > /opt/openab/skills/SHOULD_NOT_WRITE; then
                printf "%s\n" openab-skills-write-unexpectedly-succeeded
            else
                printf "%s\n" openab-skills-write-denied
            fi
        ' > "$skills_write_output" 2>&1; then
        :
    else
        sed -n '1,40p' "$skills_write_output" >&2 || true
        fail "worker skills write probe failed before reporting its result"
    fi
    grep -Fqx 'openab-skills-write-probe-started' "$skills_write_output" || {
        fail "worker skills write probe did not start"
    }
    if grep -Fqx 'openab-skills-write-unexpectedly-succeeded' \
        "$skills_write_output"; then
        fail "worker could write to the shared skills mount"
    fi
    grep -Fqx 'openab-skills-write-denied' "$skills_write_output" || {
        sed -n '1,40p' "$skills_write_output" >&2 || true
        fail "worker skills write probe returned an unexpected result"
    }
    skills_read_marker=$(kubectl -n "$WORKER_NAMESPACE" exec \
        "$skills_worker_pod" -- /bin/sh -c \
        'IFS= read -r marker < /opt/openab/skills/SKILL.md && printf "%s\n" "$marker"')
    [ "$skills_read_marker" = 'OPENAB_KIND_SHARED_SKILL_V1' ] || {
        fail "shared skill content changed after the denied write"
    }
}

assert_controller_off_control_plane() {
    controller_nodes=$(kubectl -n "$SYSTEM_NAMESPACE" get pods \
        -l 'app.kubernetes.io/name=openab-kubernetes-session,app.kubernetes.io/instance=openab-session-controller,app.kubernetes.io/component=controller' \
        -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}')
    controller_node=$(single_word "$controller_nodes" 'session controller node')
    [ "$controller_node" != "$CONTROL_PLANE_NODE" ] || {
        fail "session controller shares the API server node, so API egress is not proven"
    }
}

prepare_build_context() {
    context="$TEMPORARY_ROOT/context"
    manifest="$TEMPORARY_ROOT/build-files"
    mkdir -p "$context"

    set -- \
        Cargo.toml \
        Cargo.lock \
        src \
        crates/openab-core/Cargo.toml \
        crates/openab-core/src \
        crates/openab-gateway/Cargo.toml \
        crates/openab-gateway/src \
        crates/openab-mcp/Cargo.toml \
        crates/openab-mcp/src \
        crates/openab-kubernetes-session/Cargo.toml \
        crates/openab-kubernetes-session/Cargo.lock \
        crates/openab-kubernetes-session/src
    untracked_inputs=$(git -C "$REPOSITORY_ROOT" ls-files \
        --others --exclude-standard -- "$@")
    [ -z "$untracked_inputs" ] || {
        fail "new image source files must be staged before the Kind build"
    }
    git -C "$REPOSITORY_ROOT" ls-files --cached -z -- "$@" > "$manifest"
    cp "$REPOSITORY_ROOT/Dockerfile.kubernetes-session" \
        "$context/Dockerfile.kubernetes-session"
    (
        cd "$REPOSITORY_ROOT"
        tar --null -cf - -T "$manifest"
    ) | (
        cd "$context"
        tar -xf -
    )
    [ ! -e "$context/.git" ] || fail "Kind build context contains .git"
    [ ! -e "$context/crates/openab-kubernetes-session/target" ] || {
        fail "Kind build context contains Cargo target output"
    }
}

build_images() {
    context="$TEMPORARY_ROOT/context"
    for image_name in "$BROKER_IMAGE" "$CONTROLLER_IMAGE" "$WORKER_IMAGE"; do
        if docker image inspect "$image_name" >/dev/null 2>&1; then
            fail "refusing to replace pre-existing harness image $image_name"
        fi
    done
    IMAGES_OWNED=1
    sh "$SCRIPT_DIR/test-kubernetes-session-images.sh" --static
    docker build -f "$context/Dockerfile.kubernetes-session" \
        --target broker -t "$BROKER_IMAGE" "$context"
    docker build -f "$context/Dockerfile.kubernetes-session" \
        --target controller -t "$CONTROLLER_IMAGE" "$context"
    docker build -f "$context/Dockerfile.kubernetes-session" \
        --target worker-test -t "$WORKER_IMAGE" "$context"
}

loaded_image_digest() {
    image_name=$1
    output_name=$2
    normalized_image="docker.io/library/$image_name"
    repository=${normalized_image%:*}
    expected_digest=''
    node_names=$(kind get nodes --name "$CLUSTER_NAME")
    [ -n "$node_names" ] || fail "Kind returned no node names"

    for node_name in $node_names; do
        image_list="$TEMPORARY_ROOT/$output_name-$node_name-images"
        docker exec "$node_name" ctr --namespace=k8s.io images list > "$image_list"
        node_digests=$(awk -v ref="$normalized_image" \
            '$1 == ref { print $3 }' "$image_list")
        node_digest=$(single_word "$node_digests" \
            "loaded $output_name image target on $node_name")

        digest_hex=${node_digest#sha256:}
        if [ "$digest_hex" = "$node_digest" ] || \
            [ "${#digest_hex}" -ne 64 ]; then
            fail "loaded $output_name image has an invalid target digest on $node_name"
        fi
        case "$digest_hex" in
            *[!0-9a-f]*)
                fail "loaded $output_name image has an invalid target digest on $node_name"
                ;;
        esac

        if [ -z "$expected_digest" ]; then
            expected_digest=$node_digest
        elif [ "$node_digest" != "$expected_digest" ]; then
            fail "loaded $output_name image digest differs across Kind nodes"
        fi

        digest_reference="$repository@$node_digest"
        docker exec "$node_name" ctr --namespace=k8s.io images tag --local --force \
            "$normalized_image" "$digest_reference" >/dev/null

        inspection="$TEMPORARY_ROOT/$output_name-$node_name-cri-image.json"
        cri_visible=0
        attempts=0
        while [ "$attempts" -lt 10 ]; do
            if docker exec "$node_name" crictl inspecti "$digest_reference" \
                > "$inspection" 2>&1 &&
                grep -Fq "\"$digest_reference\"" "$inspection"; then
                cri_visible=1
                break
            fi
            attempts=$((attempts + 1))
            [ "$attempts" -ge 10 ] || sleep 1
        done
        [ "$cri_visible" -eq 1 ] || {
            sed -n '1,40p' "$inspection" >&2 || true
            fail "loaded $output_name digest reference is not visible through CRI on $node_name"
        }
    done

    printf '%s\n' "$repository@$expected_digest"
}

generate_certificates() {
    certificate_root="$TEMPORARY_ROOT/certificates"
    mkdir -p "$certificate_root"

    cat > "$certificate_root/ca.cnf" <<'EOF'
[req]
distinguished_name = distinguished_name
x509_extensions = v3_ca
prompt = no

[distinguished_name]
CN = OpenAB Kind smoke CA

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

    cat > "$certificate_root/server.cnf" <<'EOF'
[req]
distinguished_name = distinguished_name
req_extensions = request_extensions
prompt = no

[distinguished_name]
CN = openab-session-controller.openab-system.svc

[request_extensions]
subjectAltName = @alternative_names

[alternative_names]
DNS.1 = openab-session-controller.openab-system.svc
DNS.2 = openab-session-controller.openab-system.svc.cluster.local
EOF

    cat > "$certificate_root/server.ext" <<'EOF'
[server_extensions]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
subjectAltName = @alternative_names

[alternative_names]
DNS.1 = openab-session-controller.openab-system.svc
DNS.2 = openab-session-controller.openab-system.svc.cluster.local
EOF

    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
        -config "$certificate_root/ca.cnf" \
        -keyout "$certificate_root/ca.key" \
        -out "$certificate_root/ca.crt" >/dev/null 2>&1
    openssl req -newkey rsa:2048 -nodes -sha256 \
        -config "$certificate_root/server.cnf" \
        -keyout "$certificate_root/tls.key" \
        -out "$certificate_root/server.csr" >/dev/null 2>&1
    openssl x509 -req -sha256 -days 1 \
        -in "$certificate_root/server.csr" \
        -CA "$certificate_root/ca.crt" \
        -CAkey "$certificate_root/ca.key" \
        -CAcreateserial \
        -extfile "$certificate_root/server.ext" \
        -extensions server_extensions \
        -out "$certificate_root/tls.crt" >/dev/null 2>&1
}

create_namespaces_and_configuration() {
    worker_digest=$1
    certificate_root="$TEMPORARY_ROOT/certificates"
    profiles_file="$TEMPORARY_ROOT/profiles.toml"

    kubectl create namespace "$SYSTEM_NAMESPACE"
    kubectl create namespace "$WORKER_NAMESPACE"
    for namespace in "$SYSTEM_NAMESPACE" "$WORKER_NAMESPACE"; do
        kubectl label namespace "$namespace" \
            pod-security.kubernetes.io/enforce=restricted \
            pod-security.kubernetes.io/enforce-version=latest \
            --overwrite
    done

    sed "s|__WORKER_IMAGE__|$worker_digest|" \
        "$FIXTURE_ROOT/profiles.toml.in" > "$profiles_file"
    grep -Fq "image = \"$worker_digest\"" "$profiles_file" || {
        fail "worker image digest was not inserted into the profile"
    }

    kubectl -n "$SYSTEM_NAMESPACE" create configmap \
        openab-kubernetes-session-controller-config \
        --from-file=controller.toml="$FIXTURE_ROOT/controller.toml"
    kubectl -n "$SYSTEM_NAMESPACE" create configmap \
        openab-kubernetes-session-worker-profiles \
        --from-file=profiles.toml="$profiles_file"
    kubectl -n "$SYSTEM_NAMESPACE" create secret tls \
        openab-kubernetes-session-controller-tls \
        --cert="$certificate_root/tls.crt" \
        --key="$certificate_root/tls.key"
    kubectl -n "$SYSTEM_NAMESPACE" create secret generic \
        openab-kubernetes-session-controller-auth \
        --from-file=token="$BRIDGE_TOKEN_FILE"
    kubectl -n "$SYSTEM_NAMESPACE" create configmap \
        openab-kind-smoke-bridge-ca \
        --from-file=ca.crt="$certificate_root/ca.crt"
    kubectl -n "$WORKER_NAMESPACE" create configmap \
        openab-kind-smoke-relay-ca \
        --from-file=ca.crt="$certificate_root/ca.crt"
    kubectl -n "$WORKER_NAMESPACE" create configmap \
        openab-kind-smoke-skills-v1 \
        --from-file=SKILL.md="$FIXTURE_ROOT/shared-skill.md"
    kubectl -n "$WORKER_NAMESPACE" patch configmap \
        openab-kind-smoke-relay-ca \
        --type=merge \
        -p '{"immutable":true}'
    kubectl -n "$WORKER_NAMESPACE" patch configmap \
        openab-kind-smoke-skills-v1 \
        --type=merge \
        -p '{"immutable":true}'
}

write_helm_values() {
    api_server_ip=$1
    api_server_port=$2
    controller_digest=$3
    values_file=$4
    controller_repository=${controller_digest%@*}
    controller_sha=${controller_digest#*@}
    cat > "$values_file" <<EOF
enabled: true
fullnameOverride: $CONTROLLER_NAME
workerNamespace: $WORKER_NAMESPACE
image:
  repository: $controller_repository
  tag: ""
  digest: $controller_sha
  pullPolicy: Never
networkPolicy:
  controller:
    apiServerCIDRs:
      - $api_server_ip/32
    apiServerPort: $api_server_port
    brokerPeers:
      - namespaceLabels:
          kubernetes.io/metadata.name: $SYSTEM_NAMESPACE
        podLabels:
          app.kubernetes.io/name: openab-kind-smoke-broker
          app.kubernetes.io/instance: kind-smoke
  worker:
    dns:
      selectors:
        - namespaceLabels:
            kubernetes.io/metadata.name: kube-system
          podLabels:
            k8s-app: kube-dns
      cidrs: []
EOF
}

assert_invalid_cidr_rejected_by_api() {
    invalid_manifest="$TEMPORARY_ROOT/invalid-cidr.yaml"
    invalid_output="$TEMPORARY_ROOT/invalid-cidr.output"
    helm template invalid-cidr "$CHART" \
        --namespace "$SYSTEM_NAMESPACE" \
        --set enabled=true \
        --set-string 'networkPolicy.controller.apiServerCIDRs[0]=999.999.999.999/32' \
        > "$invalid_manifest"
    if kubectl create --dry-run=server -f "$invalid_manifest" \
        >"$invalid_output" 2>&1; then
        fail "Kubernetes API accepted a semantically invalid NetworkPolicy CIDR"
    fi
    grep -Fq '999.999.999.999/32' "$invalid_output" || {
        sed -n '1,40p' "$invalid_output" >&2
        fail "Kubernetes API rejected the invalid fixture for an unexpected reason"
    }
    grep -Eq 'cidr|CIDR' "$invalid_output" || {
        sed -n '1,40p' "$invalid_output" >&2
        fail "Kubernetes API rejection did not identify the CIDR field"
    }
}

prove_network_policy_enforcement() {
    probe_manifest="$TEMPORARY_ROOT/network-probe.yaml"
    probe_output="$TEMPORARY_ROOT/network-probe.output"
    sed "s|__BROKER_IMAGE__|$BROKER_DIGEST|" \
        "$FIXTURE_ROOT/network-probe-pod.yaml.in" > "$probe_manifest"
    kubectl apply -f "$probe_manifest"
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$PROBE_POD" --timeout=120s
    kubectl -n "$WORKER_NAMESPACE" exec "$PROBE_POD" -- \
        /bin/sh -c 'command -v getent >/dev/null' || {
        fail "broker test image does not provide getent for the CNI gate"
    }

    kubectl -n "$WORKER_NAMESPACE" label pod "$PROBE_POD" \
        app.kubernetes.io/managed-by=openab-session-controller \
        openab.dev/resource=worker-pod \
        --overwrite
    wait_for_managed_worker_dns "$probe_output"

    kubectl -n "$WORKER_NAMESPACE" label pod "$PROBE_POD" \
        app.kubernetes.io/managed-by- \
        openab.dev/resource-

    attempts=0
    consecutive_denials=0
    while [ "$attempts" -lt 12 ]; do
        if run_bounded 3 "$probe_output" \
            kubectl -n "$WORKER_NAMESPACE" exec "$PROBE_POD" -- \
            /bin/sh -c \
            'printf "%s\n" openab-cni-probe-started; if getent hosts kubernetes.default.svc.cluster.local; then printf "%s\n" openab-cni-dns-allowed; exit 0; else printf "%s\n" openab-cni-dns-denied; exit 42; fi'; then
            consecutive_denials=0
        else
            probe_status=$?
            if ! grep -Fq 'openab-cni-probe-started' "$probe_output"; then
                sed -n '1,20p' "$probe_output" >&2 || true
                fail "CNI deny probe did not start inside the worker namespace"
            fi
            case "$probe_status" in
                42)
                    grep -Fq 'openab-cni-dns-denied' "$probe_output" || {
                        fail "CNI deny probe returned an unexpected remote failure"
                    }
                    ;;
                124)
                    if grep -Fq 'openab-cni-dns-allowed' "$probe_output"; then
                        fail "CNI deny probe resolved DNS before its exec stream stalled"
                    fi
                    ;;
                *)
                    sed -n '1,20p' "$probe_output" >&2 || true
                    fail "CNI deny probe failed outside the expected DNS path"
                    ;;
            esac
            consecutive_denials=$((consecutive_denials + 1))
            [ "$consecutive_denials" -ge 3 ] && break
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    [ "$consecutive_denials" -ge 3 ] || {
        fail "NetworkPolicy CNI did not enforce the worker namespace deny policy"
    }

    kubectl -n "$WORKER_NAMESPACE" label pod "$PROBE_POD" \
        app.kubernetes.io/managed-by=openab-session-controller \
        openab.dev/resource=worker-pod \
        --overwrite
    wait_for_managed_worker_dns "$probe_output"

    kubectl -n "$WORKER_NAMESPACE" delete pod "$PROBE_POD" \
        --wait=true --timeout=60s
}

wait_for_managed_worker_dns() {
    probe_output=$1
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if run_bounded 3 "$probe_output" \
            kubectl -n "$WORKER_NAMESPACE" exec "$PROBE_POD" -- \
            getent hosts kubernetes.default.svc.cluster.local; then
            break
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    [ "$attempts" -lt 30 ] || {
        sed -n '1,20p' "$probe_output" >&2 || true
        fail "managed-worker DNS allow policy was not enforced"
    }
}

if ! EXISTING_CLUSTERS=$(kind get clusters); then
    fail "unable to inventory existing Kind clusters"
fi
if printf '%s\n' "$EXISTING_CLUSTERS" | grep -Fqx "$CLUSTER_NAME"; then
    fail "refusing to reuse pre-existing Kind cluster $CLUSTER_NAME"
fi
prepare_build_context
build_images

CLUSTER_OWNED=1
kind create cluster \
    --name "$CLUSTER_NAME" \
    --config "$FIXTURE_ROOT/kind.yaml" \
    --image "$KIND_NODE_IMAGE" \
    --kubeconfig "$KUBECONFIG" \
    --wait 180s
CLUSTER_CREATED=1

kind load docker-image --name "$CLUSTER_NAME" \
    "$BROKER_IMAGE" "$CONTROLLER_IMAGE" "$WORKER_IMAGE"
BROKER_DIGEST=$(loaded_image_digest "$BROKER_IMAGE" broker)
CONTROLLER_DIGEST=$(loaded_image_digest "$CONTROLLER_IMAGE" controller)
WORKER_DIGEST=$(loaded_image_digest "$WORKER_IMAGE" worker)

generate_certificates
BRIDGE_TOKEN=$(openssl rand -hex 32)
[ "${#BRIDGE_TOKEN}" -eq 64 ] || fail "OpenSSL returned an invalid bridge credential"
BRIDGE_TOKEN_FILE="$TEMPORARY_ROOT/bridge-token"
umask 077
printf '%s' "$BRIDGE_TOKEN" > "$BRIDGE_TOKEN_FILE"
unset BRIDGE_TOKEN
create_namespaces_and_configuration "$WORKER_DIGEST"

# EndpointSlice clients must deduplicate overlapping slices. This single-node
# control plane still rejects more than one distinct API address or port.
wait_for_api_server_endpoint
case "$API_SERVER_IP" in
    ''|*[!0-9.]*)
        fail "Kind returned an unsupported Kubernetes API endpoint address"
        ;;
esac
case "$API_SERVER_PORT" in
    ''|*[!0-9]*)
        fail "Kind returned an unsupported Kubernetes API endpoint port"
        ;;
esac
HELM_VALUES="$TEMPORARY_ROOT/session-values.yaml"
write_helm_values \
    "$API_SERVER_IP" "$API_SERVER_PORT" "$CONTROLLER_DIGEST" "$HELM_VALUES"

helm upgrade --install "$RELEASE_NAME" "$CHART" \
    --namespace "$SYSTEM_NAMESPACE" \
    --values "$HELM_VALUES" \
    --atomic \
    --wait \
    --timeout 5m

PRE_ACTIVATION_WORKERS=$(kubectl -n "$WORKER_NAMESPACE" get pods \
    -l 'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-pod' \
    -o name)
[ -z "$PRE_ACTIVATION_WORKERS" ] || {
    fail "worker Pod existed before a broker session was activated"
}

CONTROL_PLANE_NODES=$(kubectl get nodes \
    -l node-role.kubernetes.io/control-plane -o name)
CONTROL_PLANE_NODE_RESOURCE=$(single_word \
    "$CONTROL_PLANE_NODES" 'Kind control-plane node')
CONTROL_PLANE_NODE=${CONTROL_PLANE_NODE_RESOURCE#node/}
assert_controller_off_control_plane

assert_invalid_cidr_rejected_by_api

kubectl -n "$SYSTEM_NAMESPACE" scale deployment/"$CONTROLLER_NAME" \
    --replicas=0
wait_for_no_controller_pods
prove_network_policy_enforcement
kubectl -n "$SYSTEM_NAMESPACE" scale deployment/"$CONTROLLER_NAME" \
    --replicas=1
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/"$CONTROLLER_NAME" \
    --timeout=180s
assert_controller_off_control_plane

BROKER_MANIFEST="$TEMPORARY_ROOT/broker-pod.yaml"
sed "s|__BROKER_IMAGE__|$BROKER_DIGEST|" \
    "$FIXTURE_ROOT/broker-pod.yaml.in" > "$BROKER_MANIFEST"
kubectl apply -f "$BROKER_MANIFEST"
kubectl -n "$SYSTEM_NAMESPACE" wait \
    --for=condition=Ready "pod/$BROKER_POD" --timeout=120s

BRIDGE_A_FIFO="$TEMPORARY_ROOT/bridge-a.stdin"
BRIDGE_A_STDOUT="$TEMPORARY_ROOT/bridge-a.stdout"
BRIDGE_A_STDERR="$TEMPORARY_ROOT/bridge-a.stderr"
mkfifo "$BRIDGE_A_FIFO"
start_bridge \
    "$BRIDGE_A_FIFO" \
    "$BRIDGE_A_STDOUT" \
    "$BRIDGE_A_STDERR" \
    'discord:kind-smoke:thread-1' \
    '00000000-0000-0000-0000-000000000064' \
    absent
BRIDGE_A_PID=$STARTED_BRIDGE_PID
STARTED_BRIDGE_PID=''
exec 3> "$BRIDGE_A_FIFO"
BRIDGE_A_WRITER_OPEN=1

INITIALIZE_LINE=$(sed -n '1p' "$FIXTURE_ROOT/smoke.ndjson")
SESSION_NEW_LINE=$(sed -n '2p' "$FIXTURE_ROOT/smoke.ndjson")
[ -n "$INITIALIZE_LINE" ] || fail "smoke fixture has no initialize request"
[ -n "$SESSION_NEW_LINE" ] || fail "smoke fixture has no session/new request"
send_bridge_request a 'bridge initialization' "$INITIALIZE_LINE"
wait_for_output 'bridge initialization' '"id":1' \
    "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 180
grep -Fq 'openab-kubernetes-session-fake-acp' "$BRIDGE_A_STDOUT" || {
    fail "bridge initialization did not reach the fake ACP worker"
}

WORKER_POD_RESOURCE=$(single_resource_name pod \
    'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-pod' \
    'smoke-session worker Pod')
WORKER_POD=${WORKER_POD_RESOURCE#pod/}
kubectl -n "$WORKER_NAMESPACE" wait \
    --for=condition=Ready "pod/$WORKER_POD" --timeout=120s
assert_shared_skills_config_map
assert_worker_shared_skills "$WORKER_POD"

send_bridge_request a 'fake ACP session creation' "$SESSION_NEW_LINE"
wait_for_output 'fake ACP session creation' '"id":2' \
    "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
grep -Fq 'openab-fake-session-v1' "$BRIDGE_A_STDOUT" || {
    fail "fake ACP did not return its fixed session identity"
}

ANCHOR_RESOURCE=$(single_resource_name configmap \
    'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=session-anchor' \
    'smoke-session anchor')
ANCHOR_NAME=${ANCHOR_RESOURCE#configmap/}
ANCHOR_UID=$(kubectl -n "$WORKER_NAMESPACE" get configmap "$ANCHOR_NAME" \
    -o jsonpath='{.metadata.uid}')
[ -n "$ANCHOR_UID" ] || fail "session anchor has no Kubernetes UID"
ANCHOR_STATE=$(kubectl -n "$WORKER_NAMESPACE" get configmap "$ANCHOR_NAME" \
    -o jsonpath='{.data.anchor\.json}')
printf '%s\n' "$ANCHOR_STATE" | grep -Eq \
    '"phase"[[:space:]]*:[[:space:]]*"ready"' || {
    fail "session anchor did not persist the ready phase"
}
REGISTRATION_SECRET=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.spec.volumes[?(@.name=="registration")].secret.secretName}')
[ -n "$REGISTRATION_SECRET" ] || fail "worker Pod has no registration Secret reference"
REGISTRATION_LOOKUP="$TEMPORARY_ROOT/registration-secret-lookup"
if kubectl -n "$WORKER_NAMESPACE" get secret "$REGISTRATION_SECRET" \
    >"$REGISTRATION_LOOKUP" 2>&1; then
    fail "one-shot registration Secret still exists"
fi
grep -Fq "$REGISTRATION_SECRET" "$REGISTRATION_LOOKUP" || {
    fail "registration Secret lookup failed for an unexpected object"
}
grep -Fq '(NotFound)' "$REGISTRATION_LOOKUP" || {
    sed -n '1,20p' "$REGISTRATION_LOOKUP" >&2 || true
    fail "registration Secret lookup failed for an unexpected reason"
}
REGISTRATION_SECRETS=$(kubectl -n "$WORKER_NAMESPACE" get secrets \
    -l 'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=registration-secret' \
    -o name)
[ -z "$REGISTRATION_SECRETS" ] || fail "another managed registration Secret still exists"

WORKER_IMAGE_OBSERVED=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.spec.containers[0].image}')
[ "$WORKER_IMAGE_OBSERVED" = "$WORKER_DIGEST" ] || {
    fail "worker Pod does not use the loaded digest-qualified image"
}

POD_AUTOMOUNT=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.spec.automountServiceAccountToken}')
[ "$POD_AUTOMOUNT" = 'false' ] || fail "worker Pod can automount a Kubernetes token"
WORKER_SERVICE_ACCOUNT=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.spec.serviceAccountName}')
[ -n "$WORKER_SERVICE_ACCOUNT" ] || fail "worker Pod has no private ServiceAccount"
SA_AUTOMOUNT=$(kubectl -n "$WORKER_NAMESPACE" get serviceaccount \
    "$WORKER_SERVICE_ACCOUNT" \
    -o jsonpath='{.automountServiceAccountToken}')
[ "$SA_AUTOMOUNT" = 'false' ] || {
    fail "worker ServiceAccount can automount a Kubernetes token"
}
kubectl -n "$WORKER_NAMESPACE" exec "$WORKER_POD" -- \
    /bin/sh -c 'test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token' || {
    fail "worker filesystem contains a Kubernetes API token"
}
PROJECTED_SERVICE_ACCOUNT_TOKENS=$(kubectl -n "$WORKER_NAMESPACE" get pod \
    "$WORKER_POD" \
    -o jsonpath='{range .spec.volumes[*].projected.sources[*]}{.serviceAccountToken.path}{"\n"}{end}')
[ -z "$PROJECTED_SERVICE_ACCOUNT_TOKENS" ] || {
    fail "worker Pod explicitly projects a Kubernetes API token"
}
WORKER_SECRET_VOLUMES=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{range .spec.volumes[*]}{.secret.secretName}{"\n"}{end}')
WORKER_PROJECTED_SECRET_VOLUMES=$(kubectl -n "$WORKER_NAMESPACE" get pod \
    "$WORKER_POD" \
    -o jsonpath='{range .spec.volumes[*].projected.sources[*]}{.secret.name}{"\n"}{end}')
for secret_name in $WORKER_SECRET_VOLUMES $WORKER_PROJECTED_SECRET_VOLUMES; do
    [ "$secret_name" = "$REGISTRATION_SECRET" ] && continue
    secret_type=$(kubectl -n "$WORKER_NAMESPACE" get secret "$secret_name" \
        -o jsonpath='{.type}')
    [ "$secret_type" != 'kubernetes.io/service-account-token' ] || {
        fail "worker Pod mounts a legacy Kubernetes API token Secret"
    }
done

WORKSPACE_PVC_RESOURCE=$(single_resource_name persistentvolumeclaim \
    'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=workspace-pvc' \
    'smoke-session workspace PVC')
WORKSPACE_PVC=${WORKSPACE_PVC_RESOURCE#persistentvolumeclaim/}
WORKER_NETWORK_POLICY_RESOURCE=$(single_resource_name networkpolicy \
    'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-network-policy' \
    'smoke-session worker NetworkPolicy')
WORKER_NETWORK_POLICY=${WORKER_NETWORK_POLICY_RESOURCE#*/}
WORKER_NETWORK_POLICY_UID=$(kubernetes_resource_uid networkpolicy \
    "$WORKER_NETWORK_POLICY" 'worker NetworkPolicy')
WORKER_POD_UID=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.metadata.uid}')
WORKSPACE_PVC_UID=$(kubectl -n "$WORKER_NAMESPACE" get persistentvolumeclaim \
    "$WORKSPACE_PVC" -o jsonpath='{.metadata.uid}')
WORKER_SERVICE_ACCOUNT_UID=$(kubectl -n "$WORKER_NAMESPACE" get serviceaccount \
    "$WORKER_SERVICE_ACCOUNT" -o jsonpath='{.metadata.uid}')
[ -n "$WORKER_POD_UID" ] || fail "worker Pod has no Kubernetes UID"
[ -n "$WORKSPACE_PVC_UID" ] || fail "workspace PVC has no Kubernetes UID"
[ -n "$WORKER_SERVICE_ACCOUNT_UID" ] || {
    fail "worker ServiceAccount has no Kubernetes UID"
}
assert_anchor_owner pod "$WORKER_POD" "$ANCHOR_NAME" "$ANCHOR_UID" 'worker Pod'
assert_anchor_owner persistentvolumeclaim "$WORKSPACE_PVC" \
    "$ANCHOR_NAME" "$ANCHOR_UID" 'workspace PVC'
assert_anchor_owner serviceaccount "$WORKER_SERVICE_ACCOUNT" \
    "$ANCHOR_NAME" "$ANCHOR_UID" 'worker ServiceAccount'
assert_anchor_owner networkpolicy "$WORKER_NETWORK_POLICY" \
    "$ANCHOR_NAME" "$ANCHOR_UID" 'worker NetworkPolicy'

RELAY_CA_IMMUTABLE=$(kubectl -n "$WORKER_NAMESPACE" get configmap \
    openab-kind-smoke-relay-ca -o jsonpath='{.immutable}')
[ "$RELAY_CA_IMMUTABLE" = 'true' ] || fail "worker relay CA is mutable"
WORKER_RELAY_CA_NAME=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD" \
    -o jsonpath='{.spec.volumes[?(@.name=="controller-ca")].configMap.name}')
[ "$WORKER_RELAY_CA_NAME" = 'openab-kind-smoke-relay-ca' ] || {
    fail "worker Pod does not mount the operator-owned relay CA"
}
RELAY_CA_OWNERS=$(kubectl -n "$WORKER_NAMESPACE" get configmap \
    openab-kind-smoke-relay-ca -o jsonpath='{.metadata.ownerReferences[*].uid}')
[ -z "$RELAY_CA_OWNERS" ] || fail "operator-owned worker relay CA has a session owner"

if [ "$MODE" = '--isolation' ]; then
    BRIDGE_B_FIFO="$TEMPORARY_ROOT/bridge-b.stdin"
    BRIDGE_B_STDOUT="$TEMPORARY_ROOT/bridge-b.stdout"
    BRIDGE_B_STDERR="$TEMPORARY_ROOT/bridge-b.stderr"
    mkfifo "$BRIDGE_B_FIFO"
    start_bridge \
        "$BRIDGE_B_FIFO" \
        "$BRIDGE_B_STDOUT" \
        "$BRIDGE_B_STDERR" \
        'discord:kind-smoke:thread-2' \
        '00000000-0000-0000-0000-000000000065' \
        absent
    BRIDGE_B_PID=$STARTED_BRIDGE_PID
    STARTED_BRIDGE_PID=''
    exec 4> "$BRIDGE_B_FIFO"
    BRIDGE_B_WRITER_OPEN=1

    send_bridge_request b 'bridge B initialization' "$INITIALIZE_LINE"
    wait_for_output 'bridge B initialization' '"id":1' \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 180
    grep -Fq 'openab-kubernetes-session-fake-acp' "$BRIDGE_B_STDOUT" || {
        fail "bridge B initialization did not reach the fake ACP worker"
    }
    send_bridge_request b 'fake ACP session B creation' "$SESSION_NEW_LINE"
    wait_for_output 'fake ACP session B creation' '"id":2' \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    grep -Fq 'openab-fake-session-v1' "$BRIDGE_B_STDOUT" || {
        fail "fake ACP session B did not return its fixed identity"
    }

    ANCHOR_B_RESOURCE=$(other_resource_name configmap \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=session-anchor' \
        "configmap/$ANCHOR_NAME" 'isolation-session anchors')
    ANCHOR_NAME_B=${ANCHOR_B_RESOURCE#configmap/}
    ANCHOR_UID_B=$(kubectl -n "$WORKER_NAMESPACE" get configmap "$ANCHOR_NAME_B" \
        -o jsonpath='{.metadata.uid}')
    [ -n "$ANCHOR_UID_B" ] || fail "session B anchor has no Kubernetes UID"
    ANCHOR_STATE_B=$(kubectl -n "$WORKER_NAMESPACE" get configmap "$ANCHOR_NAME_B" \
        -o jsonpath='{.data.anchor\.json}')
    printf '%s\n' "$ANCHOR_STATE_B" | grep -Eq \
        '"phase"[[:space:]]*:[[:space:]]*"ready"' || {
        fail "session B anchor did not persist the ready phase"
    }

    WORKER_POD_B=$(single_owned_resource_name pod \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-pod' \
        "$ANCHOR_UID_B" 'session B worker Pod')
    WORKSPACE_PVC_B=$(single_owned_resource_name persistentvolumeclaim \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=workspace-pvc' \
        "$ANCHOR_UID_B" 'session B workspace PVC')
    WORKER_SERVICE_ACCOUNT_B=$(single_owned_resource_name serviceaccount \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-service-account' \
        "$ANCHOR_UID_B" 'session B worker ServiceAccount')
    WORKER_NETWORK_POLICY_B=$(single_owned_resource_name networkpolicy \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-network-policy' \
        "$ANCHOR_UID_B" 'session B worker NetworkPolicy')
    WORKER_NETWORK_POLICY_UID_B=$(kubernetes_resource_uid networkpolicy \
        "$WORKER_NETWORK_POLICY_B" 'worker NetworkPolicy B')
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$WORKER_POD_B" --timeout=120s
    if ! kill -0 "$BRIDGE_A_PID" >/dev/null 2>&1; then
        fail "session A bridge did not remain active while session B started"
    fi
    if ! kill -0 "$BRIDGE_B_PID" >/dev/null 2>&1; then
        fail "session B bridge did not remain active after session creation"
    fi
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$WORKER_POD" --timeout=30s
    CURRENT_WORKER_POD_UID=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD" -o jsonpath='{.metadata.uid}')
    [ "$CURRENT_WORKER_POD_UID" = "$WORKER_POD_UID" ] || {
        fail "session A worker Pod was replaced while session B started"
    }
    CURRENT_ANCHOR_STATE=$(kubectl -n "$WORKER_NAMESPACE" get configmap \
        "$ANCHOR_NAME" -o jsonpath='{.data.anchor\.json}')
    printf '%s\n' "$CURRENT_ANCHOR_STATE" | grep -Eq \
        '"phase"[[:space:]]*:[[:space:]]*"ready"' || {
        fail "session A did not remain ready while session B started"
    }

    WORKER_POD_UID_B=$(kubectl -n "$WORKER_NAMESPACE" get pod "$WORKER_POD_B" \
        -o jsonpath='{.metadata.uid}')
    WORKSPACE_PVC_UID_B=$(kubectl -n "$WORKER_NAMESPACE" get persistentvolumeclaim \
        "$WORKSPACE_PVC_B" -o jsonpath='{.metadata.uid}')
    WORKER_SERVICE_ACCOUNT_UID_B=$(kubectl -n "$WORKER_NAMESPACE" get serviceaccount \
        "$WORKER_SERVICE_ACCOUNT_B" -o jsonpath='{.metadata.uid}')
    [ -n "$WORKER_POD_UID_B" ] || fail "session B worker Pod has no Kubernetes UID"
    [ -n "$WORKSPACE_PVC_UID_B" ] || fail "session B workspace PVC has no Kubernetes UID"
    [ -n "$WORKER_SERVICE_ACCOUNT_UID_B" ] || {
        fail "session B worker ServiceAccount has no Kubernetes UID"
    }
    POD_SERVICE_ACCOUNT_B=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD_B" -o jsonpath='{.spec.serviceAccountName}')
    [ "$POD_SERVICE_ACCOUNT_B" = "$WORKER_SERVICE_ACCOUNT_B" ] || {
        fail "worker Pod B does not use its anchor-owned ServiceAccount"
    }

    if [ "$WORKER_POD" = "$WORKER_POD_B" ] || \
        [ "$WORKER_POD_UID" = "$WORKER_POD_UID_B" ]; then
        fail "two sessions unexpectedly share a worker Pod"
    fi
    if [ "$WORKSPACE_PVC" = "$WORKSPACE_PVC_B" ] || \
        [ "$WORKSPACE_PVC_UID" = "$WORKSPACE_PVC_UID_B" ]; then
        fail "two sessions unexpectedly share a workspace PVC"
    fi
    if [ "$WORKER_SERVICE_ACCOUNT" = "$WORKER_SERVICE_ACCOUNT_B" ] || \
        [ "$WORKER_SERVICE_ACCOUNT_UID" = "$WORKER_SERVICE_ACCOUNT_UID_B" ]; then
        fail "two sessions unexpectedly share a ServiceAccount"
    fi

    assert_anchor_owner pod "$WORKER_POD_B" \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" 'worker Pod B'
    assert_anchor_owner persistentvolumeclaim "$WORKSPACE_PVC_B" \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" 'workspace PVC B'
    assert_anchor_owner serviceaccount "$WORKER_SERVICE_ACCOUNT_B" \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" 'worker ServiceAccount B'
    assert_anchor_owner networkpolicy "$WORKER_NETWORK_POLICY_B" \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" 'worker NetworkPolicy B'
    assert_worker_pod_isolation_contract \
        "$WORKER_POD" "$WORKSPACE_PVC" "$WORKSPACE_PVC_B" \
        'worker Pod A' "session B's"
    assert_worker_pod_isolation_contract \
        "$WORKER_POD_B" "$WORKSPACE_PVC_B" "$WORKSPACE_PVC" \
        'worker Pod B' "session A's"
    assert_worker_shared_skills "$WORKER_POD_B"
    assert_worker_network_policy_contract \
        a "$WORKER_NETWORK_POLICY" "$WORKER_POD" "$WORKER_POD_B" \
        'session A'
    assert_worker_network_policy_contract \
        b "$WORKER_NETWORK_POLICY_B" "$WORKER_POD_B" "$WORKER_POD" \
        'session B'
    assert_worker_relay_network_policy_contract "$WORKER_POD" "$WORKER_POD_B"

    WORKER_IMAGE_OBSERVED_B=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD_B" -o jsonpath='{.spec.containers[0].image}')
    [ "$WORKER_IMAGE_OBSERVED_B" = "$WORKER_DIGEST" ] || {
        fail "worker Pod B does not use the loaded digest-qualified image"
    }
    SA_AUTOMOUNT_B=$(kubectl -n "$WORKER_NAMESPACE" get serviceaccount \
        "$WORKER_SERVICE_ACCOUNT_B" \
        -o jsonpath='{.automountServiceAccountToken}')
    [ "$SA_AUTOMOUNT_B" = 'false' ] || {
        fail "worker ServiceAccount B can automount a Kubernetes token"
    }
    kubectl -n "$WORKER_NAMESPACE" exec "$WORKER_POD_B" -- \
        /bin/sh -c 'test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token' || {
        fail "worker B filesystem contains a Kubernetes API token"
    }
    REGISTRATION_SECRET_B=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD_B" \
        -o jsonpath='{.spec.volumes[?(@.name=="registration")].secret.secretName}')
    [ -n "$REGISTRATION_SECRET_B" ] || {
        fail "worker Pod B has no registration Secret reference"
    }
    [ "$REGISTRATION_SECRET" != "$REGISTRATION_SECRET_B" ] || {
        fail "two sessions unexpectedly reuse a registration Secret name"
    }
    REGISTRATION_LOOKUP_B="$TEMPORARY_ROOT/registration-secret-b-lookup"
    if kubectl -n "$WORKER_NAMESPACE" get secret "$REGISTRATION_SECRET_B" \
        > "$REGISTRATION_LOOKUP_B" 2>&1; then
        fail "session B one-shot registration Secret still exists"
    fi
    grep -Fq '(NotFound)' "$REGISTRATION_LOOKUP_B" || {
        sed -n '1,20p' "$REGISTRATION_LOOKUP_B" >&2 || true
        fail "session B registration Secret lookup failed unexpectedly"
    }
    REGISTRATION_SECRETS_AFTER_B=$(kubectl -n "$WORKER_NAMESPACE" get secrets \
        -l 'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=registration-secret' \
        -o name)
    [ -z "$REGISTRATION_SECRETS_AFTER_B" ] || {
        fail "a managed registration Secret remains after both workers registered"
    }

    WORKSPACE_A_WRITE_V1=$(fixture_request 101)
    WORKSPACE_B_READ_BEFORE_WRITE=$(fixture_request 201)
    WORKSPACE_B_WRITE_V1=$(fixture_request 202)
    WORKSPACE_A_READ_V1=$(fixture_request 102)
    WORKSPACE_A_WRITE_V2=$(fixture_request 103)
    WORKSPACE_B_READ_V1=$(fixture_request 203)
    WORKSPACE_A_MARKER_V1=$(printf '%s\n' "$WORKSPACE_A_WRITE_V1" | \
        jq -r '.params.content')
    WORKSPACE_A_MARKER_V2=$(printf '%s\n' "$WORKSPACE_A_WRITE_V2" | \
        jq -r '.params.content')
    WORKSPACE_B_MARKER_V1=$(printf '%s\n' "$WORKSPACE_B_WRITE_V1" | \
        jq -r '.params.content')

    send_bridge_request a 'session A initial workspace write' \
        "$WORKSPACE_A_WRITE_V1"
    wait_for_rpc_response 'session A initial workspace write' 101 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_empty_result \
        'session A could not write its private workspace state' \
        "$BRIDGE_A_STDOUT" 101

    send_bridge_request b 'session B pre-write workspace read' \
        "$WORKSPACE_B_READ_BEFORE_WRITE"
    wait_for_rpc_response 'session B pre-write workspace read' 201 \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    assert_rpc_error \
        'session B unexpectedly observed session A workspace state before its own write' \
        "$BRIDGE_B_STDOUT" 201 -32001 'Workspace probe failed'

    send_bridge_request b 'session B initial workspace write' \
        "$WORKSPACE_B_WRITE_V1"
    wait_for_rpc_response 'session B initial workspace write' 202 \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    assert_rpc_empty_result \
        'session B could not write its private workspace state' \
        "$BRIDGE_B_STDOUT" 202

    send_bridge_request a 'session A workspace read after B write' \
        "$WORKSPACE_A_READ_V1"
    wait_for_rpc_response 'session A workspace read after B write' 102 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_content_result \
        'session B workspace write changed session A state' \
        "$BRIDGE_A_STDOUT" 102 "$WORKSPACE_A_MARKER_V1"

    send_bridge_request a 'session A workspace overwrite' \
        "$WORKSPACE_A_WRITE_V2"
    wait_for_rpc_response 'session A workspace overwrite' 103 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_empty_result \
        'session A could not overwrite its private workspace state' \
        "$BRIDGE_A_STDOUT" 103

    send_bridge_request b 'session B workspace read after A overwrite' \
        "$WORKSPACE_B_READ_V1"
    wait_for_rpc_response 'session B workspace read after A overwrite' 203 \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    assert_rpc_content_result \
        'session A workspace overwrite changed session B state' \
        "$BRIDGE_B_STDOUT" 203 "$WORKSPACE_B_MARKER_V1"

    if ! kill -0 "$BRIDGE_A_PID" >/dev/null 2>&1; then
        fail "session A bridge stopped during workspace isolation probes"
    fi
    if ! kill -0 "$BRIDGE_B_PID" >/dev/null 2>&1; then
        fail "session B bridge stopped during workspace isolation probes"
    fi
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$WORKER_POD" --timeout=30s
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$WORKER_POD_B" --timeout=30s
    FINAL_WORKER_POD_UID=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD" -o jsonpath='{.metadata.uid}')
    FINAL_WORKER_POD_UID_B=$(kubectl -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD_B" -o jsonpath='{.metadata.uid}')
    [ "$FINAL_WORKER_POD_UID" = "$WORKER_POD_UID" ] || {
        fail "session A worker was replaced during workspace isolation probes"
    }
    [ "$FINAL_WORKER_POD_UID_B" = "$WORKER_POD_UID_B" ] || {
        fail "session B worker was replaced during workspace isolation probes"
    }

    A_PRE_FAILURE_PROMPT=$(lifecycle_request 301)
    A_REPLACEMENT_LOAD=$(lifecycle_request 310)
    A_REPLACEMENT_WORKSPACE_READ=$(lifecycle_request 311)
    A_REPLACEMENT_PROMPT=$(lifecycle_request 312)
    B_POST_REPLACEMENT_WORKSPACE_READ=$(lifecycle_request 499)

    send_bridge_request a 'session A pre-failure prompt' \
        "$A_PRE_FAILURE_PROMPT"
    wait_for_rpc_response 'session A pre-failure prompt' 301 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_prompt_result \
        'session A pre-failure prompt returned an unexpected result' \
        "$BRIDGE_A_STDOUT" 301
    refresh_session_b 401 'session B pre-failure prompt'

    ANCHOR_BASELINE_A="$TEMPORARY_ROOT/anchor-baseline-a.json"
    ANCHOR_BASELINE_B="$TEMPORARY_ROOT/anchor-baseline-b.json"
    wait_for_anchor_state \
        "$ANCHOR_NAME" "$ANCHOR_UID" ready "$WORKER_POD_UID" \
        "$ANCHOR_BASELINE_A" 30
    wait_for_anchor_state \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" ready "$WORKER_POD_UID_B" \
        "$ANCHOR_BASELINE_B" 30
    SESSION_ID_A=$(jq -r '.state.sessionId' "$ANCHOR_BASELINE_A")
    SESSION_ID_B=$(jq -r '.state.sessionId' "$ANCHOR_BASELINE_B")
    [ "$SESSION_ID_A" != "$SESSION_ID_B" ] || {
        fail "two thread mappings unexpectedly share a logical session identity"
    }
    CHILDREN_BASELINE_B="$TEMPORARY_ROOT/children-baseline-b.json"
    assert_active_session_children \
        "$SESSION_ID_B" "$CHILDREN_BASELINE_B" \
        "$WORKER_NETWORK_POLICY_B" "$WORKER_NETWORK_POLICY_UID_B" \
        "$WORKSPACE_PVC_B" "$WORKSPACE_PVC_UID_B" \
        "$WORKER_POD_B" "$WORKER_POD_UID_B" \
        "$WORKER_SERVICE_ACCOUNT_B" "$WORKER_SERVICE_ACCOUNT_UID_B" \
        'session B baseline children did not match its private generation'

    if ! kill -0 "$BRIDGE_A_PID" >/dev/null 2>&1; then
        fail "session A bridge died before its worker Pod failure was injected"
    fi
    POD_BEFORE_FAILURE_A="$TEMPORARY_ROOT/pod-before-failure-a.json"
    kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get pod \
        "$WORKER_POD" -o json > "$POD_BEFORE_FAILURE_A"
    jq -e --arg pod_uid "$WORKER_POD_UID" '
        .metadata.uid == $pod_uid
        and .metadata.deletionTimestamp == null
        and any(
            .status.conditions[]?;
            .type == "Ready" and .status == "True"
        )
    ' "$POD_BEFORE_FAILURE_A" >/dev/null || {
        fail "session A worker Pod was not live before failure injection"
    }
    kubectl --request-timeout=70s -n "$WORKER_NAMESPACE" delete pod \
        "$WORKER_POD" --wait=true --timeout=60s
    if wait_for_process_exit "$BRIDGE_A_PID" 60; then
        :
    else
        sed -n '1,80p' "$BRIDGE_A_STDERR" >&2 || true
        fail "session A bridge did not exit after its worker Pod was deleted"
    fi
    BRIDGE_A_FAILURE_EXIT_STATUS=$BRIDGE_EXIT_STATUS
    exec 3>&-
    BRIDGE_A_WRITER_OPEN=0
    BRIDGE_A_PID=''
    [ "$BRIDGE_A_FAILURE_EXIT_STATUS" -ne 0 ] || {
        fail "session A bridge did not exit after its worker Pod was deleted"
    }
    refresh_session_b 402 'session B keepalive after worker A failed'

    ANCHOR_BLOCKED_A="$TEMPORARY_ROOT/anchor-blocked-a.json"
    wait_for_anchor_state \
        "$ANCHOR_NAME" "$ANCHOR_UID" blocked '' \
        "$ANCHOR_BLOCKED_A" 120
    refresh_session_b 403 'session B keepalive after worker A was blocked'
    wait_for_exact_uid_absent pods "$WORKER_POD_UID" \
        a-generation-1-pod 60
    wait_for_exact_uid_absent serviceaccounts "$WORKER_SERVICE_ACCOUNT_UID" \
        a-generation-1-service-account 60
    wait_for_exact_uid_absent networkpolicies.networking.k8s.io \
        "$WORKER_NETWORK_POLICY_UID" a-generation-1-network-policy 60
    BLOCKED_CHILDREN_A="$TEMPORARY_ROOT/blocked-children-a.json"
    assert_retained_session_children \
        "$SESSION_ID_A" "$BLOCKED_CHILDREN_A" \
        "$WORKSPACE_PVC" "$WORKSPACE_PVC_UID" \
        'session A blocked cleanup retained generation-scoped compute'
    refresh_session_b 404 'session B keepalive after worker A cleanup'

    ANCHOR_AFTER_BLOCK_B="$TEMPORARY_ROOT/anchor-after-block-b.json"
    wait_for_anchor_state \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" ready "$WORKER_POD_UID_B" \
        "$ANCHOR_AFTER_BLOCK_B" 30
    if ! kill -0 "$BRIDGE_B_PID" >/dev/null 2>&1; then
        fail "session A replacement affected session B"
    fi
    assert_anchor_stable_identity \
        "$ANCHOR_AFTER_BLOCK_B" "$ANCHOR_BASELINE_B" \
        'session A replacement affected session B'

    BRIDGE_A_FIFO="$TEMPORARY_ROOT/bridge-a-generation-2.stdin"
    BRIDGE_A_STDOUT="$TEMPORARY_ROOT/bridge-a-generation-2.stdout"
    BRIDGE_A_STDERR="$TEMPORARY_ROOT/bridge-a-generation-2.stderr"
    mkfifo "$BRIDGE_A_FIFO"
    start_bridge \
        "$BRIDGE_A_FIFO" \
        "$BRIDGE_A_STDOUT" \
        "$BRIDGE_A_STDERR" \
        'discord:kind-smoke:thread-1' \
        '00000000-0000-0000-0000-000000000066' \
        present
    BRIDGE_A_PID=$STARTED_BRIDGE_PID
    STARTED_BRIDGE_PID=''
    exec 3> "$BRIDGE_A_FIFO"
    BRIDGE_A_WRITER_OPEN=1

    send_bridge_request a 'session A replacement initialization' \
        "$INITIALIZE_LINE"
    wait_for_output 'session A replacement initialization' '"id":1' \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 180
    refresh_session_b 405 'session B keepalive after worker A2 initialized'
    send_bridge_request a 'session A replacement load' \
        "$A_REPLACEMENT_LOAD"
    wait_for_rpc_response 'session A replacement load' 310 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_empty_result \
        'session A replacement could not load the retained ACP session' \
        "$BRIDGE_A_STDOUT" 310

    WORKER_POD_A2=$(single_owned_resource_name pod \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-pod' \
        "$ANCHOR_UID" 'session A replacement worker Pod')
    WORKSPACE_PVC_A2=$(single_owned_resource_name persistentvolumeclaim \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=workspace-pvc' \
        "$ANCHOR_UID" 'session A retained workspace PVC')
    WORKER_SERVICE_ACCOUNT_A2=$(single_owned_resource_name serviceaccount \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-service-account' \
        "$ANCHOR_UID" 'session A replacement worker ServiceAccount')
    WORKER_NETWORK_POLICY_A2=$(single_owned_resource_name networkpolicy \
        'app.kubernetes.io/managed-by=openab-session-controller,openab.dev/resource=worker-network-policy' \
        "$ANCHOR_UID" 'session A replacement worker NetworkPolicy')
    kubectl -n "$WORKER_NAMESPACE" wait \
        --for=condition=Ready "pod/$WORKER_POD_A2" --timeout=120s
    WORKER_POD_UID_A2=$(kubernetes_resource_uid pod \
        "$WORKER_POD_A2" 'session A replacement worker Pod')
    WORKSPACE_PVC_UID_A2=$(kubernetes_resource_uid persistentvolumeclaim \
        "$WORKSPACE_PVC_A2" 'session A retained workspace PVC')
    WORKER_SERVICE_ACCOUNT_UID_A2=$(kubernetes_resource_uid serviceaccount \
        "$WORKER_SERVICE_ACCOUNT_A2" 'session A replacement ServiceAccount')
    WORKER_NETWORK_POLICY_UID_A2=$(kubernetes_resource_uid networkpolicy \
        "$WORKER_NETWORK_POLICY_A2" 'session A replacement NetworkPolicy')

    ANCHOR_REPLACEMENT_A="$TEMPORARY_ROOT/anchor-replacement-a.json"
    wait_for_anchor_state \
        "$ANCHOR_NAME" "$ANCHOR_UID" ready "$WORKER_POD_UID_A2" \
        "$ANCHOR_REPLACEMENT_A" 60
    refresh_session_b 406 'session B keepalive after worker A2 became ready'
    if ! jq -e \
        --slurpfile baseline "$ANCHOR_BASELINE_A" \
        --arg attempt_id '00000000-0000-0000-0000-000000000066' \
        --arg pod_uid "$WORKER_POD_UID_A2" '
        .uid == $baseline[0].uid
        and .state.sessionId == $baseline[0].state.sessionId
        and .state.scopeId == $baseline[0].state.scopeId
        and .state.profile == $baseline[0].state.profile
        and .state.incarnationId == $baseline[0].state.incarnationId
        and .state.fence.generation
            == ($baseline[0].state.fence.generation + 1)
        and .state.fence.attemptId == $attempt_id
        and .state.fence.attemptId
            != $baseline[0].state.fence.attemptId
        and .state.phase == "ready"
        and .state.podUid == $pod_uid
    ' "$ANCHOR_REPLACEMENT_A" >/dev/null; then
        fail "session A replacement did not advance exactly one generation"
    fi
    if [ "$WORKSPACE_PVC_A2" != "$WORKSPACE_PVC" ] || \
        [ "$WORKSPACE_PVC_UID_A2" != "$WORKSPACE_PVC_UID" ] || \
        [ "$WORKER_POD_UID_A2" = "$WORKER_POD_UID" ] || \
        [ "$WORKER_SERVICE_ACCOUNT_UID_A2" = "$WORKER_SERVICE_ACCOUNT_UID" ] || \
        [ "$WORKER_NETWORK_POLICY_UID_A2" = "$WORKER_NETWORK_POLICY_UID" ]; then
        fail "session A replacement changed its logical identity or PVC"
    fi
    assert_anchor_owner pod "$WORKER_POD_A2" \
        "$ANCHOR_NAME" "$ANCHOR_UID" 'replacement worker Pod A'
    assert_anchor_owner serviceaccount "$WORKER_SERVICE_ACCOUNT_A2" \
        "$ANCHOR_NAME" "$ANCHOR_UID" 'replacement worker ServiceAccount A'
    assert_anchor_owner networkpolicy "$WORKER_NETWORK_POLICY_A2" \
        "$ANCHOR_NAME" "$ANCHOR_UID" 'replacement worker NetworkPolicy A'
    assert_worker_pod_isolation_contract \
        "$WORKER_POD_A2" "$WORKSPACE_PVC_A2" "$WORKSPACE_PVC_B" \
        'replacement worker Pod A' "session B's"
    assert_worker_shared_skills "$WORKER_POD_A2"
    assert_worker_network_policy_contract \
        a2 "$WORKER_NETWORK_POLICY_A2" "$WORKER_POD_A2" "$WORKER_POD_B" \
        'session A replacement'

    REPLACEMENT_CHILDREN_A="$TEMPORARY_ROOT/replacement-children-a.json"
    assert_active_session_children \
        "$SESSION_ID_A" "$REPLACEMENT_CHILDREN_A" \
        "$WORKER_NETWORK_POLICY_A2" "$WORKER_NETWORK_POLICY_UID_A2" \
        "$WORKSPACE_PVC_A2" "$WORKSPACE_PVC_UID_A2" \
        "$WORKER_POD_A2" "$WORKER_POD_UID_A2" \
        "$WORKER_SERVICE_ACCOUNT_A2" "$WORKER_SERVICE_ACCOUNT_UID_A2" \
        'session A replacement retained an unexpected child resource'

    send_bridge_request a 'session A replacement workspace read' \
        "$A_REPLACEMENT_WORKSPACE_READ"
    wait_for_rpc_response 'session A replacement workspace read' 311 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_content_result \
        'session A replacement did not retain its workspace state' \
        "$BRIDGE_A_STDOUT" 311 "$WORKSPACE_A_MARKER_V2"
    send_bridge_request a 'session A replacement prompt' \
        "$A_REPLACEMENT_PROMPT"
    wait_for_rpc_response 'session A replacement prompt' 312 \
        "$BRIDGE_A_STDOUT" "$BRIDGE_A_PID" "$BRIDGE_A_STDERR" 60
    assert_rpc_prompt_result \
        'session A replacement prompt returned an unexpected result' \
        "$BRIDGE_A_STDOUT" 312

    send_bridge_request b 'session B post-replacement workspace read' \
        "$B_POST_REPLACEMENT_WORKSPACE_READ"
    wait_for_rpc_response 'session B post-replacement workspace read' 499 \
        "$BRIDGE_B_STDOUT" "$BRIDGE_B_PID" "$BRIDGE_B_STDERR" 60
    assert_rpc_content_result \
        'session A replacement affected session B' \
        "$BRIDGE_B_STDOUT" 499 "$WORKSPACE_B_MARKER_V1"
    ANCHOR_POST_REPLACEMENT_B="$TEMPORARY_ROOT/anchor-post-replacement-b.json"
    wait_for_anchor_state \
        "$ANCHOR_NAME_B" "$ANCHOR_UID_B" ready "$WORKER_POD_UID_B" \
        "$ANCHOR_POST_REPLACEMENT_B" 30
    assert_anchor_stable_identity \
        "$ANCHOR_POST_REPLACEMENT_B" "$ANCHOR_BASELINE_B" \
        'session A replacement affected session B'
    CHILDREN_POST_REPLACEMENT_B="$TEMPORARY_ROOT/children-post-replacement-b.json"
    snapshot_session_children \
        "$SESSION_ID_B" "$CHILDREN_POST_REPLACEMENT_B"
    assert_session_children_unchanged \
        "$CHILDREN_POST_REPLACEMENT_B" "$CHILDREN_BASELINE_B" \
        'session A replacement affected session B'
    if ! kill -0 "$BRIDGE_A_PID" >/dev/null 2>&1 || \
        ! kill -0 "$BRIDGE_B_PID" >/dev/null 2>&1; then
        fail "session A replacement affected session B"
    fi

fi

SERVICE_TYPE=$(kubectl -n "$SYSTEM_NAMESPACE" get service "$CONTROLLER_NAME" \
    -o jsonpath='{.spec.type}')
[ "$SERVICE_TYPE" = 'ClusterIP' ] || fail "controller Service is publicly exposed"
NODE_PORT=$(kubectl -n "$SYSTEM_NAMESPACE" get service "$CONTROLLER_NAME" \
    -o jsonpath='{.spec.ports[0].nodePort}')
[ -z "$NODE_PORT" ] || fail "controller Service allocated a NodePort"
PUBLIC_INGRESS=$(kubectl -n "$SYSTEM_NAMESPACE" get service "$CONTROLLER_NAME" \
    -o jsonpath='{.status.loadBalancer.ingress}')
[ -z "$PUBLIC_INGRESS" ] || fail "controller Service has public load-balancer ingress"
EXTERNAL_IPS=$(kubectl -n "$SYSTEM_NAMESPACE" get service "$CONTROLLER_NAME" \
    -o jsonpath='{.spec.externalIPs}')
[ -z "$EXTERNAL_IPS" ] || fail "controller Service has external IPs"
WORKER_SERVICES_SNAPSHOT="$TEMPORARY_ROOT/worker-services.json"
kubectl --request-timeout=5s -n "$WORKER_NAMESPACE" get services -o json \
    > "$WORKER_SERVICES_SNAPSHOT"
jq -e '
    type == "object"
    and (.items | (type == "array" and length == 0))
' "$WORKER_SERVICES_SNAPSHOT" >/dev/null || {
    fail "worker namespace exposes a Service"
}

COMPLETED=1
if [ "$MODE" = '--isolation' ]; then
    printf '%s\n' 'kubernetes-session Kind test: isolation checks passed'
else
    printf '%s\n' 'kubernetes-session Kind test: smoke checks passed'
fi
