#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "${0%/*}" && pwd)
TARGET="$SCRIPT_DIR/test-kubernetes-session-kind.sh"
PROFILE_FIXTURE="$SCRIPT_DIR/../tests/fixtures/kubernetes-session-kind/profiles.toml.in"
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
    for command_name in docker kind helm kubectl openssl git; do
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

[ -f "$TARGET" ] || fail "missing scripts/test-kubernetes-session-kind.sh"
[ -f "$PROFILE_FIXTURE" ] || fail "missing Kind worker profile fixture"

grep -Fq 'cidr = "192.0.2.1/32"' "$PROFILE_FIXTURE" || {
    fail "Kind profile must use a non-relay documentation egress target"
}
if grep -Fq 'port = 8443' "$PROFILE_FIXTURE"; then
    fail "Kind profile must leave relay egress to the static chart policy"
fi

unknown_stdout="$TEMPORARY_ROOT/unknown.stdout"
unknown_stderr="$TEMPORARY_ROOT/unknown.stderr"
if "$TARGET" --unknown >"$unknown_stdout" 2>"$unknown_stderr"; then
    fail "unknown mode unexpectedly succeeded"
fi
[ ! -s "$unknown_stdout" ] || fail "unknown mode wrote unexpected stdout"
grep -Fqx "kubernetes-session Kind test: usage: $TARGET [--check|--smoke]" \
    "$unknown_stderr" || fail "unknown mode did not emit the exact usage failure"

for missing in docker kind helm kubectl openssl git; do
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

printf '%s\n' 'kubernetes-session Kind contract test: all checks passed'
