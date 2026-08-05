#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
DOCKERFILE="$REPOSITORY_ROOT/Dockerfile.kubernetes-session"
MODE=${1:-all}

fail() {
    printf '%s\n' "kubernetes-session image test: $*" >&2
    exit 1
}

require_exact_line() {
    expected=$1
    grep -Fqx "$expected" "$DOCKERFILE" || fail "missing Dockerfile contract: $expected"
}

static_checks() {
    [ -f "$DOCKERFILE" ] || fail "Dockerfile.kubernetes-session is unavailable"

    require_exact_line 'FROM rust:1-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS session-source'
    require_exact_line 'FROM rust:1-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS openab-builder'
    require_exact_line 'ARG RUNTIME_IMAGE=debian:trixie-slim@sha256:3a39a0592364683e6bab97937b72cad5a8fa6dcbbee90edb3bb48c7f8e94f258'
    require_exact_line 'FROM session-runtime AS broker'
    require_exact_line 'FROM session-runtime AS controller'
    require_exact_line 'FROM session-runtime AS worker-base'
    require_exact_line 'FROM worker-base AS worker-test'
    require_exact_line 'USER 1000:1000'
    require_exact_line 'ENTRYPOINT ["/usr/bin/tini", "--"]'

    if grep -Eq '^[[:space:]]*(ADD[[:space:]]+https?://|COPY[[:space:]]+\.[[:space:]]+\.)' "$DOCKERFILE"; then
        fail "Dockerfile must not use remote ADD or an unscoped COPY . ."
    fi
    if grep -Eq '^COPY[[:space:]].*--chown=' "$DOCKERFILE"; then
        fail "runtime executables must remain root-owned"
    fi
}

require_docker() {
    command -v docker >/dev/null 2>&1 || fail "docker is required for image checks"
    docker info >/dev/null 2>&1 || fail "a running Docker daemon is required for image checks"
}

assert_equal() {
    description=$1
    expected=$2
    actual=$3
    [ "$actual" = "$expected" ] || {
        printf '%s\nexpected:\n%s\nactual:\n%s\n' "$description" "$expected" "$actual" >&2
        exit 1
    }
}

assert_image_contract() {
    image=$1
    expected_inventory=$2

    actual_user=$(docker image inspect --format '{{.Config.User}}' "$image")
    assert_equal "$image user" '1000:1000' "$actual_user"

    actual_entrypoint=$(docker image inspect --format '{{json .Config.Entrypoint}}' "$image")
    assert_equal "$image entrypoint" '["/usr/bin/tini","--"]' "$actual_entrypoint"

    actual_inventory=$(docker run --rm --read-only --entrypoint /bin/sh "$image" -c '
        for executable in /usr/local/bin/*; do
            if [ -f "$executable" ] && [ -x "$executable" ]; then
                test "$(stat -c %u:%g "$executable")" = 0:0
                basename "$executable"
            fi
        done
    ' | LC_ALL=C sort)
    assert_equal "$image executable inventory" "$expected_inventory" "$actual_inventory"

    docker run --rm --read-only --entrypoint /bin/sh "$image" -c '
        test "$(id -u)" = 1000
        test "$(id -g)" = 1000
        test -x /usr/bin/tini
        test -r /etc/ssl/certs/ca-certificates.crt
    '
}

build_checks() {
    require_docker

    temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/openab-session-images.XXXXXX")
    context="$temporary_root/context"
    manifest="$temporary_root/files"
    mkdir -p "$context"

    broker_image="openab-session-broker-smoke:$$"
    controller_image="openab-session-controller-smoke:$$"
    worker_image="openab-session-worker-smoke:$$"
    worker_test_image="openab-session-worker-test-smoke:$$"

    cleanup() {
        docker image rm -f "$broker_image" "$controller_image" "$worker_image" "$worker_test_image" >/dev/null 2>&1 || true
        rm -rf "$temporary_root"
    }
    trap cleanup EXIT HUP INT TERM

    # Never send arbitrary repository or untracked content to the Docker
    # daemon. These pathspecs are the complete source inputs referenced by the
    # Dockerfile; tests, local credentials, Git metadata, and target output are
    # deliberately outside the context. A new source file must be staged first
    # so the clean-context build cannot silently leak an unrelated local file.
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
    untracked_build_inputs=$(git -C "$REPOSITORY_ROOT" ls-files --others --exclude-standard -- "$@")
    [ -z "$untracked_build_inputs" ] || fail "new image source files must be staged before a clean-context build"
    git -C "$REPOSITORY_ROOT" ls-files --cached -z -- "$@" > "$manifest"
    cp "$DOCKERFILE" "$context/Dockerfile.kubernetes-session"
    (
        cd "$REPOSITORY_ROOT"
        tar --null -cf - -T "$manifest"
    ) | (
        cd "$context"
        tar -xf -
    )
    [ ! -e "$context/.git" ] || fail "clean build context must not contain .git"
    [ ! -e "$context/crates/openab-kubernetes-session/target" ] || fail "clean build context must not contain Cargo target output"

    docker build -f "$context/Dockerfile.kubernetes-session" --target broker -t "$broker_image" "$context"
    docker build -f "$context/Dockerfile.kubernetes-session" --target controller -t "$controller_image" "$context"
    docker build -f "$context/Dockerfile.kubernetes-session" --target worker-base -t "$worker_image" "$context"
    docker build -f "$context/Dockerfile.kubernetes-session" --target worker-test -t "$worker_test_image" "$context"

    assert_image_contract "$broker_image" "openab
openab-kubernetes-session"
    assert_image_contract "$controller_image" 'openab-kubernetes-session-controller'
    assert_image_contract "$worker_image" 'openab-kubernetes-session-worker'
    assert_image_contract "$worker_test_image" "openab-kubernetes-session-fake-acp
openab-kubernetes-session-worker"

    for image in "$worker_image" "$worker_test_image"; do
        docker run --rm --read-only --entrypoint /bin/sh "$image" -c '
            test "$HOME" = /session/home
            cut -d: -f3,6 /etc/passwd | grep -Fx "1000:/session/home"
        '
    done

    docker run --rm --read-only --entrypoint /usr/local/bin/openab-kubernetes-session-fake-acp "$worker_test_image" </dev/null
}

case "$MODE" in
    --static)
        static_checks
        ;;
    --build|all)
        static_checks
        build_checks
        ;;
    *)
        fail "usage: $0 [--static|--build]"
        ;;
esac
