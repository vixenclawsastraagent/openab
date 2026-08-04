#![cfg(feature = "controller-runtime")]

use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::controller::{resolve_profile_revisions, ProfileResolutionError};
use openab_kubernetes_session::profile_config::TrustedControllerConfigV1;
use serde_json::{json, Value};
use std::error::Error as _;
use std::time::Duration;
use tower_test::mock;

const NAMESPACE: &str = "team-a-workers";

#[derive(Clone, Copy, Default)]
struct References<'a> {
    runtime_class: Option<(&'a str, &'a str)>,
    skills: Option<&'a str>,
}

fn revision(profile: &str, version: &str, references: References<'_>) -> String {
    let runtime_class = references
        .runtime_class
        .map_or_else(String::new, |(name, handler)| {
            format!(
                r#"
[profiles.{profile}.revisions."{version}".runtime_class]
name = "{name}"
expected_handler = "{handler}"
"#,
            )
        });
    let skills = references.skills.map_or_else(String::new, |name| {
        format!(
            r#"
[profiles.{profile}.revisions."{version}".skills]
config_map_name = "{name}"
"#,
        )
    });

    format!(
        r#"
[profiles.{profile}.revisions."{version}"]
image = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[profiles.{profile}.revisions."{version}".relay]
url = "wss://openab-session-controller.openab-system.svc:8443/v1/worker"
ca_config_map_name = "openab-session-controller-ca-v1"

[profiles.{profile}.revisions."{version}".supervisor]
executable = "/usr/local/bin/openab-session-supervisor"
args = ["serve"]

[profiles.{profile}.revisions."{version}".workspace]
size = "20Gi"
storage_class = "encrypted-rwo"
access_mode = "read_write_once_pod"

[profiles.{profile}.revisions."{version}".resources.requests]
cpu = "250m"
memory = "256Mi"
ephemeral_storage = "1Gi"

[profiles.{profile}.revisions."{version}".resources.limits]
cpu = "1"
memory = "2Gi"
ephemeral_storage = "8Gi"

[profiles.{profile}.revisions."{version}".run_as]
uid = 10001
gid = 10001

[[profiles.{profile}.revisions."{version}".egress]]
target = "cidr"
cidr = "10.96.0.10/32"

[[profiles.{profile}.revisions."{version}".egress.ports]]
protocol = "udp"
port = 53
{runtime_class}{skills}
"#,
    )
}

fn profile_config(profile: &str, current: &str, revisions: &[(&str, References<'_>)]) -> String {
    let revisions = revisions
        .iter()
        .map(|(version, references)| revision(profile, version, *references))
        .collect::<String>();
    format!(
        r#"
[profiles.{profile}]
current_version = "{current}"
{revisions}
"#,
    )
}

fn config_with_profiles(profiles: &[String]) -> TrustedControllerConfigV1 {
    TrustedControllerConfigV1::from_toml(&format!(
        r#"
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20
{}
"#,
        profiles.concat()
    ))
    .expect("valid test profile configuration")
}

fn config(current: &str, revisions: &[(&str, References<'_>)]) -> TrustedControllerConfigV1 {
    config_with_profiles(&[profile_config("codex-strict", current, revisions)])
}

fn client_and_handle() -> (Client, mock::Handle<Request<Body>, Response<Body>>) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    (Client::new(service, "default"), handle)
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn runtime_class(name: &str, handler: &str) -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {
            "name": name,
            "uid": format!("{name}-uid"),
            "resourceVersion": "runtime-rv-1"
        },
        "handler": handler
    })
}

fn skills_config_map(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "uid": format!("{name}-uid"),
            "resourceVersion": "skills-rv-1"
        },
        "immutable": true,
        "data": { "must-not-be-retained": "sensitive-skill-body" }
    })
}

fn api_failure(sentinel: &str) -> Response<Body> {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": sentinel,
            "reason": "InternalError",
            "code": 500
        }),
    )
}

async fn next_get(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    expected_path: &str,
) -> tower_test::mock::SendResponse<Response<Body>> {
    let (request, send) = handle.next_request().await.expect("Kubernetes GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), expected_path);
    send
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected = tokio::time::timeout(Duration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn invalid_worker_namespace_fails_before_kubernetes_and_is_redacted() {
    const SENTINEL: &str = "invalid/secret-namespace";
    let config = config("v1", &[("v1", References::default())]);
    let (client, handle) = client_and_handle();

    let error = resolve_profile_revisions(client, SENTINEL, &config)
        .await
        .unwrap_err();

    assert_eq!(error, ProfileResolutionError::InvalidWorkerNamespace);
    assert!(!format!("{error}\n{error:?}").contains(SENTINEL));
    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn resolves_all_current_references_before_historical_references() {
    let alpha_current = References {
        runtime_class: Some(("kata-alpha-current", "kata-qemu")),
        skills: Some("skills-alpha-current-v2"),
    };
    let alpha_historical = References {
        runtime_class: Some(("kata-alpha-historical", "kata-qemu")),
        skills: Some("skills-alpha-historical-v1"),
    };
    let zulu_current = References {
        runtime_class: Some(("kata-zulu-current", "kata-qemu")),
        skills: Some("skills-zulu-current-v1"),
    };
    let config = config_with_profiles(&[
        profile_config(
            "alpha-profile",
            "v2",
            &[("v1", alpha_historical), ("v2", alpha_current)],
        ),
        profile_config("zulu-profile", "v1", &[("v1", zulu_current)]),
    ]);
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/kata-alpha-current",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        runtime_class("kata-alpha-current", "kata-qemu"),
    ));
    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/skills-alpha-current-v2",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        skills_config_map("skills-alpha-current-v2"),
    ));
    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/kata-zulu-current",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        runtime_class("kata-zulu-current", "kata-qemu"),
    ));
    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/skills-zulu-current-v1",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        skills_config_map("skills-zulu-current-v1"),
    ));
    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/kata-alpha-historical",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        runtime_class("kata-alpha-historical", "kata-qemu"),
    ));
    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/skills-alpha-historical-v1",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        skills_config_map("skills-alpha-historical-v1"),
    ));

    let resolved = task.await.unwrap().unwrap();
    assert_eq!(resolved.profiles().len(), 3);
    assert_eq!(resolved.profiles()[0].profile().name(), "alpha-profile");
    assert_eq!(resolved.profiles()[0].profile().version(), "v2");
    assert_eq!(resolved.profiles()[1].profile().name(), "zulu-profile");
    assert_eq!(resolved.profiles()[2].profile().name(), "alpha-profile");
    assert_eq!(resolved.profiles()[2].profile().version(), "v1");
    assert_eq!(resolved.current_profile_refs().len(), 2);
    assert_eq!(resolved.current_profile_refs()[0].version(), "v2");
    assert_eq!(resolved.unavailable_historical_revision_count(), 0);
    let debug = format!("{resolved:?}");
    for hidden in [
        "alpha-profile",
        "v2",
        "kata-alpha-current",
        "skills-alpha-current-v2",
        NAMESPACE,
        "sensitive-skill-body",
    ] {
        assert!(!debug.contains(hidden));
    }
}

#[tokio::test]
async fn current_reference_failure_is_fatal_before_historical_resolution() {
    let current = References {
        runtime_class: Some(("current-private-name", "kata-qemu")),
        skills: None,
    };
    let historical = References {
        runtime_class: Some(("historical-private-name", "kata-qemu")),
        skills: None,
    };
    let config = config("v2", &[("v1", historical), ("v2", current)]);
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/current-private-name",
    )
    .await
    .send_response(api_failure("sensitive Kubernetes response body"));

    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error, ProfileResolutionError::CurrentReferenceUnavailable);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn historical_failure_omits_only_that_revision() {
    let historical = References {
        runtime_class: None,
        skills: Some("deleted-historical-skills"),
    };
    let config = config("v2", &[("v1", historical), ("v2", References::default())]);
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/deleted-historical-skills",
    )
    .await
    .send_response(api_failure("historical object is absent"));

    let resolved = task.await.unwrap().unwrap();
    assert_eq!(resolved.profiles().len(), 1);
    assert_eq!(resolved.profiles()[0].profile().version(), "v2");
    assert_eq!(resolved.unavailable_historical_revision_count(), 1);
}

#[tokio::test]
async fn shared_failed_reference_is_fetched_once_but_counted_per_historical_revision() {
    let shared = References {
        runtime_class: None,
        skills: Some("shared-historical-skills"),
    };
    let config = config(
        "v3",
        &[
            ("v1", shared),
            ("v2", shared),
            ("v3", References::default()),
        ],
    );
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/shared-historical-skills",
    )
    .await
    .send_response(api_failure("one cached failure"));

    let resolved = task.await.unwrap().unwrap();
    assert_eq!(resolved.profiles().len(), 1);
    assert_eq!(resolved.unavailable_historical_revision_count(), 2);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn profiles_without_cluster_references_do_not_call_kubernetes() {
    let config = config(
        "v2",
        &[("v1", References::default()), ("v2", References::default())],
    );
    let (client, handle) = client_and_handle();

    let resolved = resolve_profile_revisions(client, NAMESPACE, &config)
        .await
        .unwrap();
    assert_eq!(resolved.profiles().len(), 2);
    assert_eq!(resolved.unavailable_historical_revision_count(), 0);

    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn mutable_deleting_and_mismatched_observations_fail_current_resolution() {
    let mut mutable = skills_config_map("skills-current");
    mutable["immutable"] = json!(false);
    let mut deleting = skills_config_map("skills-current");
    deleting["metadata"]["deletionTimestamp"] = json!("2026-08-03T01:02:03Z");
    let mut missing_uid = skills_config_map("skills-current");
    missing_uid["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("uid");
    let mut missing_resource_version = skills_config_map("skills-current");
    missing_resource_version["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");

    for observed in [mutable, deleting, missing_uid, missing_resource_version] {
        let profile_config = config(
            "v1",
            &[(
                "v1",
                References {
                    runtime_class: None,
                    skills: Some("skills-current"),
                },
            )],
        );
        let (client, handle) = client_and_handle();
        let task = tokio::spawn(async move {
            resolve_profile_revisions(client, NAMESPACE, &profile_config).await
        });
        let mut handle = std::pin::pin!(handle);
        next_get(
            &mut handle,
            "/api/v1/namespaces/team-a-workers/configmaps/skills-current",
        )
        .await
        .send_response(json_response(StatusCode::OK, observed));
        assert_eq!(
            task.await.unwrap().unwrap_err(),
            ProfileResolutionError::CurrentReferenceInvalid
        );
    }

    let mismatched = runtime_class("kata", "different-handler");
    let mut missing_resource_version = runtime_class("kata", "expected-handler");
    missing_resource_version["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    for observed in [mismatched, missing_resource_version] {
        let profile_config = config(
            "v1",
            &[(
                "v1",
                References {
                    runtime_class: Some(("kata", "expected-handler")),
                    skills: None,
                },
            )],
        );
        let (client, handle) = client_and_handle();
        let task = tokio::spawn(async move {
            resolve_profile_revisions(client, NAMESPACE, &profile_config).await
        });
        let mut handle = std::pin::pin!(handle);
        next_get(&mut handle, "/apis/node.k8s.io/v1/runtimeclasses/kata")
            .await
            .send_response(json_response(StatusCode::OK, observed));
        assert_eq!(
            task.await.unwrap().unwrap_err(),
            ProfileResolutionError::CurrentReferenceInvalid
        );
    }
}

#[tokio::test]
async fn mutable_and_deleting_historical_skills_are_omitted_and_counted() {
    let config = config(
        "v3",
        &[
            (
                "v1",
                References {
                    runtime_class: None,
                    skills: Some("mutable-historical-skills"),
                },
            ),
            (
                "v2",
                References {
                    runtime_class: None,
                    skills: Some("deleting-historical-skills"),
                },
            ),
            ("v3", References::default()),
        ],
    );
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    let mut mutable = skills_config_map("mutable-historical-skills");
    mutable["immutable"] = json!(false);
    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/mutable-historical-skills",
    )
    .await
    .send_response(json_response(StatusCode::OK, mutable));

    let mut deleting = skills_config_map("deleting-historical-skills");
    deleting["metadata"]["deletionTimestamp"] = json!("2026-08-03T01:02:03Z");
    next_get(
        &mut handle,
        "/api/v1/namespaces/team-a-workers/configmaps/deleting-historical-skills",
    )
    .await
    .send_response(json_response(StatusCode::OK, deleting));

    let resolved = task.await.unwrap().unwrap();
    assert_eq!(resolved.profiles().len(), 1);
    assert_eq!(resolved.profiles()[0].profile().version(), "v3");
    assert_eq!(resolved.unavailable_historical_revision_count(), 2);
}

#[tokio::test]
async fn shared_runtime_class_is_fetched_once_and_validated_against_each_intent() {
    let config = config(
        "v2",
        &[
            (
                "v1",
                References {
                    runtime_class: Some(("shared-runtime", "historical-handler")),
                    skills: None,
                },
            ),
            (
                "v2",
                References {
                    runtime_class: Some(("shared-runtime", "current-handler")),
                    skills: None,
                },
            ),
        ],
    );
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(async move { resolve_profile_revisions(client, NAMESPACE, &config).await });
    let mut handle = std::pin::pin!(handle);

    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/shared-runtime",
    )
    .await
    .send_response(json_response(
        StatusCode::OK,
        runtime_class("shared-runtime", "current-handler"),
    ));

    let resolved = task.await.unwrap().unwrap();
    assert_eq!(resolved.profiles().len(), 1);
    assert_eq!(resolved.profiles()[0].profile().version(), "v2");
    assert_eq!(resolved.unavailable_historical_revision_count(), 1);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn errors_and_result_debug_never_expose_operator_values_or_kubernetes_bodies() {
    const SENTINEL_PROFILE: &str = "sensitive-profile-name";
    const SENTINEL_REVISION: &str = "private-revision";
    const SENTINEL_NAME: &str = "sensitive-runtime-name";
    const SENTINEL_BODY: &str = "credential-like-value-from-kubernetes";
    let failing_config = config_with_profiles(&[profile_config(
        SENTINEL_PROFILE,
        SENTINEL_REVISION,
        &[(
            SENTINEL_REVISION,
            References {
                runtime_class: Some((SENTINEL_NAME, "private-handler")),
                skills: None,
            },
        )],
    )]);
    let (client, handle) = client_and_handle();
    let task =
        tokio::spawn(
            async move { resolve_profile_revisions(client, NAMESPACE, &failing_config).await },
        );
    let mut handle = std::pin::pin!(handle);
    next_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/sensitive-runtime-name",
    )
    .await
    .send_response(api_failure(SENTINEL_BODY));

    let error = task.await.unwrap().unwrap_err();
    let rendered = format!("{error}\n{error:?}");
    for secret in [
        SENTINEL_PROFILE,
        SENTINEL_REVISION,
        SENTINEL_NAME,
        SENTINEL_BODY,
        NAMESPACE,
    ] {
        assert!(!rendered.contains(secret));
    }
    assert!(error.source().is_none());

    let success_config = config_with_profiles(&[profile_config(
        SENTINEL_PROFILE,
        SENTINEL_REVISION,
        &[(SENTINEL_REVISION, References::default())],
    )]);
    let (client, _handle) = client_and_handle();
    let resolved = resolve_profile_revisions(client, NAMESPACE, &success_config)
        .await
        .unwrap();
    let debug = format!("{resolved:?}");
    for secret in [SENTINEL_PROFILE, SENTINEL_REVISION, NAMESPACE] {
        assert!(!debug.contains(secret));
    }
}
