//! Bounded plaintext Kubernetes health probes for the trusted controller.

use super::{ControllerReadiness, ControllerReadinessState};
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::FutureExt;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

const MAX_CONCURRENT_CONNECTIONS: usize = 16;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HEADERS: usize = 16;
const MAX_HTTP_BUFFER_BYTES: usize = 8 * 1024;

const LIVE_BODY: &str = "live\n";
const NOT_LIVE_BODY: &str = "not live\n";
const READY_BODY: &str = "ready\n";
const NOT_READY_BODY: &str = "not ready\n";
const NOT_FOUND_BODY: &str = "not found\n";
const METHOD_NOT_ALLOWED_BODY: &str = "method not allowed\n";

trait ProbeReadiness: Clone + Send + Sync + 'static {
    fn observed_state(&self) -> Option<ControllerReadinessState>;
}

impl ProbeReadiness for ControllerReadiness {
    fn observed_state(&self) -> Option<ControllerReadinessState> {
        ControllerReadiness::observed_state(self)
    }
}

#[derive(Clone, Copy)]
struct ProbePolicy {
    max_connections: NonZeroUsize,
    header_read_timeout: Duration,
}

impl ProbePolicy {
    fn production() -> Self {
        Self {
            max_connections: NonZeroUsize::new(MAX_CONCURRENT_CONNECTIONS)
                .expect("probe connection limit is nonzero"),
            header_read_timeout: HEADER_READ_TIMEOUT,
        }
    }
}

/// Plaintext HTTP server exposing only Kubernetes liveness and readiness probes.
///
/// The caller owns listener binding policy and should expose this server only
/// on the add-on's dedicated probe port. It intentionally does not share the
/// authenticated TLS controller listener. Connection concurrency, request
/// headers, header-read time, and connection lifetime are fixed and bounded.
pub struct ControllerProbeServer {
    router: Router,
}

impl ControllerProbeServer {
    pub fn new(readiness: ControllerReadiness) -> Self {
        Self {
            router: build_router(readiness),
        }
    }

    /// Serve probes until the supplied shutdown signal resolves.
    ///
    /// Shutdown stops acceptance and synchronously drops every in-flight probe
    /// connection. Probe requests carry no state that requires graceful drain.
    pub async fn serve_until<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), ControllerProbeServeError>
    where
        F: Future<Output = ()> + Send,
    {
        serve_router_until(self.router, listener, shutdown, ProbePolicy::production()).await
    }
}

#[derive(Debug, Error)]
pub enum ControllerProbeServeError {
    #[error("controller probe listener failed to accept a connection")]
    Accept(#[source] io::Error),
}

fn build_router<R>(readiness: R) -> Router
where
    R: ProbeReadiness,
{
    Router::new()
        .route("/livez", any(livez::<R>))
        .route("/readyz", any(readyz::<R>))
        .fallback(not_found)
        .with_state(readiness)
}

async fn livez<R>(State(readiness): State<R>, method: Method) -> Response
where
    R: ProbeReadiness,
{
    if method != Method::GET {
        return method_not_allowed();
    }

    match readiness.observed_state() {
        Some(_) => probe_response(StatusCode::OK, LIVE_BODY),
        None => probe_response(StatusCode::SERVICE_UNAVAILABLE, NOT_LIVE_BODY),
    }
}

async fn readyz<R>(State(readiness): State<R>, method: Method) -> Response
where
    R: ProbeReadiness,
{
    if method != Method::GET {
        return method_not_allowed();
    }

    match readiness.observed_state() {
        Some(ControllerReadinessState::Ready) => probe_response(StatusCode::OK, READY_BODY),
        Some(ControllerReadinessState::NotReady) | None => {
            probe_response(StatusCode::SERVICE_UNAVAILABLE, NOT_READY_BODY)
        }
    }
}

async fn not_found() -> Response {
    probe_response(StatusCode::NOT_FOUND, NOT_FOUND_BODY)
}

fn method_not_allowed() -> Response {
    let mut response = probe_response(StatusCode::METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED_BODY);
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET"));
    response
}

fn probe_response(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, Body::from(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

async fn serve_router_until<F>(
    router: Router,
    listener: TcpListener,
    shutdown: F,
    policy: ProbePolicy,
) -> Result<(), ControllerProbeServeError>
where
    F: Future<Output = ()> + Send,
{
    let mut connections: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => return Ok(()),
            Some(()) = connections.next(), if !connections.is_empty() => {}
            accepted = listener.accept(), if has_connection_capacity(connections.len(), policy) => {
                match accepted {
                    Ok((stream, _)) => {
                        connections.push(serve_connection(router.clone(), stream, policy));
                    }
                    Err(source) if is_transient_accept_error(&source) => continue,
                    Err(source) => return Err(ControllerProbeServeError::Accept(source)),
                }
            }
        }
    }
}

fn is_transient_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

fn has_connection_capacity(active: usize, policy: ProbePolicy) -> bool {
    active < policy.max_connections.get()
}

fn serve_connection(
    router: Router,
    stream: TcpStream,
    policy: ProbePolicy,
) -> BoxFuture<'static, ()> {
    async move {
        let mut http = http1::Builder::new();
        http.timer(TokioTimer::new())
            .header_read_timeout(policy.header_read_timeout)
            .keep_alive(false)
            .max_headers(MAX_HEADERS)
            .max_buf_size(MAX_HTTP_BUFFER_BYTES)
            .auto_date_header(false);
        let service = TowerToHyperService::new(router);
        let _ = http.serve_connection(TokioIo::new(stream), service).await;
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{header, Method, Request, StatusCode};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout};
    use tower::ServiceExt;

    const MAX_PROBE_BODY_BYTES: usize = 64;

    #[derive(Clone)]
    struct TestReadiness {
        state: Arc<Mutex<Option<ControllerReadinessState>>>,
    }

    impl TestReadiness {
        fn new(state: ControllerReadinessState) -> Self {
            Self {
                state: Arc::new(Mutex::new(Some(state))),
            }
        }

        fn set(&self, state: ControllerReadinessState) {
            *self.state.lock().expect("test readiness mutex poisoned") = Some(state);
        }

        fn close(&self) {
            *self.state.lock().expect("test readiness mutex poisoned") = None;
        }
    }

    impl ProbeReadiness for TestReadiness {
        fn observed_state(&self) -> Option<ControllerReadinessState> {
            *self.state.lock().expect("test readiness mutex poisoned")
        }
    }

    struct ProbeResponse {
        status: StatusCode,
        body: String,
        cache_control: String,
        allow: Option<String>,
    }

    async fn request(router: Router, method: Method, path: &'static str) -> ProbeResponse {
        let response = router
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .expect("probe request must build"),
            )
            .await
            .expect("probe router must be infallible");
        let status = response.status();
        let cache_control = response
            .headers()
            .get(header::CACHE_CONTROL)
            .expect("probe response must disable caches")
            .to_str()
            .expect("cache-control must be ASCII")
            .to_owned();
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/plain; charset=utf-8"))
        );
        let allow = response
            .headers()
            .get(header::ALLOW)
            .map(|value| value.to_str().expect("allow must be ASCII").to_owned());
        let body = to_bytes(response.into_body(), MAX_PROBE_BODY_BYTES)
            .await
            .expect("probe body must be bounded");
        ProbeResponse {
            status,
            body: String::from_utf8(body.to_vec()).expect("probe body must be UTF-8"),
            cache_control,
            allow,
        }
    }

    #[tokio::test]
    async fn livez_tracks_supervisor_lifetime_not_readiness() {
        let readiness = TestReadiness::new(ControllerReadinessState::NotReady);
        let router = build_router(readiness.clone());

        let live = request(router.clone(), Method::GET, "/livez").await;
        assert_eq!(live.status, StatusCode::OK);
        assert_eq!(live.body, LIVE_BODY);
        assert_eq!(live.cache_control, "no-store");

        readiness.close();
        let terminal = request(router, Method::GET, "/livez").await;
        assert_eq!(terminal.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(terminal.body, NOT_LIVE_BODY);
    }

    #[tokio::test]
    async fn readyz_tracks_ready_not_ready_and_closed_states() {
        let readiness = TestReadiness::new(ControllerReadinessState::Ready);
        let router = build_router(readiness.clone());

        let ready = request(router.clone(), Method::GET, "/readyz").await;
        assert_eq!(ready.status, StatusCode::OK);
        assert_eq!(ready.body, READY_BODY);

        readiness.set(ControllerReadinessState::NotReady);
        let not_ready = request(router.clone(), Method::GET, "/readyz").await;
        assert_eq!(not_ready.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(not_ready.body, NOT_READY_BODY);

        readiness.close();
        let closed = request(router, Method::GET, "/readyz").await;
        assert_eq!(closed.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(closed.body, NOT_READY_BODY);
    }

    #[tokio::test]
    async fn probe_surface_rejects_unknown_paths_and_non_get_methods() {
        let router = build_router(TestReadiness::new(ControllerReadinessState::Ready));

        let unknown = request(router.clone(), Method::GET, "/metrics").await;
        assert_eq!(unknown.status, StatusCode::NOT_FOUND);
        assert_eq!(unknown.body, NOT_FOUND_BODY);
        assert_eq!(unknown.allow, None);

        let post = request(router.clone(), Method::POST, "/livez").await;
        assert_eq!(post.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(post.body, METHOD_NOT_ALLOWED_BODY);
        assert_eq!(post.allow.as_deref(), Some("GET"));

        let head = request(router, Method::HEAD, "/readyz").await;
        assert_eq!(head.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(head.allow.as_deref(), Some("GET"));
    }

    #[tokio::test]
    async fn partial_header_is_disconnected_after_the_fixed_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener must bind");
        let address = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(serve_router_until(
            build_router(TestReadiness::new(ControllerReadinessState::Ready)),
            listener,
            async move {
                let _ = shutdown_rx.await;
            },
            ProbePolicy {
                max_connections: NonZeroUsize::new(1).unwrap(),
                header_read_timeout: Duration::from_millis(25),
            },
        ));
        let mut client = TcpStream::connect(address)
            .await
            .expect("probe client must connect");
        client
            .write_all(b"GET /livez HTTP/1.1\r\nHost: local")
            .await
            .expect("partial probe header must write");

        let mut response = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("partial header must be disconnected")
            .expect("probe client read must succeed");

        shutdown_tx.send(()).expect("probe shutdown must signal");
        timeout(Duration::from_secs(1), task)
            .await
            .expect("probe shutdown must be bounded")
            .expect("probe task must join")
            .expect("probe server must stop cleanly");
    }

    #[tokio::test]
    async fn shutdown_drops_a_hostile_open_connection_without_drain() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener must bind");
        let address = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let serving = serve_router_until(
            build_router(TestReadiness::new(ControllerReadinessState::Ready)),
            listener,
            async move {
                let _ = shutdown_rx.await;
            },
            ProbePolicy::production(),
        );
        let task = tokio::spawn(serving);
        let mut client = TcpStream::connect(address)
            .await
            .expect("probe client must connect");
        client
            .write_all(b"GET /livez HTTP/1.1\r\n")
            .await
            .expect("hostile partial header must write");
        sleep(Duration::from_millis(10)).await;

        shutdown_tx.send(()).expect("probe shutdown must signal");

        timeout(Duration::from_secs(1), task)
            .await
            .expect("probe shutdown must be bounded")
            .expect("probe task must join")
            .expect("probe server must stop cleanly");
        let mut byte = [0_u8; 1];
        match timeout(Duration::from_secs(1), client.read(&mut byte))
            .await
            .expect("hostile connection must be dropped")
        {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                ) => {}
            Ok(bytes) => panic!("hostile connection returned {bytes} byte(s) instead of closing"),
            Err(error) => panic!("hostile connection closed unexpectedly: {error}"),
        }
    }

    #[test]
    fn transient_accept_errors_are_retried_but_listener_failures_are_fatal() {
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
        ] {
            assert!(is_transient_accept_error(&io::Error::from(kind)));
        }

        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(!is_transient_accept_error(&io::Error::from(kind)));
        }
    }

    #[test]
    fn production_policy_is_fixed_and_bounded() {
        let policy = ProbePolicy::production();
        assert_eq!(policy.max_connections.get(), MAX_CONCURRENT_CONNECTIONS);
        assert_eq!(policy.header_read_timeout, HEADER_READ_TIMEOUT);
        assert!(has_connection_capacity(
            MAX_CONCURRENT_CONNECTIONS - 1,
            policy
        ));
        assert!(!has_connection_capacity(MAX_CONCURRENT_CONNECTIONS, policy));
    }
}
