use crate::acp::protocol::{JsonRpcMessage, JsonRpcRequest};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::ChildStdin;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Duration;
use tracing::debug;

pub(super) const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const LIFECYCLE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const OPENAB_META_NAMESPACE: &str = "openab.dev";
pub(crate) const MAPPING_ABSENT_INITIALIZATION_ERROR_CODE: i64 = -32041;

pub(crate) type PendingRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LifecycleCapabilities {
    pub(crate) close: bool,
    pub(crate) release_v1: bool,
}

pub(crate) fn parse_lifecycle_capabilities(
    initialize_result: Option<&Value>,
) -> LifecycleCapabilities {
    let session_capabilities = initialize_result
        .and_then(|result| result.get("agentCapabilities"))
        .and_then(|capabilities| capabilities.get("sessionCapabilities"));

    LifecycleCapabilities {
        close: session_capabilities
            .and_then(|capabilities| capabilities.get("close"))
            .is_some_and(Value::is_object),
        release_v1: session_capabilities
            .and_then(|capabilities| capabilities.get("_meta"))
            .and_then(|meta| meta.get(OPENAB_META_NAMESPACE))
            .and_then(|openab| openab.get("sessionRelease"))
            .and_then(|release| release.get("version"))
            .and_then(Value::as_u64)
            == Some(1),
    }
}

pub(super) async fn write_bounded_line<W>(
    writer: &Arc<Mutex<W>>,
    data: &str,
    write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    tokio::time::timeout(write_timeout, async {
        let mut writer = writer.lock().await;
        writer.write_all(data.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("stdin write timeout"))??;
    Ok(())
}

pub(crate) async fn send_bounded_request<W>(
    writer: &Arc<Mutex<W>>,
    next_id: &AtomicU64,
    pending: &PendingRequests,
    method: &str,
    params: Option<Value>,
    write_timeout: Duration,
    response_timeout: Duration,
) -> Result<JsonRpcMessage>
where
    W: AsyncWrite + Unpin + Send,
{
    let id = next_id.fetch_add(1, Ordering::Relaxed);
    let request = JsonRpcRequest::new(id, method, params);
    let data = serde_json::to_string(&request)?;
    let (response_tx, response_rx) = oneshot::channel();
    pending.lock().await.insert(id, response_tx);

    debug!(data = data.trim(), "acp_send");
    if let Err(error) = write_bounded_line(writer, &data, write_timeout).await {
        pending.lock().await.remove(&id);
        return Err(error);
    }

    let response = match tokio::time::timeout(response_timeout, response_rx).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            pending.lock().await.remove(&id);
            return Err(anyhow!("channel closed waiting for {method}"));
        }
        Err(_) => {
            pending.lock().await.remove(&id);
            return Err(anyhow!("timeout waiting for {method} response"));
        }
    };

    if let Some(error) = &response.error {
        return Err(anyhow!("{error}"));
    }
    Ok(response)
}

async fn send_bounded_notification<W>(
    writer: &Arc<Mutex<W>>,
    method: &str,
    params: Value,
    write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let data = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    }))?;
    debug!(data = data.trim(), "acp_send");
    write_bounded_line(writer, &data, write_timeout).await
}

struct AcpLifecycleHandle<W> {
    writer: Arc<Mutex<W>>,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    session_id: String,
    capabilities: LifecycleCapabilities,
}

impl<W> AcpLifecycleHandle<W>
where
    W: AsyncWrite + Unpin + Send,
{
    fn new(
        writer: Arc<Mutex<W>>,
        next_id: Arc<AtomicU64>,
        pending: PendingRequests,
        session_id: String,
        capabilities: LifecycleCapabilities,
    ) -> Self {
        Self {
            writer,
            next_id,
            pending,
            session_id,
            capabilities,
        }
    }

    async fn cancel_inner(&self) -> Result<()> {
        send_bounded_notification(
            &self.writer,
            "session/cancel",
            json!({"sessionId": self.session_id}),
            CONTROL_WRITE_TIMEOUT,
        )
        .await
    }

    async fn close_inner(&self) -> Result<()> {
        if !self.capabilities.close {
            return Err(anyhow!("session/close capability was not advertised"));
        }
        let response = send_bounded_request(
            &self.writer,
            &self.next_id,
            &self.pending,
            "session/close",
            Some(json!({"sessionId": self.session_id})),
            CONTROL_WRITE_TIMEOUT,
            LIFECYCLE_RESPONSE_TIMEOUT,
        )
        .await?;
        if !response.result.as_ref().is_some_and(Value::is_object) {
            return Err(anyhow!("session/close returned an invalid acknowledgement"));
        }
        Ok(())
    }

    async fn release_inner(&self) -> Result<()> {
        if !self.capabilities.release_v1 {
            return Err(anyhow!(
                "_openab/session/release capability was not advertised"
            ));
        }
        let response = send_bounded_request(
            &self.writer,
            &self.next_id,
            &self.pending,
            "_openab/session/release",
            Some(json!({"sessionId": self.session_id})),
            CONTROL_WRITE_TIMEOUT,
            LIFECYCLE_RESPONSE_TIMEOUT,
        )
        .await?;
        if !response.result.as_ref().is_some_and(Value::is_object) {
            return Err(anyhow!(
                "_openab/session/release returned an invalid acknowledgement"
            ));
        }
        Ok(())
    }
}

/// Lock-free lifecycle controls for an initialized isolated-session bridge.
///
/// Implementations must bound their own writes and request waits so pool
/// lifecycle paths never hold state indefinitely.
#[async_trait::async_trait]
pub(crate) trait SessionLifecycleControl: Send + Sync {
    fn capabilities(&self) -> LifecycleCapabilities;
    async fn cancel(&self) -> Result<()>;
    // The pool integration calls this through LifecycleHandle. Keep a default
    // so existing local/test controls remain inert unless they opt in.
    #[allow(dead_code)]
    async fn close(&self) -> Result<()> {
        Err(anyhow!("session/close lifecycle control is not available"))
    }
    async fn release(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl SessionLifecycleControl for AcpLifecycleHandle<ChildStdin> {
    fn capabilities(&self) -> LifecycleCapabilities {
        self.capabilities
    }

    async fn cancel(&self) -> Result<()> {
        self.cancel_inner().await
    }

    async fn close(&self) -> Result<()> {
        self.close_inner().await
    }

    async fn release(&self) -> Result<()> {
        self.release_inner().await
    }
}

pub(crate) type LifecycleHandle = Arc<dyn SessionLifecycleControl>;

pub(super) fn new_lifecycle_handle(
    writer: Arc<Mutex<ChildStdin>>,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    session_id: String,
    capabilities: LifecycleCapabilities,
) -> LifecycleHandle {
    Arc::new(AcpLifecycleHandle::new(
        writer,
        next_id,
        pending,
        session_id,
        capabilities,
    ))
}

/// Broker-owned context injected when spawning an ACP process for a logical
/// session. This is deliberately separate from operator-controlled `[agent]`
/// environment configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerMappingExpectation {
    Absent,
    Present,
}

impl BrokerMappingExpectation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Present => "present",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionSpawnContext {
    logical_session_key: String,
    attempt_id: String,
    broker_mapping_expectation: BrokerMappingExpectation,
}

impl SessionSpawnContext {
    pub(crate) fn new(
        logical_session_key: impl Into<String>,
        broker_mapping_expectation: BrokerMappingExpectation,
    ) -> Self {
        Self::from_parts(
            logical_session_key,
            uuid::Uuid::new_v4().to_string(),
            broker_mapping_expectation,
        )
    }

    pub(super) fn from_parts(
        logical_session_key: impl Into<String>,
        attempt_id: impl Into<String>,
        broker_mapping_expectation: BrokerMappingExpectation,
    ) -> Self {
        Self {
            logical_session_key: logical_session_key.into(),
            attempt_id: attempt_id.into(),
            broker_mapping_expectation,
        }
    }

    pub(crate) fn logical_session_key(&self) -> &str {
        &self.logical_session_key
    }

    pub(crate) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub(crate) fn broker_mapping_expectation(&self) -> BrokerMappingExpectation {
        self.broker_mapping_expectation
    }
}

/// Sanitized signal that the controller authoritatively found no worker state
/// for the durable mapping supplied by this broker.
///
/// The untrusted JSON-RPC message and data are deliberately not retained.
/// Callers can downcast `anyhow::Error` to this type without parsing text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MappingAbsentInitialization;

impl fmt::Display for MappingAbsentInitialization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("isolated session durable mapping is absent")
    }
}

impl std::error::Error for MappingAbsentInitialization {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MappingAbsentInitializationData {
    version: u8,
    outcome: MappingAbsentInitializationOutcome,
    attempt_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingAbsentInitializationEnvelope {
    jsonrpc: String,
    #[serde(rename = "id")]
    _request_id: u64,
    error: MappingAbsentInitializationError,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingAbsentInitializationError {
    code: i64,
    #[serde(rename = "message")]
    _message: String,
    data: MappingAbsentInitializationData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MappingAbsentInitializationOutcome {
    MappingAbsent,
}

pub(super) fn mapping_absent_initialization(
    context: Option<&SessionSpawnContext>,
    message: &JsonRpcMessage,
) -> Option<MappingAbsentInitialization> {
    let context = context?;
    if context.broker_mapping_expectation() != BrokerMappingExpectation::Present {
        return None;
    }
    let envelope: MappingAbsentInitializationEnvelope =
        serde_json::from_str(message.raw.as_deref()?).ok()?;
    if envelope.jsonrpc != "2.0" || envelope.error.code != MAPPING_ABSENT_INITIALIZATION_ERROR_CODE
    {
        return None;
    }
    let data = envelope.error.data;
    if data.version != 1
        || !matches!(
            data.outcome,
            MappingAbsentInitializationOutcome::MappingAbsent
        )
        || data.attempt_id != context.attempt_id()
        || uuid::Uuid::parse_str(&data.attempt_id)
            .ok()
            .map(|attempt_id| attempt_id.is_nil())
            .unwrap_or(true)
    {
        return None;
    }
    Some(MappingAbsentInitialization)
}

pub(super) fn session_spawn_env(
    context: Option<&SessionSpawnContext>,
) -> Vec<(&'static str, &str)> {
    context
        .map(|context| {
            vec![
                (super::SESSION_KEY_ENV, context.logical_session_key()),
                (super::SESSION_ATTEMPT_ID_ENV, context.attempt_id()),
                (
                    super::SESSION_MAPPING_EXPECTATION_ENV,
                    context.broker_mapping_expectation().as_str(),
                ),
            ]
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        mapping_absent_initialization, parse_lifecycle_capabilities, send_bounded_request,
        session_spawn_env, AcpLifecycleHandle, BrokerMappingExpectation, LifecycleCapabilities,
        PendingRequests, SessionSpawnContext, MAPPING_ABSENT_INITIALIZATION_ERROR_CODE,
    };
    use crate::acp::connection::run_reader_loop;
    use crate::acp::protocol::JsonRpcMessage;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::{mpsc, Mutex};
    use tokio::time::Duration;

    fn pending_requests() -> PendingRequests {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn response(id: u64, result: serde_json::Value) -> JsonRpcMessage {
        JsonRpcMessage {
            id: Some(id),
            method: None,
            result: Some(result),
            error: None,
            params: None,
            raw: None,
        }
    }

    async fn read_json<R>(reader: &mut BufReader<R>) -> serde_json::Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    async fn run_lifecycle_request(
        result: serde_json::Value,
        close: bool,
    ) -> (serde_json::Value, anyhow::Result<()>) {
        let (writer, peer) = duplex(8 * 1024);
        let pending = pending_requests();
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&pending),
            "outer-session".to_string(),
            LifecycleCapabilities {
                close: true,
                release_v1: true,
            },
        );
        let request = tokio::spawn(async move {
            if close {
                handle.close_inner().await
            } else {
                handle.release_inner().await
            }
        });
        let sent = read_json(&mut BufReader::new(peer)).await;
        let id = sent["id"].as_u64().unwrap();
        pending
            .lock()
            .await
            .remove(&id)
            .expect("pending lifecycle request")
            .send(response(id, result))
            .expect("lifecycle receiver");
        (sent, request.await.unwrap())
    }

    #[test]
    fn no_session_spawn_context_adds_no_reserved_env() {
        assert!(session_spawn_env(None).is_empty());
    }

    #[test]
    fn openab_v1_session_spawn_context_maps_broker_owned_values() {
        let context = SessionSpawnContext::from_parts(
            "discord:thread-123",
            "87f08c0d-25e7-47dc-a7b6-e3f3cc89f977",
            BrokerMappingExpectation::Present,
        );

        assert_eq!(
            session_spawn_env(Some(&context)),
            vec![
                ("OPENAB_SESSION_KEY", "discord:thread-123"),
                (
                    "OPENAB_SESSION_ATTEMPT_ID",
                    "87f08c0d-25e7-47dc-a7b6-e3f3cc89f977"
                ),
                ("OPENAB_SESSION_MAPPING_EXPECTATION", "present"),
            ]
        );
    }

    #[test]
    fn mapping_expectation_environment_value_is_closed() {
        assert_eq!(BrokerMappingExpectation::Absent.as_str(), "absent");
        assert_eq!(BrokerMappingExpectation::Present.as_str(), "present");
    }

    fn mapping_absent_response(attempt_id: &str) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": MAPPING_ABSENT_INITIALIZATION_ERROR_CODE,
                "message": "controller mapping is absent",
                "data": {
                    "version": 1,
                    "outcome": "mapping_absent",
                    "attemptId": attempt_id,
                },
            },
        })
    }

    fn message_with_retained_raw(raw: serde_json::Value) -> JsonRpcMessage {
        let raw = serde_json::to_string(&raw).unwrap();
        let mut message: JsonRpcMessage = serde_json::from_str(&raw).unwrap();
        message.raw = Some(raw.into());
        message
    }

    fn message_with_retained_raw_text(raw: &str) -> JsonRpcMessage {
        // Construct the permissive generic view from a normalized Value while
        // retaining the exact original text for strict duplicate detection.
        let normalized: serde_json::Value = serde_json::from_str(raw).unwrap();
        let mut message: JsonRpcMessage = serde_json::from_value(normalized).unwrap();
        message.raw = Some(raw.into());
        message
    }

    #[test]
    fn mapping_absence_requires_an_exact_retained_json_rpc_envelope() {
        let attempt_id = "87f08c0d-25e7-47dc-a7b6-e3f3cc89f977";
        let context = SessionSpawnContext::from_parts(
            "discord:thread-123",
            attempt_id,
            BrokerMappingExpectation::Present,
        );
        let valid = mapping_absent_response(attempt_id);
        assert!(mapping_absent_initialization(
            Some(&context),
            &message_with_retained_raw(valid.clone())
        )
        .is_some());

        let missing_raw: JsonRpcMessage = serde_json::from_value(valid.clone()).unwrap();
        assert!(missing_raw.raw.is_none());
        assert!(mapping_absent_initialization(Some(&context), &missing_raw).is_none());

        let mut invalid_envelopes = Vec::new();

        let mut response = valid.clone();
        response.as_object_mut().unwrap().remove("jsonrpc");
        invalid_envelopes.push(response);

        let mut response = valid.clone();
        response["jsonrpc"] = json!("1.0");
        invalid_envelopes.push(response);

        for (field, value) in [
            ("result", serde_json::Value::Null),
            ("method", json!("initialize")),
            ("params", json!({})),
            ("unexpected", json!(true)),
        ] {
            let mut response = valid.clone();
            response
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), value);
            invalid_envelopes.push(response);
        }

        let mut response = valid;
        response["error"]
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_string(), json!(true));
        invalid_envelopes.push(response);

        for response in invalid_envelopes {
            // Legacy ACP parsing remains permissive; only the typed
            // security-sensitive classification rejects the extension.
            let message = message_with_retained_raw(response);
            assert!(message.error.is_some());
            assert!(mapping_absent_initialization(Some(&context), &message).is_none());
        }
    }

    #[test]
    fn mapping_absence_rejects_duplicate_keys_at_every_envelope_layer() {
        let attempt_id = "87f08c0d-25e7-47dc-a7b6-e3f3cc89f977";
        let context = SessionSpawnContext::from_parts(
            "discord:thread-123",
            attempt_id,
            BrokerMappingExpectation::Present,
        );
        let value = mapping_absent_response(attempt_id);
        let canonical = serde_json::to_string(&value).unwrap();
        let error_json = serde_json::to_string(&value["error"]).unwrap();
        let data_json = serde_json::to_string(&value["error"]["data"]).unwrap();
        let error_field = format!(r#""error":{error_json}"#);
        let data_field = format!(r#""data":{data_json}"#);
        let attempt_field = format!(r#""attemptId":"{attempt_id}""#);

        let duplicates = [
            (
                "root jsonrpc",
                canonical.replacen(
                    r#""jsonrpc":"2.0""#,
                    r#""jsonrpc":"2.0","jsonrpc":"2.0""#,
                    1,
                ),
            ),
            (
                "root id",
                canonical.replacen(r#""id":1"#, r#""id":1,"id":1"#, 1),
            ),
            (
                "root error",
                canonical.replacen(&error_field, &format!("{error_field},{error_field}"), 1),
            ),
            (
                "error code",
                canonical.replacen(r#""code":-32041"#, r#""code":-32041,"code":-32041"#, 1),
            ),
            (
                "error data",
                canonical.replacen(&data_field, &format!("{data_field},{data_field}"), 1),
            ),
            (
                "data attemptId",
                canonical.replacen(
                    &attempt_field,
                    &format!("{attempt_field},{attempt_field}"),
                    1,
                ),
            ),
        ];

        for (case, duplicate) in duplicates {
            assert_ne!(
                duplicate, canonical,
                "test fixture did not duplicate {case}"
            );
            let message = message_with_retained_raw_text(&duplicate);
            assert!(
                mapping_absent_initialization(Some(&context), &message).is_none(),
                "duplicate {case} must not produce a typed absence"
            );
        }
    }

    #[test]
    fn lifecycle_capabilities_require_exact_shapes() {
        let supported = json!({
            "agentCapabilities": {
                "sessionCapabilities": {
                    "close": {},
                    "_meta": {
                        "openab.dev": {
                            "sessionRelease": {"version": 1}
                        }
                    }
                }
            }
        });
        assert_eq!(
            parse_lifecycle_capabilities(Some(&supported)),
            LifecycleCapabilities {
                close: true,
                release_v1: true,
            }
        );

        for (index, unsupported) in [
            json!({"agentCapabilities": {"sessionCapabilities": {
                "close": true,
                "_meta": {"openab.dev": {"sessionRelease": {"version": 2}}}
            }}}),
            json!({"agentCapabilities": {
                "sessionCapabilities": {"close": {}},
                "_meta": {"openab.dev": {"sessionRelease": {"version": 1}}}
            }}),
        ]
        .iter()
        .enumerate()
        {
            let capabilities = parse_lifecycle_capabilities(Some(unsupported));
            assert!(!capabilities.release_v1);
            if index == 0 {
                assert!(!capabilities.close);
            }
        }
        assert_eq!(
            parse_lifecycle_capabilities(None),
            LifecycleCapabilities::default()
        );
    }

    #[tokio::test]
    async fn bounded_request_uses_shared_unique_id_and_resolves_response() {
        let (writer, peer) = duplex(8 * 1024);
        let writer = Arc::new(Mutex::new(writer));
        let pending = pending_requests();
        let next_id = Arc::new(AtomicU64::new(7));
        let request = tokio::spawn({
            let writer = Arc::clone(&writer);
            let pending = Arc::clone(&pending);
            let next_id = Arc::clone(&next_id);
            async move {
                send_bounded_request(
                    &writer,
                    &next_id,
                    &pending,
                    "session/close",
                    Some(json!({"sessionId": "outer-session"})),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                )
                .await
            }
        });

        let sent = read_json(&mut BufReader::new(peer)).await;
        assert_eq!(sent["id"], 7);
        assert_eq!(sent["method"], "session/close");

        let responder = pending.lock().await.remove(&7).expect("pending request");
        responder
            .send(response(7, json!({})))
            .expect("response receiver");

        request.await.unwrap().unwrap();
        assert_eq!(next_id.load(Ordering::Relaxed), 8);
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bounded_request_failures_remove_pending_entries() {
        let (writer, _peer) = duplex(8 * 1024);
        let writer = Arc::new(Mutex::new(writer));
        let pending = pending_requests();
        let next_id = Arc::new(AtomicU64::new(1));

        let error = send_bounded_request(
            &writer,
            &next_id,
            &pending,
            "session/close",
            Some(json!({"sessionId": "outer-session"})),
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("timeout waiting for session/close"));
        assert!(pending.lock().await.is_empty());

        let (writer, peer) = duplex(8 * 1024);
        drop(peer);
        let writer = Arc::new(Mutex::new(writer));

        send_bounded_request(
            &writer,
            &next_id,
            &pending,
            "session/close",
            Some(json!({"sessionId": "outer-session"})),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn release_uses_extension_method_and_requires_object_ack() {
        let (sent, result) = run_lifecycle_request(json!({}), false).await;
        assert_eq!(sent["method"], "_openab/session/release");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        result.unwrap();

        let (_, result) = run_lifecycle_request(serde_json::Value::Null, false).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("invalid acknowledgement"));
    }

    #[tokio::test]
    async fn close_uses_standard_method_and_requires_object_ack() {
        let (sent, result) = run_lifecycle_request(json!({}), true).await;
        assert_eq!(sent["method"], "session/close");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        result.unwrap();

        let (_, result) = run_lifecycle_request(serde_json::Value::Null, true).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("invalid acknowledgement"));
    }

    #[tokio::test]
    async fn close_error_response_clears_shared_pending_entry() {
        let (client_writer, agent_reader) = duplex(8 * 1024);
        let (mut agent_writer, client_reader) = duplex(8 * 1024);
        let (permission_writer, _permission_reader) = duplex(8 * 1024);
        let pending = pending_requests();
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));
        let reader = tokio::spawn(run_reader_loop(
            client_reader,
            Arc::new(Mutex::new(permission_writer)),
            Arc::clone(&pending),
            notify_tx,
            None,
        ));
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(client_writer)),
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&pending),
            "outer-session".to_string(),
            LifecycleCapabilities {
                close: true,
                release_v1: false,
            },
        );

        let request = tokio::spawn(async move { handle.close_inner().await });
        let sent = read_json(&mut BufReader::new(agent_reader)).await;
        let id = sent["id"].as_u64().unwrap();
        let reply = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "close failed"}
        });
        agent_writer
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .unwrap();
        agent_writer.flush().await.unwrap();

        let error = request.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("close failed"));
        assert!(pending.lock().await.is_empty());

        drop(agent_writer);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_is_notification_without_request_id() {
        let (writer, peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        handle.cancel_inner().await.unwrap();

        let mut peer = BufReader::new(peer);
        let mut line = String::new();
        peer.read_line(&mut line).await.unwrap();
        let sent: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(sent["method"], "session/cancel");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        assert!(sent.get("id").is_none());
    }

    #[tokio::test]
    async fn unsupported_release_fails_before_writing() {
        let (writer, mut peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        let error = handle.release_inner().await.unwrap_err();
        assert!(error.to_string().contains("not advertised"));

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(
            Duration::from_millis(10),
            tokio::io::AsyncReadExt::read(&mut peer, &mut byte),
        )
        .await;
        assert!(read.is_err(), "unsupported release must not write");
    }

    #[tokio::test]
    async fn unsupported_close_fails_before_writing() {
        let (writer, mut peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        let error = handle.close_inner().await.unwrap_err();
        assert!(error.to_string().contains("not advertised"));

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(
            Duration::from_millis(10),
            tokio::io::AsyncReadExt::read(&mut peer, &mut byte),
        )
        .await;
        assert!(read.is_err(), "unsupported close must not write");
    }
}
