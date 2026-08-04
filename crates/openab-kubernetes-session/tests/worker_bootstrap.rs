#![cfg(feature = "worker-runtime")]

use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::client_transport::{MAX_CLIENT_CA_PEM_BYTES, MAX_CLIENT_URL_BYTES};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::Fence;
use openab_kubernetes_session::wire::{encode_frame, WorkerRegistrationV1};
use openab_kubernetes_session::worker::bootstrap::{
    WorkerBootstrap, WorkerBootstrapEnvironment, WorkerBootstrapError, WorkerCommand,
    WorkerCommandError, WorkerEnvironmentError, MAX_WORKER_REGISTRATION_BINDING_BYTES,
    WORKER_BOOTSTRAP_ENV_NAMES,
};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, Cursor, Read};
use std::rc::Rc;
use uuid::Uuid;

const CONTROLLER_URL: &str = "wss://controller-sensitive.example.test:8443/v1/worker";
const POD_UID: &str = "4db5a02c-74e2-4a27-838f-7f3483c541a9";
const TOKEN: &[u8; 32] = b"secret-token-must-never-appear!!";

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn command() -> WorkerCommand {
    WorkerCommand::parse(args(&[
        "serve",
        "--",
        "/usr/local/bin/acp-sensitive",
        "--model",
        "test-model",
    ]))
    .unwrap()
}

fn environment_values() -> BTreeMap<&'static str, OsString> {
    BTreeMap::from([
        ("OPENAB_SESSION_CONTROLLER_URL", CONTROLLER_URL.into()),
        (
            "OPENAB_SESSION_CONTROLLER_CA_FILE",
            "/var/run/openab-controller-ca/ca.crt".into(),
        ),
        (
            "OPENAB_REGISTRATION_TOKEN_FILE",
            "/var/run/openab-registration/token".into(),
        ),
        (
            "OPENAB_REGISTRATION_BINDING_FILE",
            "/var/run/openab-registration/binding.json".into(),
        ),
        ("OPENAB_WORKER_POD_UID", POD_UID.into()),
        ("OPENAB_SESSION_ROOT", "/session".into()),
        ("OPENAB_WORKSPACE", "/session/workspace".into()),
        ("HOME", "/session/home".into()),
    ])
}

fn environment() -> WorkerBootstrapEnvironment {
    let values = environment_values();
    WorkerBootstrapEnvironment::from_lookup(|name| values.get(name).cloned()).unwrap()
}

fn registration_bytes() -> Vec<u8> {
    let binding = SessionBinding::new(
        ScopeId::derive("team-sensitive"),
        SessionId::derive("team-sensitive", "discord:thread-sensitive"),
        Fence::new(7, Uuid::from_u128(100)).unwrap(),
        Uuid::from_u128(200),
    )
    .unwrap();
    encode_frame(&WorkerRegistrationV1::new(&binding)).unwrap()
}

fn ca_pem() -> Vec<u8> {
    generate_simple_self_signed(vec!["controller-sensitive.example.test".to_owned()])
        .unwrap()
        .cert
        .pem()
        .into_bytes()
}

fn load_with(
    token: impl Read,
    registration: impl Read,
    ca: impl Read,
) -> Result<WorkerBootstrap, WorkerBootstrapError> {
    WorkerBootstrap::load_from_readers(command(), environment(), token, registration, ca)
}

#[test]
fn command_accepts_only_serve_separator_and_an_absolute_child() {
    let parsed = command();
    assert_eq!(
        parsed.executable().to_str(),
        Some("/usr/local/bin/acp-sensitive")
    );
    assert_eq!(
        parsed.arguments(),
        [OsString::from("--model"), OsString::from("test-model")]
    );
    assert!(!format!("{parsed:?}").contains("acp-sensitive"));
    let no_arguments =
        WorkerCommand::parse(args(&["serve", "--", "/does/not/need/to/exist"])).unwrap();
    assert!(no_arguments.arguments().is_empty());

    for (values, expected) in [
        (vec![], WorkerCommandError::MissingSubcommand),
        (vec!["start"], WorkerCommandError::UnknownSubcommand),
        (vec!["serve"], WorkerCommandError::MissingSeparator),
        (
            vec!["serve", "/usr/local/bin/acp"],
            WorkerCommandError::InvalidSeparator,
        ),
        (vec!["serve", "--"], WorkerCommandError::MissingExecutable),
        (
            vec!["serve", "--", "relative/acp"],
            WorkerCommandError::RelativeExecutable,
        ),
        (
            vec!["serve", "--", ""],
            WorkerCommandError::RelativeExecutable,
        ),
        (
            vec!["serve", "--", "--flag"],
            WorkerCommandError::RelativeExecutable,
        ),
        (
            vec!["serve", "--", "--"],
            WorkerCommandError::RelativeExecutable,
        ),
    ] {
        assert_eq!(command_error(WorkerCommand::parse(args(&values))), expected);
    }
}

#[test]
fn environment_reads_only_the_fixed_contract_in_order() {
    let values = environment_values();
    let mut requested = Vec::new();
    let parsed = WorkerBootstrapEnvironment::from_lookup(|name| {
        requested.push(name);
        values.get(name).cloned()
    })
    .unwrap();

    assert_eq!(requested, WORKER_BOOTSTRAP_ENV_NAMES);
    assert_eq!(parsed.controller_url(), CONTROLLER_URL);
    assert_eq!(parsed.pod_uid(), POD_UID);
    let debug = format!("{parsed:?}");
    assert!(!debug.contains("controller-sensitive"));
    assert!(!debug.contains(POD_UID));
}

#[test]
fn environment_rejects_every_missing_or_drifted_value_without_echoing_it() {
    for missing in WORKER_BOOTSTRAP_ENV_NAMES {
        let mut values = environment_values();
        values.remove(missing);
        assert_eq!(
            environment_error(WorkerBootstrapEnvironment::from_lookup(|name| {
                values.get(name).cloned()
            })),
            WorkerEnvironmentError::Unavailable { name: missing }
        );
    }

    for (name, invalid) in [
        ("OPENAB_SESSION_CONTROLLER_CA_FILE", "/sensitive/wrong-ca"),
        ("OPENAB_REGISTRATION_TOKEN_FILE", "/sensitive/wrong-token"),
        (
            "OPENAB_REGISTRATION_BINDING_FILE",
            "/sensitive/wrong-binding",
        ),
        ("OPENAB_SESSION_ROOT", "/sensitive/session"),
        ("OPENAB_WORKSPACE", "/sensitive/workspace"),
        ("HOME", "/sensitive/home"),
    ] {
        let mut values = environment_values();
        values.insert(name, invalid.into());
        let error = environment_error(WorkerBootstrapEnvironment::from_lookup(|key| {
            values.get(key).cloned()
        }));
        assert_eq!(error, WorkerEnvironmentError::Invalid { name });
        assert!(!format!("{error:?} {error}").contains("sensitive/wrong"));
    }

    for invalid_url in [
        "ws://controller.example.test/v1/worker",
        "wss://controller.example.test/v1/bridge",
        "wss://user@controller.example.test/v1/worker",
        "wss://controller.example.test/v1/worker?secret=value",
        "wss://controller.example.test/v1/worker#sensitive-fragment",
    ] {
        let mut values = environment_values();
        values.insert("OPENAB_SESSION_CONTROLLER_URL", invalid_url.into());
        assert_eq!(
            environment_error(WorkerBootstrapEnvironment::from_lookup(|name| {
                values.get(name).cloned()
            })),
            WorkerEnvironmentError::Invalid {
                name: "OPENAB_SESSION_CONTROLLER_URL",
            }
        );
    }

    for valid_uid in ["a".to_owned(), "x".repeat(256)] {
        let mut values = environment_values();
        values.insert("OPENAB_WORKER_POD_UID", valid_uid.into());
        assert!(WorkerBootstrapEnvironment::from_lookup(|name| values.get(name).cloned()).is_ok());
    }

    for invalid_uid in [
        String::new(),
        "pod uid".to_owned(),
        "pod/uid".to_owned(),
        "pod\\uid".to_owned(),
        "pod\nuid".to_owned(),
        "pød".to_owned(),
        "x".repeat(257),
    ] {
        let mut values = environment_values();
        values.insert("OPENAB_WORKER_POD_UID", invalid_uid.into());
        assert_eq!(
            environment_error(WorkerBootstrapEnvironment::from_lookup(|name| {
                values.get(name).cloned()
            })),
            WorkerEnvironmentError::Invalid {
                name: "OPENAB_WORKER_POD_UID",
            }
        );
    }
}

#[test]
fn environment_bounds_the_exact_worker_url() {
    let exact_host = "a".repeat(MAX_CLIENT_URL_BYTES - "wss:///v1/worker".len());
    let exact_url = format!("wss://{exact_host}/v1/worker");
    assert_eq!(exact_url.len(), MAX_CLIENT_URL_BYTES);
    let mut values = environment_values();
    values.insert("OPENAB_SESSION_CONTROLLER_URL", exact_url.into());
    assert!(WorkerBootstrapEnvironment::from_lookup(|name| values.get(name).cloned()).is_ok());

    let oversized_host = format!("{exact_host}a");
    let oversized_url = format!("wss://{oversized_host}/v1/worker");
    let mut values = environment_values();
    values.insert("OPENAB_SESSION_CONTROLLER_URL", oversized_url.into());
    assert_eq!(
        environment_error(WorkerBootstrapEnvironment::from_lookup(|name| {
            values.get(name).cloned()
        })),
        WorkerEnvironmentError::Invalid {
            name: "OPENAB_SESSION_CONTROLLER_URL",
        }
    );
}

#[cfg(unix)]
#[test]
fn environment_rejects_non_utf8_values_without_echoing_them() {
    use std::os::unix::ffi::OsStringExt;

    let mut values = environment_values();
    values.insert(
        "OPENAB_WORKER_POD_UID",
        OsString::from_vec(vec![b's', b'e', b'c', b'r', b'e', b't', 0xff]),
    );
    let error = environment_error(WorkerBootstrapEnvironment::from_lookup(|name| {
        values.get(name).cloned()
    }));
    assert_eq!(
        error,
        WorkerEnvironmentError::Invalid {
            name: "OPENAB_WORKER_POD_UID",
        }
    );
    assert!(!format!("{error:?} {error}").contains("secret"));
}

#[test]
fn bootstrap_loads_exact_inputs_and_redacts_all_material() {
    let mut raw_token = [0_u8; 32];
    raw_token.copy_from_slice(&[
        0x00, 0xff, b'\n', b'\r', 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ]);
    let registration = registration_bytes();
    let ca = ca_pem();
    let bootstrap = load_with(
        Cursor::new(raw_token),
        Cursor::new(registration),
        Cursor::new(ca.clone()),
    )
    .unwrap();

    assert_eq!(bootstrap.command().executable(), command().executable());
    assert_eq!(bootstrap.controller_url(), CONTROLLER_URL);
    assert_eq!(bootstrap.pod_uid(), POD_UID);
    assert_eq!(bootstrap.controller_ca_pem(), ca);
    assert_eq!(bootstrap.registration_token(), &raw_token);
    assert_eq!(
        bootstrap.registration().session_id(),
        SessionId::derive("team-sensitive", "discord:thread-sensitive")
    );
    let debug = format!("{bootstrap:?}");
    for sentinel in [
        "secret-token-must-never-appear",
        "acp-sensitive",
        "controller-sensitive",
        POD_UID,
        "binding",
        "CERTIFICATE",
    ] {
        assert!(!debug.contains(sentinel));
    }
}

#[test]
fn token_length_is_exactly_32_raw_bytes() {
    for token in [
        Vec::new(),
        vec![b'a'; 31],
        vec![b'a'; 33],
        [vec![b'a'; 32], vec![b'\n']].concat(),
    ] {
        assert_eq!(
            bootstrap_error(load_with(
                Cursor::new(token),
                Cursor::new(registration_bytes()),
                Cursor::new(ca_pem()),
            )),
            WorkerBootstrapError::InvalidRegistrationTokenLength
        );
    }
    assert!(load_with(
        Cursor::new([
            0x00, 0xff, b'\n', b'\r', 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        Cursor::new(registration_bytes()),
        Cursor::new(ca_pem()),
    )
    .is_ok());
    let mut newline_is_raw = vec![b'a'; 31];
    newline_is_raw.push(b'\n');
    assert!(load_with(
        Cursor::new(newline_is_raw),
        Cursor::new(registration_bytes()),
        Cursor::new(ca_pem()),
    )
    .is_ok());
}

#[test]
fn binding_accepts_the_exact_limit_and_rejects_plus_one_or_invalid_json() {
    let mut exact = registration_bytes();
    exact.resize(MAX_WORKER_REGISTRATION_BINDING_BYTES, b' ');
    assert!(load_with(
        Cursor::new(TOKEN),
        Cursor::new(exact.clone()),
        Cursor::new(ca_pem()),
    )
    .is_ok());

    exact.push(b' ');
    assert_eq!(
        bootstrap_error(load_with(
            Cursor::new(TOKEN),
            Cursor::new(exact),
            Cursor::new(ca_pem()),
        )),
        WorkerBootstrapError::RegistrationBindingTooLarge
    );
    let outer_envelope = openab_kubernetes_session::wire::WorkerToControllerV1::Registration(
        serde_json::from_slice(&registration_bytes()).unwrap(),
    );
    let valid: serde_json::Value = serde_json::from_slice(&registration_bytes()).unwrap();
    let mut wrong_version = valid.clone();
    wrong_version["version"] = serde_json::json!(2);
    let mut wrong_binding_version = valid.clone();
    wrong_binding_version["binding"]["version"] = serde_json::json!(2);
    let mut unknown_field = valid.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("sensitiveUnknown".to_owned(), serde_json::json!(true));
    let mut unknown_binding_field = valid.clone();
    unknown_binding_field["binding"]
        .as_object_mut()
        .unwrap()
        .insert("sensitiveUnknown".to_owned(), serde_json::json!(true));
    let mut zero_generation = valid.clone();
    zero_generation["binding"]["generation"] = serde_json::json!(0);
    let mut nil_attempt = valid.clone();
    nil_attempt["binding"]["attemptId"] = serde_json::json!(Uuid::nil());
    let mut nil_incarnation = valid;
    nil_incarnation["binding"]["incarnationId"] = serde_json::json!(Uuid::nil());

    for invalid in [
        Vec::new(),
        vec![0xff, 0xfe],
        b"{\"sensitive-binding\":".to_vec(),
        b"{}".to_vec(),
        b"null".to_vec(),
        encode_frame(&outer_envelope).unwrap(),
        serde_json::to_vec(&wrong_version).unwrap(),
        serde_json::to_vec(&wrong_binding_version).unwrap(),
        serde_json::to_vec(&unknown_field).unwrap(),
        serde_json::to_vec(&unknown_binding_field).unwrap(),
        serde_json::to_vec(&zero_generation).unwrap(),
        serde_json::to_vec(&nil_attempt).unwrap(),
        serde_json::to_vec(&nil_incarnation).unwrap(),
    ] {
        let error = bootstrap_error(load_with(
            Cursor::new(TOKEN),
            Cursor::new(invalid),
            Cursor::new(ca_pem()),
        ));
        assert_eq!(error, WorkerBootstrapError::InvalidRegistrationBinding);
        assert!(!format!("{error:?} {error}").contains("sensitive-binding"));
    }
}

#[test]
fn ca_accepts_the_exact_limit_and_rejects_plus_one_or_non_certificate_pem() {
    let mut exact = ca_pem();
    exact.resize(MAX_CLIENT_CA_PEM_BYTES, b' ');
    assert!(load_with(
        Cursor::new(TOKEN),
        Cursor::new(registration_bytes()),
        Cursor::new(exact.clone()),
    )
    .is_ok());

    let multiple = [ca_pem(), ca_pem()].concat();
    assert!(load_with(
        Cursor::new(TOKEN),
        Cursor::new(registration_bytes()),
        Cursor::new(multiple),
    )
    .is_ok());

    exact.push(b' ');
    assert_eq!(
        bootstrap_error(load_with(
            Cursor::new(TOKEN),
            Cursor::new(registration_bytes()),
            Cursor::new(exact),
        )),
        WorkerBootstrapError::ControllerCaTooLarge
    );

    let CertifiedKey { signing_key, .. } =
        generate_simple_self_signed(vec!["controller.example.test".to_owned()]).unwrap();
    for invalid in [
        Vec::new(),
        b"not a certificate".to_vec(),
        b"-----BEGIN CERTIFICATE-----\nAQID\n".to_vec(),
        b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n".to_vec(),
        signing_key.serialize_pem().into_bytes(),
    ] {
        assert_eq!(
            bootstrap_error(load_with(
                Cursor::new(TOKEN),
                Cursor::new(registration_bytes()),
                Cursor::new(invalid),
            )),
            WorkerBootstrapError::InvalidControllerCa
        );
    }
}

#[test]
fn bounded_readers_stop_after_each_plus_one_ceiling() {
    let (token_reader, token_count) = CountingReader::new(vec![0_u8; 128]);
    assert_eq!(
        bootstrap_error(load_with(
            token_reader,
            Cursor::new(registration_bytes()),
            Cursor::new(ca_pem()),
        )),
        WorkerBootstrapError::InvalidRegistrationTokenLength
    );
    assert_eq!(token_count.get(), 33);

    let (binding_reader, binding_count) =
        CountingReader::new(vec![b' '; MAX_WORKER_REGISTRATION_BINDING_BYTES + 128]);
    assert_eq!(
        bootstrap_error(load_with(
            Cursor::new(TOKEN),
            binding_reader,
            Cursor::new(ca_pem()),
        )),
        WorkerBootstrapError::RegistrationBindingTooLarge
    );
    assert_eq!(
        binding_count.get(),
        MAX_WORKER_REGISTRATION_BINDING_BYTES + 1
    );

    let (ca_reader, ca_count) = CountingReader::new(vec![b' '; MAX_CLIENT_CA_PEM_BYTES + 128]);
    assert_eq!(
        bootstrap_error(load_with(
            Cursor::new(TOKEN),
            Cursor::new(registration_bytes()),
            ca_reader,
        )),
        WorkerBootstrapError::ControllerCaTooLarge
    );
    assert_eq!(ca_count.get(), MAX_CLIENT_CA_PEM_BYTES + 1);
}

#[test]
fn reader_errors_are_categorized_and_never_expose_sources() {
    let cases = [
        (
            load_with(
                FailingReader,
                Cursor::new(registration_bytes()),
                Cursor::new(ca_pem()),
            ),
            WorkerBootstrapError::RegistrationTokenRead,
        ),
        (
            load_with(Cursor::new(TOKEN), FailingReader, Cursor::new(ca_pem())),
            WorkerBootstrapError::RegistrationBindingRead,
        ),
        (
            load_with(
                Cursor::new(TOKEN),
                Cursor::new(registration_bytes()),
                FailingReader,
            ),
            WorkerBootstrapError::ControllerCaRead,
        ),
    ];
    for (result, expected) in cases {
        let error = bootstrap_error(result);
        assert_eq!(error, expected);
        assert!(!format!("{error:?} {error}").contains("sensitive reader source"));
    }
}

struct FailingReader;

impl Read for FailingReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("sensitive reader source"))
    }
}

struct CountingReader {
    inner: Cursor<Vec<u8>>,
    count: Rc<Cell<usize>>,
}

impl CountingReader {
    fn new(bytes: Vec<u8>) -> (Self, Rc<Cell<usize>>) {
        let count = Rc::new(Cell::new(0));
        (
            Self {
                inner: Cursor::new(bytes),
                count: Rc::clone(&count),
            },
            count,
        )
    }
}

impl Read for CountingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.count.set(self.count.get() + read);
        Ok(read)
    }
}

fn command_error(result: Result<WorkerCommand, WorkerCommandError>) -> WorkerCommandError {
    match result {
        Ok(_) => panic!("command unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn environment_error(
    result: Result<WorkerBootstrapEnvironment, WorkerEnvironmentError>,
) -> WorkerEnvironmentError {
    match result {
        Ok(_) => panic!("environment unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn bootstrap_error(result: Result<WorkerBootstrap, WorkerBootstrapError>) -> WorkerBootstrapError {
    match result {
        Ok(_) => panic!("bootstrap unexpectedly succeeded"),
        Err(error) => error,
    }
}
