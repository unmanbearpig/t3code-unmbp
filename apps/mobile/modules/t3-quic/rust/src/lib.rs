//! Pull-based mobile WebTransport. Registry locks never span an await or a foreign callback.
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{
    collections::HashMap,
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{runtime::Runtime, sync::watch};
use tokio_util::sync::CancellationToken;
use wtransport::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt,
    config::{DnsLookupFuture, DnsResolver, IpBindConfig},
    endpoint::endpoint_side::Client,
    error::{ConnectingError, ConnectionError},
};

#[cfg(any(target_os = "android", feature = "host-jni"))]
mod android;

const ABORTED: i32 = -1;
const NETWORK: i32 = -2;
const REJECTED: i32 = -3;
const PROTOCOL: i32 = -4;
const CAPACITY: i32 = -5;
#[cfg(any(target_os = "android", feature = "host-jni"))]
const UNAVAILABLE: i32 = -6;
const MAX_BYTES: usize = 65_536;
const MAX_STREAMS: usize = 65; // Control plus 64 application streams. CONNECT has separate native credit.
const MAX_OPERATIONS: usize = 2 * MAX_STREAMS + 66;
const MAX_OWNERS: usize = 64;
const MAX_SAFE_ID: u64 = (1 << 53) - 1;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("t3-quic")
            .enable_all()
            .build()
            .expect("QUIC runtime")
    })
}
fn registry() -> &'static Mutex<HashMap<u64, Arc<Owner>>> {
    static OWNERS: OnceLock<Mutex<HashMap<u64, Arc<Owner>>>> = OnceLock::new();
    OWNERS.get_or_init(Default::default)
}
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq)]
struct Closed {
    status: i32,
    code: u64,
}
struct LiveConnection {
    connection: Connection,
    _endpoint: Endpoint<Client>,
}
struct Owner {
    id: u64,
    cancelled: CancellationToken,
    connecting: AtomicBool,
    opening: AtomicUsize,
    connection: Mutex<Option<LiveConnection>>,
    streams: Mutex<HashMap<u64, Arc<Stream>>>,
    next_stream: AtomicU64,
    operations: Mutex<HashMap<u64, PendingOperation>>,
    closed: watch::Sender<Option<Closed>>,
}
struct PendingOperation {
    cancel: CancellationToken,
    kind: u32,
    handle: u64,
}

struct Stream {
    send: Mutex<Option<SendStream>>,
    recv: Mutex<Option<RecvStream>>,
    writing: AtomicBool,
    reading: AtomicBool,
    send_ended: AtomicBool,
    recv_ended: AtomicBool,
    send_cancel: CancellationToken,
    recv_cancel: CancellationToken,
    reset_code: AtomicU64,
    stop_code: AtomicU64,
}
impl Owner {
    fn new(id: u64) -> Self {
        Self {
            id,
            cancelled: CancellationToken::new(),
            connecting: AtomicBool::new(false),
            opening: AtomicUsize::new(0),
            connection: Mutex::new(None),
            streams: Mutex::new(HashMap::new()),
            next_stream: AtomicU64::new(1),
            operations: Mutex::new(HashMap::new()),
            closed: watch::channel(None).0,
        }
    }
    fn close(&self, closed: Closed) {
        self.closed.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some(closed);
                true
            }
        });
        self.cancelled.cancel();
        // Removal happens before native cleanup, so a new owner never reuses a stale generation.
        lock(registry()).remove(&self.id);
        if let Some(live) = lock(&self.connection).take() {
            live.connection
                .close(VarInt::from_u32(closed.code.saturating_sub(1) as u32), b"");
            live._endpoint.close(VarInt::from_u32(0), b"");
        }
        lock(&self.streams).clear();
    }
    fn connection(&self) -> Result<Connection, i32> {
        if self.cancelled.is_cancelled() {
            return Err(ABORTED);
        }
        lock(&self.connection)
            .as_ref()
            .map(|live| live.connection.clone())
            .ok_or(PROTOCOL)
    }
    fn stream(&self, id: u64) -> Result<Arc<Stream>, i32> {
        lock(&self.streams).get(&id).cloned().ok_or(PROTOCOL)
    }
    fn remove_ended(&self, id: u64, stream: &Stream) {
        if stream.send_ended.load(Ordering::Acquire) && stream.recv_ended.load(Ordering::Acquire) {
            lock(&self.streams).remove(&id);
        }
    }
}
impl Stream {
    fn new(send: SendStream, recv: RecvStream) -> Self {
        Self {
            send: Mutex::new(Some(send)),
            recv: Mutex::new(Some(recv)),
            writing: AtomicBool::new(false),
            reading: AtomicBool::new(false),
            send_ended: AtomicBool::new(false),
            recv_ended: AtomicBool::new(false),
            send_cancel: CancellationToken::new(),
            recv_cancel: CancellationToken::new(),
            reset_code: AtomicU64::new(0),
            stop_code: AtomicU64::new(0),
        }
    }
    fn reset(&self, code: u32) {
        self.reset_code.store(code as u64, Ordering::Release);
        self.send_ended.store(true, Ordering::Release);
        self.send_cancel.cancel();
        if let Some(mut send) = lock(&self.send).take() {
            let _ = send.reset(stream_error_code(code));
        }
    }
    fn stop(&self, code: u32) {
        self.stop_code.store(code as u64, Ordering::Release);
        self.recv_ended.store(true, Ordering::Release);
        self.recv_cancel.cancel();
        if let Some(recv) = lock(&self.recv).take() {
            recv.stop(stream_error_code(code));
        }
    }
}
// WebTransport HTTP/3 application errors skip every reserved 0x1f codepoint.
fn stream_error_code(code: u32) -> VarInt {
    VarInt::try_from_u64(0x52e4_a40f_a8db + code as u64 + code as u64 / 0x1e)
        .expect("u32 WebTransport code")
}

#[derive(Debug)]
struct FixedAddress(SocketAddr);
impl DnsResolver for FixedAddress {
    fn resolve(&self, _: &str) -> Pin<Box<dyn DnsLookupFuture>> {
        let address = self.0;
        Box::pin(async move { Ok(Some(address)) })
    }
}
fn public_tls() -> rustls::ClientConfig {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}
fn client_config(address: SocketAddr, mut tls: rustls::ClientConfig) -> ClientConfig {
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let mut transport = wtransport::quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(72u32.into());
    transport.max_concurrent_uni_streams(8u32.into());
    transport.stream_receive_window((256u32 * 1024).into());
    transport.receive_window((4u32 * 1024 * 1024).into());
    transport.send_window(4 * 1024 * 1024);
    ClientConfig::builder()
        .with_bind_config(if address.is_ipv4() {
            IpBindConfig::InAddrAnyV4
        } else {
            IpBindConfig::InAddrAnyV6
        })
        .with_custom_tls_and_transport(tls, transport)
        .dns_resolver(FixedAddress(address))
        .build()
}
struct EndpointGuard(Option<Endpoint<Client>>);
impl Drop for EndpointGuard {
    fn drop(&mut self) {
        if let Some(endpoint) = self.0.take() {
            endpoint.close(VarInt::from_u32(0), b"");
        }
    }
}
async fn connect_address(
    url: String,
    address: SocketAddr,
    delay: Duration,
    tls: rustls::ClientConfig,
) -> Result<LiveConnection, i32> {
    tokio::time::sleep(delay).await;
    let endpoint = Endpoint::client(client_config(address, tls)).map_err(|_| NETWORK)?;
    let mut guard = EndpointGuard(Some(endpoint));
    let connection = guard
        .0
        .as_ref()
        .ok_or(NETWORK)?
        .connect(url)
        .await
        .map_err(|error| match error {
            ConnectingError::SessionRejected => REJECTED,
            ConnectingError::InvalidUrl(_) => PROTOCOL,
            _ => NETWORK,
        })?;
    Ok(LiveConnection {
        connection,
        _endpoint: guard.0.take().ok_or(NETWORK)?,
    })
}
async fn connect(url: String, tls: rustls::ClientConfig) -> Result<LiveConnection, i32> {
    let parsed = url::Url::parse(&url).map_err(|_| PROTOCOL)?;
    if parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(PROTOCOL);
    }
    let host = parsed.host_str().ok_or(PROTOCOL)?;
    let port = parsed.port_or_known_default().ok_or(PROTOCOL)?;
    let addresses: Vec<_> = match parsed.host().ok_or(PROTOCOL)? {
        url::Host::Ipv4(ip) => vec![SocketAddr::new(ip.into(), port)],
        url::Host::Ipv6(ip) => vec![SocketAddr::new(ip.into(), port)],
        _ => tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| NETWORK)?
            .collect(),
    };
    // getaddrinfo follows the OS/VPN resolver. Race both families using ordinary wildcard UDP sockets.
    let mut attempts = FuturesUnordered::new();
    let mut seen = Vec::new();
    for address in addresses {
        if seen.len() >= 16 {
            break;
        }
        if seen.contains(&address) {
            continue;
        }
        let delay = Duration::from_millis((seen.len().min(4) * 150) as u64);
        seen.push(address);
        attempts.push(connect_address(url.clone(), address, delay, tls.clone()));
    }
    let mut last = NETWORK;
    while let Some(result) = attempts.next().await {
        match result {
            Ok(live) => return Ok(live),
            Err(error) => last = error,
        }
    }
    Err(last)
}

struct OpeningGuard<'a>(&'a AtomicUsize);
impl Drop for OpeningGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ResultValue {
    status: i32,
    handle: u64,
    bytes: Vec<u8>,
}
impl ResultValue {
    fn empty(status: i32) -> Self {
        Self {
            status,
            handle: 0,
            bytes: Vec::new(),
        }
    }
}
async fn perform(
    owner: Arc<Owner>,
    kind: u32,
    handle: u64,
    data: Vec<u8>,
    argument: u32,
    cancel: CancellationToken,
) -> ResultValue {
    let result = perform_result(Arc::clone(&owner), kind, handle, data, argument, cancel).await;
    if kind == 1 && result.is_err() {
        owner.close(Closed {
            status: result.as_ref().err().copied().unwrap_or(PROTOCOL),
            code: 0,
        });
    }
    match result {
        Ok(value) => value,
        Err(status) => ResultValue::empty(status),
    }
}
async fn perform_result(
    owner: Arc<Owner>,
    kind: u32,
    handle: u64,
    data: Vec<u8>,
    argument: u32,
    cancel: CancellationToken,
) -> Result<ResultValue, i32> {
    if kind != 6 && (cancel.is_cancelled() || owner.cancelled.is_cancelled()) {
        return Err(ABORTED);
    }
    match kind {
        1 => {
            if owner.connecting.swap(true, Ordering::AcqRel) {
                return Err(PROTOCOL);
            }
            let url = String::from_utf8(data).map_err(|_| PROTOCOL)?;
            let live = tokio::select! { biased;
                _ = cancel.cancelled() => return Err(ABORTED),
                _ = owner.cancelled.cancelled() => return Err(ABORTED),
                result = tokio::time::timeout(Duration::from_secs(2), connect(url, public_tls())) => result.map_err(|_| NETWORK)??,
            };
            let connection = live.connection.clone();
            {
                let mut slot = lock(&owner.connection);
                if owner.cancelled.is_cancelled() {
                    live._endpoint.close(VarInt::from_u32(0), b"");
                    return Err(ABORTED);
                }
                *slot = Some(live);
            }
            let watching_owner = Arc::clone(&owner);
            runtime().spawn(async move {
                let error = tokio::select! { biased;
                    _ = watching_owner.cancelled.cancelled() => return,
                    error = connection.closed() => error,
                };
                let closed = match error {
                    ConnectionError::ApplicationClosed(close) => Closed {
                        status: 2,
                        code: close.code().into_inner().min(u32::MAX as u64) + 1,
                    },
                    ConnectionError::LocallyClosed => Closed { status: 2, code: 0 },
                    _ => Closed {
                        status: NETWORK,
                        code: 0,
                    },
                };
                watching_owner.close(closed);
            });
            Ok(ResultValue::empty(0))
        }
        2 => {
            // Reserving before await also bounds concurrent native opens.
            let opening = owner.opening.fetch_add(1, Ordering::AcqRel);
            let _opening = OpeningGuard(&owner.opening);
            if opening >= MAX_STREAMS {
                return Err(CAPACITY);
            }
            let id = owner.next_stream.fetch_add(1, Ordering::Relaxed);
            if id > MAX_SAFE_ID {
                return Err(CAPACITY);
            }
            let connection = owner.connection()?;
            if lock(&owner.streams).len() >= MAX_STREAMS {
                return Err(CAPACITY);
            }
            let opened = async {
                connection
                    .open_bi()
                    .await
                    .map_err(|_| NETWORK)?
                    .await
                    .map_err(|_| NETWORK)
            };
            let (mut send, recv) = tokio::select! { biased;
                _ = cancel.cancelled() => return Err(ABORTED),
                _ = owner.cancelled.cancelled() => return Err(ABORTED),
                result = opened => result?,
            };
            let mut streams = lock(&owner.streams);
            if cancel.is_cancelled()
                || owner.cancelled.is_cancelled()
                || streams.len() >= MAX_STREAMS
            {
                let _ = send.reset(stream_error_code(0));
                recv.stop(stream_error_code(0));
                return Err(ABORTED);
            }
            streams.insert(id, Arc::new(Stream::new(send, recv)));
            Ok(ResultValue {
                status: 0,
                handle: id,
                bytes: Vec::new(),
            })
        }
        3 => {
            if argument == 0 || argument as usize > MAX_BYTES {
                return Err(CAPACITY);
            }
            let stream = owner.stream(handle)?;
            if stream.recv_cancel.is_cancelled() {
                return Err(ABORTED);
            }
            if stream.reading.swap(true, Ordering::AcqRel) {
                return Err(CAPACITY);
            }
            let mut recv = lock(&stream.recv).take();
            let result = if let Some(recv) = recv.as_mut() {
                let mut bytes = vec![0; argument as usize];
                let read = tokio::select! { biased;
                    _ = cancel.cancelled() => Err(ABORTED),
                    _ = owner.cancelled.cancelled() => Err(ABORTED),
                    _ = stream.recv_cancel.cancelled() => Err(ABORTED),
                    result = recv.read(&mut bytes) => result.map_err(|_| NETWORK),
                };
                match read {
                    Ok(Some(length)) => {
                        bytes.truncate(length);
                        Ok(ResultValue {
                            status: 0,
                            handle: 0,
                            bytes,
                        })
                    }
                    Ok(None) => {
                        stream.recv_ended.store(true, Ordering::Release);
                        Ok(ResultValue::empty(1))
                    }
                    Err(error) => {
                        stream.recv_ended.store(true, Ordering::Release);
                        Err(error)
                    }
                }
            } else if stream.recv_ended.load(Ordering::Acquire) {
                Ok(ResultValue::empty(1))
            } else {
                Err(PROTOCOL)
            };
            let mut slot = lock(&stream.recv);
            if stream.recv_ended.load(Ordering::Acquire) || stream.recv_cancel.is_cancelled() {
                if let Some(recv) = recv {
                    recv.stop(stream_error_code(
                        stream.stop_code.load(Ordering::Acquire) as u32
                    ));
                }
            } else {
                *slot = recv;
            }
            drop(slot);
            stream.reading.store(false, Ordering::Release);
            owner.remove_ended(handle, &stream);
            result
        }
        4 | 5 => {
            let stream = owner.stream(handle)?;
            if stream.send_cancel.is_cancelled() {
                return Err(ABORTED);
            }
            if stream.writing.swap(true, Ordering::AcqRel) {
                return Err(CAPACITY);
            }
            let mut send = lock(&stream.send).take();
            let result = if let Some(send) = send.as_mut() {
                tokio::select! { biased;
                    _ = cancel.cancelled() => Err(ABORTED),
                    _ = owner.cancelled.cancelled() => Err(ABORTED),
                    _ = stream.send_cancel.cancelled() => Err(ABORTED),
                    result = async {
                        if kind == 5 {
                            // Quinn finish is synchronous. wtransport finish waits for peer ACK.
                            send.quic_stream_mut().finish().map_err(|_| NETWORK)
                        } else { send.write_all(&data).await.map_err(|_| NETWORK) }
                    } => result,
                }
            } else {
                Err(PROTOCOL)
            };
            let mut slot = lock(&stream.send);
            if result.is_err() || kind == 5 || stream.send_cancel.is_cancelled() {
                stream.send_ended.store(true, Ordering::Release);
                if (result.is_err() || stream.send_cancel.is_cancelled())
                    && let Some(send) = send.as_mut()
                {
                    let _ = send.reset(stream_error_code(
                        stream.reset_code.load(Ordering::Acquire) as u32,
                    ));
                }
            } else {
                *slot = send;
            }
            drop(slot);
            stream.writing.store(false, Ordering::Release);
            owner.remove_ended(handle, &stream);
            result.map(|_| ResultValue::empty(0))
        }
        6 => {
            let mut closed = owner.closed.subscribe();
            loop {
                if let Some(value) = *closed.borrow_and_update() {
                    return Ok(ResultValue {
                        status: value.status,
                        handle: value.code,
                        bytes: Vec::new(),
                    });
                }
                tokio::select! { biased;
                    _ = cancel.cancelled() => return Err(ABORTED),
                    changed = closed.changed() => if changed.is_err() { return Err(ABORTED); },
                }
            }
        }
        _ => Err(PROTOCOL),
    }
}

type Completion = Box<dyn FnOnce(ResultValue) + Send + 'static>;
fn submit(
    owner_id: u64,
    operation: u64,
    kind: u32,
    handle: u64,
    data: Vec<u8>,
    argument: u32,
    complete: Completion,
) -> i32 {
    if data.len() > MAX_BYTES || operation == 0 || operation > MAX_SAFE_ID {
        return CAPACITY;
    }
    let Some(owner) = lock(registry()).get(&owner_id).cloned() else {
        return ABORTED;
    };
    let cancel = CancellationToken::new();
    {
        let mut operations = lock(&owner.operations);
        if operations.len() >= MAX_OPERATIONS || operations.contains_key(&operation) {
            return CAPACITY;
        }
        operations.insert(
            operation,
            PendingOperation {
                cancel: cancel.clone(),
                kind,
                handle,
            },
        );
    }
    runtime().spawn(async move {
        let outcome = AssertUnwindSafe(perform(
            Arc::clone(&owner),
            kind,
            handle,
            data,
            argument,
            cancel,
        ))
        .catch_unwind()
        .await;
        let value = outcome.unwrap_or_else(|_| {
            owner.close(Closed {
                status: PROTOCOL,
                code: 0,
            });
            ResultValue::empty(PROTOCOL)
        });
        lock(&owner.operations).remove(&operation);
        // Never call Kotlin/Swift with a Rust registry or stream mutex held.
        let _ = catch_unwind(AssertUnwindSafe(|| complete(value)));
    });
    0
}

/// Allocates a generation before any connect can start. Zero means unavailable/capacity.
#[unsafe(no_mangle)]
pub extern "C" fn t3_quic_owner_create() -> u64 {
    catch_unwind(|| {
        let _ = runtime();
        let mut owners = lock(registry());
        if owners.len() >= MAX_OWNERS {
            return 0;
        }
        let id = NEXT_OWNER.fetch_add(1, Ordering::Relaxed);
        if id > MAX_SAFE_ID {
            return 0;
        }
        owners.insert(id, Arc::new(Owner::new(id)));
        id
    })
    .unwrap_or(0)
}

pub type CCallback = unsafe extern "C" fn(u64, u64, i32, u64, *mut u8, usize);
/// Input is copied before returning; accepted operations call back exactly once.
/// # Safety
/// `data` must reference `length` bytes, and callback/context must remain valid until completion.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn t3_quic_submit(
    owner: u64,
    operation: u64,
    kind: u32,
    handle: u64,
    data: *const u8,
    length: usize,
    argument: u32,
    callback: CCallback,
    context: u64,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if length > MAX_BYTES || (length > 0 && data.is_null()) {
            return CAPACITY;
        }
        let data = if length == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(data, length) }.to_vec()
        };
        submit(
            owner,
            operation,
            kind,
            handle,
            data,
            argument,
            Box::new(move |value| {
                let bytes = value.bytes.into_boxed_slice();
                let length = bytes.len();
                let pointer = if length == 0 {
                    std::ptr::null_mut()
                } else {
                    Box::into_raw(bytes) as *mut u8
                };
                unsafe {
                    callback(
                        context,
                        operation,
                        value.status,
                        value.handle,
                        pointer,
                        length,
                    );
                }
            }),
        )
    }))
    .unwrap_or(PROTOCOL)
}
/// # Safety
/// Free exactly once with the original pointer/length returned by a completion callback.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn t3_quic_buffer_free(data: *mut u8, length: usize) {
    let _ = catch_unwind(|| {
        if !data.is_null() {
            drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(data, length)) });
        }
    });
}
#[unsafe(no_mangle)]
pub extern "C" fn t3_quic_cancel(owner: u64, operation: u64) {
    let _ = catch_unwind(|| {
        if let Some(owner) = lock(registry()).get(&owner).cloned()
            && let Some(operation) = lock(&owner.operations).get(&operation)
        {
            operation.cancel.cancel();
        }
    });
}
#[unsafe(no_mangle)]
pub extern "C" fn t3_quic_close(owner: u64, code: u32) {
    let _ = catch_unwind(|| {
        let value = lock(registry()).get(&owner).cloned();
        if let Some(owner) = value {
            owner.close(Closed {
                status: 2,
                code: code as u64 + 1,
            });
        }
    });
}
#[unsafe(no_mangle)]
pub extern "C" fn t3_quic_reset(owner: u64, stream: u64, code: u32) {
    let _ = catch_unwind(|| {
        if let Some(owner) = lock(registry()).get(&owner).cloned()
            && let Ok(value) = owner.stream(stream)
        {
            // Publish the peer code before a cancellation can wake task cleanup.
            value.reset(code);
            for operation in lock(&owner.operations).values() {
                if operation.handle == stream && matches!(operation.kind, 4 | 5) {
                    operation.cancel.cancel();
                }
            }
            owner.remove_ended(stream, &value);
        }
    });
}
#[unsafe(no_mangle)]
pub extern "C" fn t3_quic_stop(owner: u64, stream: u64, code: u32) {
    let _ = catch_unwind(|| {
        if let Some(owner) = lock(registry()).get(&owner).cloned()
            && let Ok(value) = owner.stream(stream)
        {
            value.stop(code);
            for operation in lock(&owner.operations).values() {
                if operation.handle == stream && operation.kind == 3 {
                    operation.cancel.cancel();
                }
            }
            owner.remove_ended(stream, &value);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use wtransport::{
        Identity, ServerConfig,
        tls::{Certificate, CertificateChain, PrivateKey},
    };

    fn result_channel(
        owner: u64,
        operation: u64,
        kind: u32,
        handle: u64,
        data: Vec<u8>,
        argument: u32,
    ) -> oneshot::Receiver<ResultValue> {
        let (tx, rx) = oneshot::channel();
        assert_eq!(
            submit(
                owner,
                operation,
                kind,
                handle,
                data,
                argument,
                Box::new(move |value| {
                    let _ = tx.send(value);
                })
            ),
            0
        );
        rx
    }
    async fn result(rx: oneshot::Receiver<ResultValue>) -> ResultValue {
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("operation deadline")
            .expect("completion")
    }
    fn fixture() -> (
        Endpoint<wtransport::endpoint::endpoint_side::Server>,
        rustls::ClientConfig,
    ) {
        fixture_with_window(256 * 1024)
    }
    fn fixture_with_window(
        window: u32,
    ) -> (
        Endpoint<wtransport::endpoint::endpoint_side::Server>,
        rustls::ClientConfig,
    ) {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .unwrap();
        let identity = Identity::new(
            CertificateChain::new(vec![
                Certificate::from_der(key.cert.der().to_vec()).unwrap(),
            ]),
            PrivateKey::from_der_pkcs8(key.signing_key.serialize_der()),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(key.cert.der().clone()).unwrap();
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut transport = wtransport::quinn::TransportConfig::default();
        transport.stream_receive_window(window.into());
        let endpoint = Endpoint::server(
            ServerConfig::builder()
                .with_bind_address("127.0.0.1:0".parse().unwrap())
                .with_custom_transport(identity, transport)
                .build(),
        )
        .unwrap();
        (endpoint, tls)
    }
    async fn connected_owner() -> (
        u64,
        Arc<Owner>,
        Endpoint<wtransport::endpoint::endpoint_side::Server>,
        Connection,
    ) {
        connected_owner_with_window(256 * 1024).await
    }
    async fn connected_owner_with_window(
        window: u32,
    ) -> (
        u64,
        Arc<Owner>,
        Endpoint<wtransport::endpoint::endpoint_side::Server>,
        Connection,
    ) {
        let (server, tls) = fixture_with_window(window);
        let address = server.local_addr().unwrap();
        let client = connect(format!("https://localhost:{}/rpc", address.port()), tls);
        let accept = async { server.accept().await.await.unwrap().accept().await.unwrap() };
        let (live, peer) = tokio::join!(client, accept);
        let id = t3_quic_owner_create();
        assert_ne!(id, 0);
        let owner = lock(registry()).get(&id).unwrap().clone();
        *lock(&owner.connection) = Some(live.unwrap());
        (id, owner, server, peer)
    }
    #[test]
    fn mapped_application_codes_skip_http3_reserved_values() {
        for code in [0, 1, 29, 30, 31, 42, 43, 65_535, u32::MAX] {
            let wire = stream_error_code(code).into_inner();
            assert_eq!(wire, 0x52e4_a40f_a8db + code as u64 + code as u64 / 30);
            assert_ne!((wire - 0x21) % 0x1f, 0);
        }
    }
    #[tokio::test]
    async fn cancelled_connect_releases_its_owner_and_settles_once() {
        let id = t3_quic_owner_create();
        let owner = lock(registry()).get(&id).unwrap().clone();
        let rx = result_channel(id, 1, 1, 0, b"https://127.0.0.1:9/rpc".to_vec(), 0);
        t3_quic_cancel(id, 1);
        assert_eq!(result(rx).await.status, ABORTED);
        assert!(owner.cancelled.is_cancelled());
        assert!(!lock(registry()).contains_key(&id));
        assert_eq!(
            submit(
                id,
                2,
                2,
                0,
                vec![],
                0,
                Box::new(|_| panic!("rejected submit callback"))
            ),
            ABORTED
        );
    }
    #[tokio::test]
    async fn close_settles_pending_wait_without_native_acknowledgement() {
        let id = t3_quic_owner_create();
        let owner = lock(registry()).get(&id).unwrap().clone();
        let closed = result_channel(id, 1, 6, 0, vec![], 0);
        t3_quic_close(id, 42);
        let closed = result(closed).await;
        assert_eq!((closed.status, closed.handle), (2, 43));
        assert!(owner.cancelled.is_cancelled());
        t3_quic_close(id, 42);
        assert!(!lock(registry()).contains_key(&id));
    }
    #[tokio::test]
    async fn production_roots_reject_the_fixture_ca() {
        let (server, _) = fixture();
        let address = server.local_addr().unwrap();
        let client = connect(
            format!("https://127.0.0.1:{}/rpc", address.port()),
            public_tls(),
        );
        let accept = async {
            let _ = server.accept().await.await;
        };
        let (result, _) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(client, accept)
        })
        .await
        .unwrap();
        assert!(result.is_err());
        server.close(VarInt::from_u32(0), b"");
    }
    #[tokio::test]
    async fn binary_stream_read_finish_and_cancellation_are_independent() {
        let (id, owner, server, peer) = connected_owner().await;
        let opened = result(result_channel(id, 1, 2, 0, vec![], 0)).await;
        assert_eq!(opened.status, 0);
        // HTTP/3 stream acceptance waits for the WebTransport header. Opening writes it.
        let (mut peer_send, mut peer_recv) = peer.accept_bi().await.unwrap();
        let payload: Vec<u8> = (0..65_536).map(|i| (i % 251) as u8).collect();
        let sent = result(result_channel(id, 2, 4, opened.handle, payload.clone(), 0)).await;
        assert_eq!(sent.status, 0);
        let mut received = vec![0; payload.len()];
        peer_recv.read_exact(&mut received).await.unwrap();
        assert_eq!(received, payload);
        let read = perform_result(
            Arc::clone(&owner),
            3,
            opened.handle,
            vec![],
            MAX_BYTES as u32,
            CancellationToken::new(),
        );
        tokio::pin!(read);
        assert!(futures_util::poll!(&mut read).is_pending());
        let concurrent = perform_result(
            Arc::clone(&owner),
            3,
            opened.handle,
            vec![],
            MAX_BYTES as u32,
            CancellationToken::new(),
        )
        .await;
        assert_eq!(concurrent.err(), Some(CAPACITY));
        peer_send.write_all(b"binary\0reply").await.unwrap();
        let received = read.await.unwrap();
        assert_eq!(received.bytes, b"binary\0reply");
        let finishing = result(result_channel(id, 5, 5, opened.handle, vec![], 0)).await;
        assert_eq!(finishing.status, 0); // Does not wait for the peer to read/ACK FIN.
        let stalled = result_channel(id, 6, 3, opened.handle, vec![], 256);
        t3_quic_stop(id, opened.handle, 43);
        assert_eq!(result(stalled).await.status, ABORTED);
        assert!(lock(&owner.streams).is_empty());
        let stopped = tokio::time::timeout(Duration::from_secs(5), peer_send.stopped())
            .await
            .unwrap();
        assert!(
            matches!(stopped, wtransport::error::StreamWriteError::Stopped(code) if code == stream_error_code(43))
        );
        t3_quic_close(id, 0);
        server.close(VarInt::from_u32(0), b"");
    }
    type TestOperation =
        Pin<Box<dyn std::future::Future<Output = Result<ResultValue, i32>> + Send>>;
    struct EagerOperation {
        future: Mutex<Option<TestOperation>>,
        completed: Mutex<Option<i32>>,
        settlements: AtomicUsize,
    }
    impl EagerOperation {
        fn poll(self: &Arc<Self>) {
            // Cancellation can wake inline while another network wake is polling.
            let Ok(mut future) = self.future.try_lock() else {
                return;
            };
            let Some(operation) = future.as_mut() else {
                return;
            };
            let waker = std::task::Waker::from(Arc::clone(self));
            let mut context = std::task::Context::from_waker(&waker);
            if let std::task::Poll::Ready(result) = operation.as_mut().poll(&mut context) {
                *lock(&self.completed) = Some(match result {
                    Ok(value) => value.status,
                    Err(status) => status,
                });
                self.settlements.fetch_add(1, Ordering::Relaxed);
                *future = None;
            }
        }
    }
    impl std::task::Wake for EagerOperation {
        fn wake(self: Arc<Self>) {
            self.poll();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.poll();
        }
    }
    #[tokio::test]
    async fn explicit_half_codes_survive_inline_pending_operation_cancellation() {
        for kind in [3, 4] {
            let (id, owner, server, peer) = connected_owner_with_window(4096).await;
            let opened = result(result_channel(id, 1, 2, 0, vec![], 0)).await;
            let (mut peer_send, mut peer_recv) = peer.accept_bi().await.unwrap();
            let cancel = CancellationToken::new();
            lock(&owner.operations).insert(
                2,
                PendingOperation {
                    cancel: cancel.clone(),
                    kind,
                    handle: opened.handle,
                },
            );
            let operation = Arc::new(EagerOperation {
                future: Mutex::new(Some(Box::pin(perform_result(
                    Arc::clone(&owner),
                    kind,
                    opened.handle,
                    if kind == 4 {
                        vec![7; MAX_BYTES]
                    } else {
                        vec![]
                    },
                    MAX_BYTES as u32,
                    cancel,
                )))),
                completed: Mutex::new(None),
                settlements: AtomicUsize::new(0),
            });
            operation.poll();
            assert!(
                lock(&operation.completed).is_none(),
                "half must be blocked before reset/stop"
            );
            if kind == 3 {
                t3_quic_stop(id, opened.handle, 43);
            } else {
                t3_quic_reset(id, opened.handle, 42);
            }
            assert_eq!(*lock(&operation.completed), Some(ABORTED));
            assert_eq!(operation.settlements.load(Ordering::Relaxed), 1);
            if kind == 3 {
                let stopped = tokio::time::timeout(Duration::from_secs(5), peer_send.stopped())
                    .await
                    .unwrap();
                assert!(
                    matches!(stopped, wtransport::error::StreamWriteError::Stopped(code) if code == stream_error_code(43))
                );
                t3_quic_reset(id, opened.handle, 42);
            } else {
                let reset = tokio::time::timeout(Duration::from_secs(5), async {
                    let mut bytes = [0; 4096];
                    loop {
                        if let Err(error) = peer_recv.read(&mut bytes).await {
                            break error;
                        }
                    }
                })
                .await
                .unwrap();
                assert!(
                    matches!(reset, wtransport::error::StreamReadError::Reset(code) if code == stream_error_code(42))
                );
                t3_quic_stop(id, opened.handle, 43);
            }
            lock(&owner.operations).remove(&2);
            assert!(lock(&owner.streams).is_empty());
            assert!(lock(&owner.operations).is_empty());
            assert_eq!(owner.opening.load(Ordering::Acquire), 0);
            t3_quic_close(id, 0);
            server.close(VarInt::from_u32(0), b"");
        }
    }
    unsafe extern "C" fn ffi_completion(
        context: u64,
        _: u64,
        status: i32,
        handle: u64,
        data: *mut u8,
        length: usize,
    ) {
        let bytes = if length == 0 {
            vec![]
        } else {
            unsafe { std::slice::from_raw_parts(data, length) }.to_vec()
        };
        unsafe {
            t3_quic_buffer_free(data, length);
        }
        let sender = unsafe { Box::from_raw(context as *mut oneshot::Sender<ResultValue>) };
        let _ = sender.send(ResultValue {
            status,
            handle,
            bytes,
        });
    }
    #[tokio::test]
    async fn ffi_copies_binary_output_and_releases_the_completion_buffer() {
        let (id, _, server, peer) = connected_owner().await;
        let opened = result(result_channel(id, 1, 2, 0, vec![], 0)).await;
        let (mut send, _) = peer.accept_bi().await.unwrap();
        let payload = vec![0xf3; MAX_BYTES];
        send.write_all(&payload).await.unwrap();
        let (tx, rx) = oneshot::channel();
        let context = Box::into_raw(Box::new(tx)) as u64;
        assert_eq!(
            unsafe {
                t3_quic_submit(
                    id,
                    2,
                    3,
                    opened.handle,
                    std::ptr::null(),
                    0,
                    MAX_BYTES as u32,
                    ffi_completion,
                    context,
                )
            },
            0
        );
        let value = result(rx).await;
        assert_eq!(value.status, 0);
        assert!(!value.bytes.is_empty());
        assert!(value.bytes.iter().all(|byte| *byte == 0xf3));
        t3_quic_close(id, 0);
        server.close(VarInt::from_u32(0), b"");
    }
    #[tokio::test]
    async fn oversize_and_duplicate_operations_do_not_allocate_unbounded_buffers() {
        let id = t3_quic_owner_create();
        assert_eq!(
            submit(
                id,
                1,
                4,
                0,
                vec![0; MAX_BYTES + 1],
                0,
                Box::new(|_| panic!("invalid operation callback"))
            ),
            CAPACITY
        );
        let pending = result_channel(id, 1, 6, 0, vec![], 0);
        assert_eq!(
            submit(
                id,
                1,
                6,
                0,
                vec![],
                0,
                Box::new(|_| panic!("duplicate callback"))
            ),
            CAPACITY
        );
        t3_quic_close(id, 0);
        assert_eq!(result(pending).await.status, 2);
    }
}
