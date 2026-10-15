use anyhow::{bail, Context, Result};
use jni::objects::{JClass, JString};
use jni::sys::{jint, jstring};
use jni::JNIEnv;
use serde::Serialize;
use std::io::Cursor;
use std::net::UdpSocket;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use wtransport::endpoint::ConnectOptions;
use wtransport::{ClientConfig, Connection, Endpoint, VarInt};

const PAYLOAD_BYTES: usize = 256 * 1024;
const STREAMS: usize = 4;

#[derive(Serialize)]
pub struct Report {
    pub ok: bool,
    pub implementation: &'static str,
    pub target: &'static str,
    pub os: &'static str,
    pub elapsed_ms: u128,
    pub checks: Vec<&'static str>,
    pub error: Option<String>,
}

impl Report {
    fn new() -> Self {
        Self {
            ok: false,
            implementation: "wtransport-0.7.2/quinn/rustls-ring",
            target: std::env::consts::ARCH,
            os: std::env::consts::OS,
            elapsed_ms: 0,
            checks: Vec::new(),
            error: None,
        }
    }
}

/// Runs on a worker thread. An explicit root set keeps TLS verification identical on host and
/// Android; production must supply Android trust roots or a deliberately chosen bundled store.
pub fn run(url: &str, ca_pem: &str, token: &str, timeout_ms: u64) -> Report {
    let started = Instant::now();
    let mut report = Report::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build();
    let result = match runtime {
        Ok(runtime) => runtime.block_on(probe(url, ca_pem, token, timeout_ms, &mut report)),
        Err(error) => Err(error.into()),
    };
    match result {
        Ok(()) => report.ok = true,
        Err(error) => report.error = Some(format!("{error:#}")),
    }
    report.elapsed_ms = started.elapsed().as_millis();
    report
}

async fn probe(
    url: &str,
    ca_pem: &str,
    token: &str,
    timeout_ms: u64,
    report: &mut Report,
) -> Result<()> {
    if !(100..=60_000).contains(&timeout_ms) {
        bail!("timeout_ms must be between 100 and 60000");
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut Cursor::new(ca_pem.as_bytes())) {
        roots.add(cert?).context("invalid CA certificate")?;
    }
    if roots.is_empty() {
        bail!("no CA certificates supplied");
    }
    let mut tls = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];

    // No Network.bindSocket, bindProcessToNetwork, SO_BINDTODEVICE or VpnService.protect.
    // A normal wildcard-bound socket follows Android's app/VPN route, including Tailscale.
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let config = ClientConfig::builder()
        .with_bind_socket(socket)
        .with_custom_tls(tls)
        .max_idle_timeout(Some(Duration::from_secs(5)))?
        .build();
    let endpoint = Endpoint::client(config)?;
    let result = timeout(Duration::from_millis(timeout_ms), async {
        let options = ConnectOptions::builder(url)
            .add_header("authorization", format!("Bearer {token}"))
            .add_header("x-t3-probe", "native-android")
            .build();
        let connection = endpoint
            .connect(options)
            .await
            .context("WebTransport CONNECT")?;
        report.checks.push("verified-tls-and-authenticated-connect");
        exercise(&connection, report).await?;
        connection.close(VarInt::from_u32(0), b"probe complete");
        connection.closed().await;
        report.checks.push("connection-close");
        Ok(())
    })
    .await
    .context("probe deadline exceeded")
    .and_then(|result| result);

    // Both success and failure tear down the socket and every in-flight stream.
    endpoint.close(VarInt::from_u32(0), b"probe cleanup");
    match timeout(Duration::from_secs(2), endpoint.wait_idle()).await {
        Ok(()) => result,
        Err(_) => {
            drop(endpoint);
            result.context("graceful shutdown deadline elapsed; endpoint dropped")
        }
    }
}

async fn echo(connection: &Connection, stream_number: usize) -> Result<()> {
    let payload: Vec<u8> = (0..PAYLOAD_BYTES)
        .map(|index| ((index + stream_number) % 251) as u8)
        .collect();
    let (mut send, mut recv) = connection.open_bi().await?.await?;
    // Read while writing so this also works with small peer flow-control windows.
    let write = async {
        send.write_all(&[0]).await?;
        send.write_all(&payload).await?;
        send.finish().await?;
        Ok::<_, anyhow::Error>(())
    };
    let read = async {
        let mut received = Vec::with_capacity(payload.len() + 1);
        let mut chunk = [0; 16 * 1024];
        while let Some(count) = recv.read(&mut chunk).await? {
            received.extend_from_slice(&chunk[..count]);
            if received.len() > payload.len() + 1 {
                bail!("echo exceeded expected response length");
            }
        }
        if received.first() != Some(&0) || received[1..] != payload {
            bail!("echo payload mismatch on stream {stream_number}");
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::try_join!(write, read)?;
    Ok(())
}

async fn exercise(connection: &Connection, report: &mut Report) -> Result<()> {
    let mut tasks = Vec::with_capacity(STREAMS);
    for number in 0..STREAMS {
        let connection = connection.clone();
        tasks.push(tokio::spawn(async move { echo(&connection, number).await }));
    }
    for task in tasks {
        task.await??;
    }
    report
        .checks
        .push("four-concurrent-256kib-streams-byte-verified-with-fin");

    let (mut send, mut recv) = connection.open_bi().await?.await?;
    send.write_all(&[1]).await?;
    let mut ready = [0];
    recv.read_exact(&mut ready).await?;
    if ready != [1] {
        bail!("cancel stream was not acknowledged");
    }
    send.reset(application_error(42))?;
    recv.stop(application_error(43));
    echo(connection, 9).await?;
    report
        .checks
        .push("reset-and-stop-sending-preserve-session");

    let (mut send, mut recv) = connection.open_bi().await?.await?;
    send.write_all(&[2]).await?;
    let mut byte = [0];
    if timeout(Duration::from_millis(150), recv.read(&mut byte))
        .await
        .is_ok()
    {
        bail!("stall fixture did not reach its read deadline");
    }
    send.reset(application_error(44))?;
    recv.stop(application_error(45));
    echo(connection, 10).await?;
    report
        .checks
        .push("stalled-stream-deadline-and-session-reuse");
    Ok(())
}

// wtransport exposes raw QUIC error codes. WebTransport application stream errors occupy
// a reserved HTTP/3 range, skipping grease values. Without this mapping quiche reports code 0.
fn application_error(code: u32) -> VarInt {
    let code = u64::from(code);
    VarInt::try_from_u64(0x52e4_a40f_a8db + code + code / 0x1e)
        .expect("a u32 application error fits the reserved HTTP/3 range")
}

/// JNI entrypoint for the standalone probe, deliberately separate from the shipped Expo app.
#[no_mangle]
pub extern "system" fn Java_dev_t3_quicprobe_NativeProbe_run(
    mut env: JNIEnv,
    _class: JClass,
    url: JString,
    roots: JString,
    token: JString,
    timeout_ms: jint,
) -> jstring {
    let read = |env: &mut JNIEnv, value: JString| -> jni::errors::Result<String> {
        Ok(env.get_string(&value)?.into())
    };
    let inputs = (|| -> jni::errors::Result<_> {
        Ok((
            read(&mut env, url)?,
            read(&mut env, roots)?,
            read(&mut env, token)?,
        ))
    })();
    let json = match inputs {
        Ok((url, roots, token)) => {
            let report = run(&url, &roots, &token, timeout_ms.max(0) as u64);
            serde_json::to_string(&report).expect("Report is serializable")
        }
        Err(error) => serde_json::json!({"ok": false, "error": error.to_string()}).to_string(),
    };
    match env.new_string(json) {
        Ok(result) => result.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}
