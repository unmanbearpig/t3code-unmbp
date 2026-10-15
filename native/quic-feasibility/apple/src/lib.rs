use std::ffi::{c_char, CStr};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use tokio::time::timeout;
use wtransport::{
    endpoint::ConnectOptions, tls::Sha256Digest, ClientConfig, Connection, Endpoint, VarInt,
};

const DEADLINE: Duration = Duration::from_secs(15);
const TICKET: &str = "isolated-fixture-ticket";

fn options(url: &str, ticket: &str) -> ConnectOptions {
    ConnectOptions::builder(url)
        .add_header("origin", "https://t3-probe.invalid")
        .add_header("authorization", format!("Bearer {ticket}"))
        .build()
}

fn endpoint(hash: Sha256Digest) -> Result<Endpoint<wtransport::endpoint::endpoint_side::Client>> {
    Ok(Endpoint::client(
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([hash])
            .build(),
    )?)
}

async fn echo(connection: &Connection, payload: Vec<u8>) -> Result<()> {
    let (mut send, mut receive) = connection.open_bi().await?.await?;
    send.write_all(&[0]).await?;
    send.write_all(&payload).await?;
    send.finish().await?;
    let mut output = Vec::new();
    let mut chunk = [0_u8; 16 * 1024];
    while let Some(count) = receive.read(&mut chunk).await? {
        ensure!(
            output.len() + count <= payload.len(),
            "echo exceeded input length"
        );
        output.extend_from_slice(&chunk[..count]);
    }
    ensure!(output == payload, "echo payload mismatch");
    Ok(())
}

async fn suite(url: &str, hash: Sha256Digest) -> Result<()> {
    // A matching pin still enforces certificate dates, ECDSA P-256 and <=14 day validity.
    let bad_endpoint = endpoint(Sha256Digest::new([0; 32]))?;
    let rejection = timeout(DEADLINE, bad_endpoint.connect(options(url, TICKET))).await?;
    ensure!(rejection.is_err(), "incorrect certificate pin was accepted");
    bad_endpoint.wait_idle().await;
    println!("PASS incorrect certificate pin rejected");

    let endpoint = endpoint(hash)?;
    let rejection = timeout(DEADLINE, endpoint.connect(options(url, "wrong-ticket"))).await?;
    ensure!(rejection.is_err(), "incorrect fixture ticket was accepted");
    println!("PASS incorrect authorization rejected");

    let connection = timeout(DEADLINE, endpoint.connect(options(url, TICKET))).await??;
    println!("PASS authenticated HTTP/3 WebTransport session");

    timeout(DEADLINE, async {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..16 {
            let connection = connection.clone();
            tasks.spawn(async move { echo(&connection, vec![index; 64 * 1024]).await });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Result::<()>::Ok(())
    })
    .await??;
    println!("PASS 16 concurrent independent streams, 1 MiB verified");

    let (mut canceled_send, canceled_receive) = connection.open_bi().await?.await?;
    canceled_send.write_all(&[1]).await?;
    canceled_send.reset(VarInt::from_u32(17))?;
    canceled_receive.stop(VarInt::from_u32(18));
    timeout(DEADLINE, echo(&connection, b"after-reset".to_vec())).await??;
    println!("PASS reset/stop stream followed by healthy sibling stream");

    connection.close(VarInt::from_u32(0), b"probe complete");
    timeout(DEADLINE, connection.closed()).await?;
    timeout(DEADLINE, endpoint.wait_idle()).await?;
    println!("PASS local close and endpoint drain");
    Ok(())
}

/// Run the isolated interop suite. This blocking probe belongs on a worker thread.
pub fn run(url: &str, hash: &str) -> Result<()> {
    let hash = hash
        .parse::<Sha256Digest>()
        .context("invalid certificate SHA-256")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async { timeout(Duration::from_secs(60), suite(url, hash)).await })??;
    Ok(())
}

/// C ABI build/link probe. The caller owns both NUL-terminated UTF-8 strings for the call.
/// Returns 0 on success, 1 on probe failure, and 2 on invalid input. Does not unwind into Swift.
///
/// # Safety
/// Both pointers must reference live NUL-terminated strings or be null.
#[no_mangle]
pub unsafe extern "C" fn t3_apple_webtransport_probe(
    url: *const c_char,
    hash: *const c_char,
) -> i32 {
    if url.is_null() || hash.is_null() {
        return 2;
    }
    let Ok(url) = CStr::from_ptr(url).to_str() else {
        return 2;
    };
    let Ok(hash) = CStr::from_ptr(hash).to_str() else {
        return 2;
    };
    match std::panic::catch_unwind(|| run(url, hash)) {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            eprintln!("probe failed: {error:#}");
            1
        }
        Err(_) => 1,
    }
}
