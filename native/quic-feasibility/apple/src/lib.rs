use std::ffi::{c_char, CStr};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use tokio::time::timeout;
use wtransport::{
    endpoint::ConnectOptions, tls::Sha256Digest, ClientConfig, Connection, Endpoint, VarInt,
};

const DEADLINE: Duration = Duration::from_secs(15);
const TICKET: &str = "isolated-fixture-ticket";

// wtransport 0.7.2 forwards raw QUIC codes. WebTransport reserves this HTTP/3
// range and skips HTTP/3's reserved values. See draft-ietf-webtrans-http3 section 4.4.
fn stream_error_code(application_code: u32) -> VarInt {
    let code = u64::from(application_code);
    VarInt::try_from_u64(0x52e4_a40f_a8db + code + code / 0x1e)
        .expect("all WebTransport u32 application error codes fit a QUIC varint")
}

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

    timeout(DEADLINE, async {
        let (mut canceled_send, mut canceled_receive) = connection.open_bi().await?.await?;
        canceled_send.write_all(&[1]).await?;
        let mut ready = [0];
        // Wait for the peer to identify the probe stream before discarding its send data.
        canceled_receive.read_exact(&mut ready).await?;
        ensure!(ready == [1], "peer did not acknowledge cancellation probe");
        canceled_send.reset(stream_error_code(17))?;
        canceled_receive.stop(stream_error_code(18));

        let (mut status_send, mut status_receive) = connection.open_bi().await?.await?;
        status_send.write_all(&[2]).await?;
        status_send.finish().await?;
        let expected = b"reset=17;stop=18";
        let mut status = [0; 16];
        status_receive.read_exact(&mut status).await?;
        ensure!(
            &status == expected,
            "peer cancellation codes differ: {status:?}"
        );
        ensure!(
            status_receive.read(&mut [0]).await?.is_none(),
            "unexpected cancellation result bytes"
        );
        Result::<()>::Ok(())
    })
    .await??;
    timeout(DEADLINE, echo(&connection, b"after-reset".to_vec())).await??;
    println!("PASS peer observed RESET_STREAM=17 and STOP_SENDING=18; sibling stream healthy");

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

#[cfg(test)]
mod tests {
    use super::stream_error_code;

    #[test]
    fn stream_errors_match_specification_boundaries() {
        assert_eq!(stream_error_code(0).into_inner(), 0x52e4_a40f_a8db);
        assert_eq!(stream_error_code(29).into_inner(), 0x52e4_a40f_a8f8);
        // The next HTTP/3 value is reserved and must be skipped.
        assert_eq!(stream_error_code(30).into_inner(), 0x52e4_a40f_a8fa);
        assert_eq!(stream_error_code(u32::MAX).into_inner(), 0x52e5_ac98_3162);
    }
}
