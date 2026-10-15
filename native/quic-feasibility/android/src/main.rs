use std::env;
use std::fs;
use t3_quic_probe::run;

fn main() -> anyhow::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        anyhow::bail!("Usage: quic-probe <https-url> <root-ca.pem> <timeout-ms>; token in T3_QUIC_PROBE_TOKEN");
    }
    let report = run(
        &args[1],
        &fs::read_to_string(&args[2])?,
        &env::var("T3_QUIC_PROBE_TOKEN")?,
        args[3].parse()?,
    );
    println!("{}", serde_json::to_string(&report)?);
    if !report.ok {
        std::process::exit(1);
    }
    Ok(())
}
