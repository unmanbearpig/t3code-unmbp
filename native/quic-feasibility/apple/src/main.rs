fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(
        args.len() == 2,
        "usage: t3-apple-webtransport-probe <url> <certificate-sha256>"
    );
    t3_apple_webtransport_probe::run(&args[0], &args[1])
}
