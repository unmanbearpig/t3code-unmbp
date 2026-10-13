# Development

## First checkout

Install `vp` using the [root README](../../README.md#install-vp). The checkout requires Node 24;
Bun is optional. From the repository root:

```sh
vp i
vp run dev
```

Open the pairing URL printed by the dev runner. The bare origin does not authenticate
a new browser.

Prefer a container? See [Dev container](../internals/devcontainer.md) for VS Code and Codespaces setup.

## Choosing a dev process

Use `vp run dev` for server and web, or `vp run dev:desktop` for the Electron client.
`dev:server` and `dev:web` start those processes separately.
See the [mobile README](../../apps/mobile/README.md) for native builds and Metro.

Flags go directly after the task name, for example `vp run dev --home-dir /tmp/t3code-dev`.
Add `--browser` to open a browser automatically.

### State and ports

Linked worktrees default to their own `.t3/userdata`, even when `T3CODE_HOME` is set.
The main checkout defaults to `~/.t3/dev/userdata`. An explicit `--home-dir` wins in both cases.
Never run a development server against the live `~/.t3/userdata`.
See [test data](../../AGENTS.md#test-data) for copying a consistent database snapshot.

Read ports from the `[dev-runner]` output. Worktrees derive stable preferences from their paths,
but occupied ports can shift them. `T3CODE_PORT_OFFSET` or `T3CODE_DEV_INSTANCE` can select a
different preference when needed.

### Comparing network transports

The Linux network benchmark compares WebSocket/TCP with WebTransport over QUIC/UDP.
Run it after `vp i`, with `unshare`, `ip`, `tc`, `iptables`, `ethtool`, `sysctl`, and
`openssl` available. Rootless user namespaces must be enabled.

```sh
vp run bench:network
```

The versioned `network-v1` suite runs 1,080 cases: nine network profiles, five
workloads, four transport variants in each of two layers, and three repetitions.
It uses netem seed `104729` for every attempt, rotates the transport order between
repetitions, and fixes payload sizes and deadlines. Each case starts a fresh server
and resets network shaping and TCP destination metrics. The complete manifest and
its SHA-256 hash are saved with the results. Workload settings cannot be overridden
in suite mode. `vp run bench:network --dry-run` prints the manifest without running
traffic.

Results default to a new directory below `~/codex-tmp/network-suite`. Record a
baseline, then rerun after a change on the same host:

```sh
vp run bench:network --out ~/codex-tmp/network-before.json
vp run bench:network --baseline ~/codex-tmp/network-before.json \
  --out ~/codex-tmp/network-after.json
```

The comparison rejects mismatched suite settings, host/runtime fingerprints,
incomplete files, missing cases, and duplicate cases. It writes
`network-after.json.comparison.json`. A new completion regression or a timing
regression signal returns exit code 1. Without a baseline, exit code 0 means the
matrix finished running; individual cases may still have error or timeout outcomes.
The runner saves results atomically after every case and records case duration and
total matrix duration so the cost of the suite can be measured before reducing it.

For an individual experiment, `--layer raw` measures the transports with binary
framing and `--layer rpc` includes Effect RPC. `--help` lists the manual-run flags.
Custom scenarios support one-way delay, jitter, random loss, and every-Nth packet
drops or extra delays:

```sh
node apps/server/bench/network/run.ts --layer raw --seed 104729 \
  --scenarios 'slow:delay=150,jitter=15,loss=2%,nthDrop=20' \
  --workloads rpc --transports ws,wt-multi --timeout 30
```

`tc netem` and `iptables` impair actual packets, including TCP retransmissions and
connection handshakes. A private network namespace, MTU 1500, and disabled loopback
offloads keep the experiment separate from other servers. Packet counters cover
both directions together, including acknowledgements. `delay=150` adds about
300ms RTT; `nthDelay=3/300` adds another 300ms to selected packets. Random loss
uses the fixed seed in suite mode. A seed repeats netem's random sequence, but
protocol packetization and scheduling can change which packet consumes a draw;
timings and TCP/QUIC packet traces are not guaranteed to be identical. JSON results
include each repetition, the revision, dependency lock hash, host/runtime, sample
counts, percentiles in milliseconds, and explicit error or timeout outcomes.
The runner keeps samples from timed-out cases. Temporary certificates and lock files live under `~/codex-tmp` and are
removed on exit.

In the RPC comparison, `rpc-ws` enables deflate, `rpc-ws-raw` disables it, `rpc-wt`
uses one ordered QUIC stream, and `rpc-wt-multi` opens a stream per request.
The synthetic text repeats across messages and compresses well; compare the raw
WebSocket variant when measuring transport differences. QUIC streams reliably
deliver bytes despite packet loss. Separate streams let unrelated responses
continue while a lost packet delays another response, while a single stream
still delivers in order.

The measurements answer separate questions:

- `connectAndPing` is time from constructing a new connection until its first echo
  returns. The raw layer also records the handshake and first ping separately.
- `rtt` measures 128 sequential 256-byte echoes after one warmup ping. It includes
  client/server scheduling and, in the RPC layer, serialization and dispatch.
- `latency` measures event creation to client receipt for 128 events of 512 bytes
  generated every 10ms. `stall` measures the gap between receipts beyond that 10ms
  interval. The RPC layer includes acknowledgement backpressure in event latency.
- `fanout` measures the same event latency and stalls with eight simultaneous
  subscriptions, each producing 64 events.
- `bulkMs` and `snapshotMs` measure a warmed connection fetching 4MiB of raw bytes
  or a 2,000-message JSON snapshot. Concurrent 256-byte pings every 20ms measure how
  responsive other requests remain during that transfer. A short transfer may finish
  before enough pings exist to estimate a tail percentile.

All timing units are milliseconds. `p50` describes a typical sample, `p99` the slow
tail, and `n` the number of delivered samples. With few samples, p99 is essentially
an observed maximum, not a precise estimate of the worst 1%. Echo contents, event
sequence and count, and response lengths are checked; incomplete or invalid work
does not qualify as a fast successful case.

Evaluate a change per workload and network profile. Fewer completed repetitions
are a regression regardless of delivered-sample latency. Compare timing only when
both versions complete all repetitions. The report flags changes of at least 10%
and 1ms whose three-run ranges do not overlap; p99 signals also require at least
100 samples in every repetition. A zero baseline uses the 1ms rule. Overlapping
ranges or sparse tails are inconclusive. These are conservative review signals,
not statistical confidence intervals, and need reruns if host load may have changed.
Avoid one aggregate score that hides a faster ping behind a slower snapshot.

For a transport migration, require better interactive tails on lossy and jittery
links while keeping cold connect, ordinary RPC, snapshot time, and completion rates
acceptable on clean and high-latency links. Compare `rpc-ws-raw` with uncompressed
QUIC to isolate transport effects; compare production behavior with matching
compression before deciding to migrate. The synthetic snapshot repeats text heavily.

These are Node client measurements. The multi-stream protocol is experimental;
it does not yet implement T3's heartbeat, authentication, reconnect, or relay
integration. The benchmark does not change the app transport or measure rendering
in web, desktop, or Android. Actual client verification and any production
transport migration need separate work, including native mobile support and
TCP fallback for connections that cannot carry UDP.
The suite does not measure UI rendering, wire bytes, CPU use, battery cost, actual
thread-history HTTP endpoints, or recovery across reconnects and network handoffs.

### Sharing and remote debugging

`vp run dev --share` publishes the web port over the machine's tailnet and prints a pairing URL
for that origin. Give the tester the complete URL, including its token. The dev runner removes
its mapping on exit.

Leave `VITE_HTTP_URL` and `VITE_WS_URL` unset. Vite proxies the backend through the browser's
origin so the same build works over localhost and remote connections.

Shared runs enable bundled dev to avoid a network round trip for each import level.
`T3CODE_BUNDLED_DEV=0` opts out when debugging bundler differences. Two reload traps matter
when changing this setup:

- The web entry must dynamically import the app so React refresh initializes before application
  chunks. Static imports can work on first load and fail after a route split.
- Bundled dev rebuilds Tailwind through watched files. Its ordinary Vite hot-update hook expects
  a server/module graph that Rolldown does not provide.

The workarounds live in the [web entry](../../apps/web/src/bootstrap.ts) and
[Tailwind plugin](../../apps/web/vite/tailwind.ts).

#### Reusable dev credential

Use this only on a hostname where you trust every service. Browsers send cookies to all ports
on that hostname. Any service you visit there can receive the reusable admin credential,
including services unrelated to T3 Code. If you run untrusted services on that hostname, keep
normal per-environment pairing instead.

To use one browser profile across web dev worktrees on the same hostname, generate one fixed
value once:

```sh
openssl rand -hex 32
```

Put that value in the main checkout's gitignored `.env`:

```dotenv
T3CODE_DEV_AUTH_TOKEN=<the value generated above>
```

The `t3.json` Setup Worktree action links that file to each worktree's `.env`. The dev runner reads repository env files at startup. `.env.local` and inherited process
environment values override `.env`, so no per-worktree export is needed after setup.

For a manual worktree or launcher without that link, export the same fixed value instead:

```sh
export T3CODE_DEV_AUTH_TOKEN="<the value generated above>"
```

Do not generate a new value at startup. Start or restart `vp run dev --share` after configuration,
then open its printed startup pairing URL once per browser profile on that hostname. Later web dev
servers on the same hostname accept the shared cookie across ports. The cookie expires after 30
days. Reload an old tab if its URL now serves a replacement environment.

The token and startup pairing URLs are reusable administrative secrets. Never put them in a
commit, pull request, or public output. Every server still seeds its own auth database record at
startup and keeps its own SQLite data, signing key, and revocation state. Desktop and non-dev
servers ignore the value. See [environment authentication](../internals/environment-auth.md#reusable-dev-credential)
for the security model.

## Checks

Run checks for the files and packages you changed:

```sh
vp test run <files>
vp lint <files>
vp run --filter <package> typecheck
```

TypeScript workspaces typecheck with tsc-rs, which has Effect diagnostics built in.
Keep `typescript`, `@effect/tsgo`, and the `effect-tsgo patch` prepare step: editors
still use them for Effect quick fixes and refactors. Keep `@effect/tsgo` on the
effect-tsgo version that tsc-rs ports (see the tsc-rs README), or the two disagree
on rule names.
Marketing keeps `astro check` for Astro files.

Use `vp run lint:mobile` for native mobile changes. CI owns the full suite; see
[ci.yml](../../.github/workflows/ci.yml) for its current jobs.
The [manual Windows lane](../../.github/workflows/windows-tests.yml) is available for focused
Windows investigation while that suite is not a required gate.

### Unused code

`vp run knip:check` checks unused files and dependencies across the repo, then
unused runtime exports in `apps/server`, `apps/desktop`, `apps/web`, and every internal package under
`packages/`. CI enforces both checks.
Exported types and Effect schemas are allowed without consumers. The schema preprocessor
recognizes schema types, including aliases and schema classes; functions that create or decode
schemas remain checked. Canonical Effect service construction APIs stay exported with an explicit
`@public` annotation, which Knip recognizes. Completely unused files remain checked too.
Named exports in web UI component modules are kept as complete component sets. Knip ignores
unused exports in `apps/web/src/components/ui/*.tsx`, while still reporting an entire unused file.
Use `vp run knip --workspace apps/web` to audit one workspace, including exports,
or `vp run knip:production --workspace apps/web` to find code kept alive only by tests.
The full export audit still has findings and is not a repo-wide CI gate. Extend the
export check's workspace selectors as more workspaces become clean. Review callers before
deleting code; production mode can also report development scripts and test fixtures.
Runtime-discovered entrypoints and dependency exceptions belong in [knip.jsonc](../../knip.jsonc).

## Desktop artifacts

Local artifact builds are unsigned by default and write to `release/`:

```sh
vp run dist:desktop:dmg
vp run dist:desktop:linux
vp run dist:desktop:win
```

DMGs default to the host architecture. Use `--arch` to choose another target and `--keep-stage`
to retain packaging files for inspection. Run `vp run dist:desktop:artifact --help` for other
options.

### Linux AppImage prerequisites

Build on Linux because the browser-secret helper links against the host's libsecret. Install
Rust, C/C++ build tools, libsecret development headers, pkg-config, and ImageMagick.

Ubuntu and Debian:

```sh
sudo apt-get update
sudo apt-get install cargo rustc build-essential libsecret-1-dev pkg-config imagemagick
```

Fedora:

```sh
sudo dnf install rust cargo gcc gcc-c++ make libsecret-devel pkgconf-pkg-config ImageMagick
```

Arch Linux:

```sh
sudo pacman -S rust base-devel libsecret pkgconf imagemagick
```

The C toolchain, pkg-config, and libsecret headers are also needed for Linux desktop development.

### macOS DMG prerequisites

Install the Xcode Command Line Tools with `xcode-select --install` and install Rust.
For a cross-architecture or universal build, add the requested Rust targets:

```sh
rustup target add aarch64-apple-darwin x86_64-apple-darwin
```

### Windows installer prerequisites

Install Rust, Python 3, and Visual Studio Build Tools with **Desktop development with C++**.
Include the Windows SDK and the MSVC build tools and Spectre-mitigated libraries for the target
architecture. Add its Rust target:

```powershell
rustup target add x86_64-pc-windows-msvc
# For an ARM64 installer:
rustup target add aarch64-pc-windows-msvc
```

NSIS is downloaded by electron-builder. WSL support additionally needs the Linux CLI archive
passed as `--wsl-runtime`; see the
[release runbook](./release.md#windows-payload-topology-and-update-validation).

### Signing and passkeys

Add `--signed` after configuring the platform credentials in the
[release runbook](./release.md). macOS passkeys need a signed, provisioned app; follow the
[Connect setup](./connect-setup.md#desktop-passkeys) for local signing and renderer HMR.
