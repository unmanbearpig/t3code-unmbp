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

The Linux network benchmark measures thread operations using the production SQL
projection, HTTP snapshot routes, persisted replay, live streams, and shared web/mobile
thread state. It compares WebSocket/TCP with experimental WebTransport over QUIC/UDP.
Run it after `vp i`, with `unshare`, `ip`, `tc`, `iptables`, `ethtool`, `sysctl`,
`openssl`, `python3`, and an HTTP/3-enabled libcurl development installation available. Rootless
user namespaces must be enabled. Build the benchmark's native client against that
libcurl if its downloaded prebuild lacks HTTP/3:

```sh
npm_config_build_from_source=true npm_config_curl_config_bin=/usr/bin/curl-config \
  vp rebuild node-libcurl
```

The first run downloads checksum-verified Caddy 2.11.7 into
`~/codex-tmp/network-bench-tools`; later runs reuse it. Other Linux architectures
can supply that version through `NETBENCH_CADDY`.

```sh
vp run bench:network
vp run bench:network --dry-run
```

The fixed `operations-v4` suite runs 648 cases: six operations, nine network profiles,
four RPC transport variants, and three repetitions. Messages contain 2,000 characters
of deterministic, distinct text. Each user/assistant pair has a completed run and
visible conversation items. The suite uses netem seed `104729`, rotates transport order,
sets a 60-second deadline per case including setup, and starts a fresh server with an
in-memory database. Setup and teardown contribute to suite duration but are excluded
from the operation timing. Workload settings cannot be overridden in suite mode.
The complete manifest and its SHA-256 hash are saved with results.

| Operation            | Starting state and action                                                                                | Timing ends when                                                                                      |
| -------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `sync-five`          | Live subscription with all 100 messages loaded; persist five additional messages                         | Shared client state has all 105 messages and their conversation items                                 |
| `open-new`           | Ready session, uncached 100-message thread; load its bounded HTTP snapshot and subscribe from its cursor | Latest history window is correct and the server completion marker has made state live                 |
| `open-long`          | Same operation with 2,000 messages in server history                                                     | Same completion condition; current policy loads the latest 20 messages                                |
| `reconnect-idle`     | Offline client with 100 cached messages; make no changes; create a new session                           | Client receives the completion marker and retains the same 100 messages                               |
| `reconnect-replay`   | Offline client with 100 cached messages; add 50 messages; create a new session                           | All 150 messages and 125 missed domain events have been applied and state is live                     |
| `reconnect-snapshot` | Offline client with 100 cached messages; add 200 messages, exceeding replay limits                       | Client replaces its state with a correct bounded snapshot of the latest conversation and becomes live |

`operationMs` is milliseconds from starting the operation to the completion above.
Incremental sync starts immediately before the server persists the batch, so it includes
SQL commit, publication, transfer, decoding, and client application. It excludes IPC
fixture triggering. Uncached opens start after the session has received configuration;
connection setup is recorded separately as `connectionReadyMs`. Reconnect timing starts
when opening the replacement transport, after the simulated offline activity has
committed. It includes the handshake and configuration subscription, but excludes outage
detection and retry backoff.

`linkTxBytes` and `linkTxPackets` are loopback transmission counter deltas over the
operation interval. They include both directions, HTTP, RPC framing, TCP/UDP/QUIC
headers, acknowledgements, and retransmissions that reach the interface. Packets dropped
after transmission are included in these counters. Reads stop at client
completion, so late acknowledgements or connection-close traffic can fall outside the
interval. These are transfer-cost measurements, not exact application payload sizes.
`events`, `socketSnapshots`, `httpSnapshots`, `syncMode`, `loadedMessages`, and
`totalMessages` explain the path taken. Incremental sync and reconnect start with a full 100-message memory cache copied
from the isolated projection. Reconnect cases verify that cache while offline and open
only the measured connection, so a previous socket cannot send teardown traffic or
receive the offline writes. They do not refetch that history during setup. The corrected
completion and reconnect boundaries, HTTP/3 snapshots, and packet shaping after UDP
segmentation require a new baseline. Earlier operation suites cannot be compared with
this suite.

The runner checks message IDs, ordering, contents, roles, run membership, visible
conversation items, history metadata, event sequences, subscription cursors, expected
replay/snapshot behavior, and completion markers. Missing or corrupted state produces an
error outcome even if the measured time looks fast. A bounded window is checked against
the real server projection and must include the newest message. It is not treated as a
fully downloaded long thread.

Results default to a new directory below `~/codex-tmp/network-suite`. Record a baseline,
then rerun the same suite after a change on the same host:

```sh
vp run bench:network --out ~/codex-tmp/network-before.json
vp run bench:network --baseline ~/codex-tmp/network-before.json \
  --out ~/codex-tmp/network-after.json
```

The comparison rejects different suite versions, settings, host/runtime fingerprints,
incomplete files, missing cases, and duplicate cases. It writes a `.comparison.json`
file. Fewer successful repetitions are a regression. Timing and transfer cost are compared
only when both revisions finish all repetitions and deliver the same message counts.
Changed delivered state requires review and cannot qualify as a speedup. A change of at least 10% and one unit
whose three-run ranges do not overlap gets a review signal. Overlapping ranges are
inconclusive; these checks are not statistical confidence intervals. Three operation
repetitions do not estimate p99. Compare each operation and profile separately. A smaller
transfer or faster ping does not compensate for slower catch-up or failed sync.

A completion regression or timing/transfer regression signal returns exit code 1. Without
a baseline, exit code 0 means the matrix finished; inspect individual error/timeout
outcomes. Results are saved atomically after every case, with case and suite durations.

The nine profiles cover clean traffic, WAN delay, high latency, mobile jitter/loss,
2% and 5% random loss, burst loss, every-third-packet delay, and every-twentieth-packet
drop. The operation suite applies `tc` ingress actions and `netem` through an IFB after
segmentation, so batching by QUIC stacks cannot turn one loss decision into several
lost datagrams. This includes TCP retransmissions and handshakes. It runs in a private
network namespace with MTU 1500 and disabled TCP/UDP offloads. A preflight sends ten
datagrams in one UDP segmentation batch and checks that exactly datagrams 3, 6, and 9
are dropped or delayed.
Counters for periodic impairment cover both directions together, including acknowledgements.
A 150ms one-way delay adds about 300ms RTT. The every-third-packet profile adds another
300ms to selected packets. A fixed seed repeats netem's random sequence, but scheduling
and packetization can change which packet consumes each draw.

`rpc-ws` uses production deflate, `rpc-ws-raw` disables compression, `rpc-wt` uses one
ordered QUIC stream, and `rpc-wt-multi` opens a QUIC stream per request. QUIC reliably
delivers data over UDP. Compare uncompressed WebSocket and QUIC to isolate transport
cost, and include compression when judging the app's current behavior. Uncached thread
snapshots use HTTPS/HTTP/1.1 for the WebSocket variants and HTTP/3 for WebTransport.
Both use libcurl, the same fixture certificate, Caddy, and the production gzip route
over a Unix socket, so the proxy introduces no additional counted TCP hop. The client
forces the protocol and verifies it at both ends; HTTP/3 never silently falls back.
Each case owns its connection pool. HTTP handshake, first-byte, and total request
times are saved alongside the complete operation time.
Operation framing and compression follow the transport implementation under test;
`rpcCompression` records the mode. This lets the same workload measure implementation
improvements while continuing to verify the complete delivered state.

The HTTP/3 snapshot and WebTransport RPC use separate QUIC connections. This suite
measures that implementation, not a browser sharing one QUIC connection. It remains
an experimental benchmark path; production clients still use their existing transports.
For a protocol-only snapshot comparison, keep the RPC transport unchanged and rerun
a manual experiment with `--http-protocol h1` and `--http-protocol h3`.

For a targeted experiment, call the runner directly. Manual flags do not change the fixed
suite:

```sh
node apps/server/bench/network/run.ts --layer operations --seed 104729 \
  --scenarios 'slow:delay=150,jitter=15,loss=2%,nthDrop=20' \
  --workloads sync-five,reconnect-replay --transports rpc-ws,rpc-wt-multi --timeout 60
```

The original 1,080-case `network-v1` transport suite and its existing baselines remain
available through `vp run bench:network:transport`. Its five synthetic workloads measure
cold connect/echo, 128 sequential echoes, one streamed subscription, eight subscriptions,
and a 4MiB raw response or 2,000-message RPC snapshot with concurrent pings. Use it to
investigate transport behavior separately from the application operation suite.

These are Node measurements through the shared client state code. They do not measure
browser/Android rendering, disk cache writes, CPU/battery use, authentication, environment
or shell synchronization, automatic reconnect scheduling, relay/tunnel operation, or
network handoffs. Provider execution and authorization use isolated fixtures. Production
clients still use WebSocket. The QUIC adapters remain experimental and require native
mobile support, authentication, heartbeat/reconnect integration, relay support, and TCP
fallback before a production migration. Temporary certificates and namespace lock files
live below `~/codex-tmp` and are removed on exit.

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
