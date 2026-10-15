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

### Benchmarking mobile reconnects

`vp run bench:network` measures a long mobile resume through the production shared
client supervisor, route driver, resolver, bearer authorization, RPC session, and
thread state. It starts from a verified 100-message offline cache with no socket.
`idle` has no missed events; `replay-50` applies exactly 50 missed domain events,
representing 20 more fixture messages. Timing ends when the completion marker has
arrived and the correct thread state is live and applied.

Install with `vp i`. Linux needs rootless user/network/PID namespaces, `flock`,
`ip`, `tc`, IFB support, `ethtool`, `sysctl`, `openssl`, `stdbuf`, and `python3`.
No dev server or real T3 home is used. One benchmark holds the process lock at a
time. Impairments remain inside a private namespace and refuse the host namespace.
Scratch files live below `~/codex-tmp` and are removed on exit; results remain.

```sh
# Show the full matrix without sending packets.
vp run bench:network --dry-run
# Run calibration only, then a small baseline matrix. Each invocation calibrates.
vp run bench:network --calibrate-only --out ~/codex-tmp/reconnect-calibration
vp run bench:network --smoke --out ~/codex-tmp/reconnect-smoke
# Freeze one manifest and compare clean worktrees in interleaved order.
node apps/server/bench/network/run.ts --write-manifest ~/codex-tmp/reconnect-manifest.json
node apps/server/bench/network/run.ts --manifest ~/codex-tmp/reconnect-manifest.json \
  --variants baseline=/absolute/baseline,descriptor-once=/absolute/variant \
  --out ~/codex-tmp/reconnect-comparison
```

Each worktree must contain this benchmark and be committed and clean. The runner
checks revision and dirty state before and after every case. It runs one case per
variant before moving to the next case, rotating forward and reverse variant order.
Every row records variant, revision, dirty flag, manifest hash, network inputs,
request attempt counts, event counts, socket count, IP bytes/packets, and timestamps.
HTTP stages retain success/failure outcomes, including descriptor checks canceled
by the route deadline during an outage.
`rows.json` retains failures; `summary.json` separates profile, workload, HTTP/TLS,
and saved-route count. It reports counts, median total time, per-stage completion
and duration medians, and nearest-rank observed p90 only with 20 successful samples.
These are observations, not confidence intervals. Compare failure counts before timings.

The full matrix has 960 cases per variant. Both HTTP and verified TLS 1.3 use one
Node listener and one TCP port, with the production response-write guard and
permessage-deflate settings. TLS is a stand-in for tailscaled's TLS terminator.
The measured path includes the real descriptor and ticket HTTP handlers,
in-memory bearer pairing/session verification, authenticated production WebSocket
upgrade, production SQL projection/replay, and shared thread-state application.
Unrelated providers, configuration discovery, analytics, and platform persistence
are fixed fixtures. The old proxy-backed operation runner is retired. No measured
request passes through a proxy, Unix socket, or forwarding hop.

The matrix crosses two workloads, one/two saved direct routes, and HTTP/TLS with:

| Profile                      | One-way settings                                    | Repetitions           |
| ---------------------------- | --------------------------------------------------- | --------------------- |
| good                         | 10 ms                                               | 4                     |
| cellular                     | 35 ms, 7 ms in-order jitter                         | 4                     |
| high-latency                 | 150 ms                                              | 4                     |
| latency-spikes               | Cellular, delay rises to 1 s for 500 ms             | 4 at each of 8 phases |
| loss-1 / loss-3              | Cellular, seeded 1% / 3% loss                       | 20 seeds each         |
| burst-loss                   | Cellular, seeded Gilbert-Elliott loss               | 20 seeds              |
| slow-start-500 / 1500 / 3000 | Drop all packets for 0.5 / 1.5 / 3 s, then cellular | 4 each                |
| constrained                  | Cellular, 1 Mbit/s download, 512 kbit/s upload      | 4                     |

The smoke matrix has 25 cases, one HTTPS/two-route idle and replay case per profile
and outage duration, plus good-network HTTP/one-route, HTTP/two-route, and TLS/one-route controls. The full matrix is estimated at about 1.5 hours per variant,
plus roughly two minutes of calibration per invocation. That is a planning estimate;
loss/retry behavior and process startup change it. The conservative 60-second case
plus 30-second setup deadline gives a much larger failure bound. For six variants,
plan roughly nine hours. Use the smoke matrix for integration checks first.

MTU is 1280 and offloads are disabled. Loopback ingress redirects to IFB after
segmentation. A two-band priority qdisc separates source-port-matched downloads
from uploads; both have independent netem rate/queue settings and a pfifo child
that preserves enqueue order through jitter and spike release. Queues allow
100,000 packets. The ordinary rate ceiling is 100 Mbit/s in each direction.
A separate Python controller uses monotonic deadlines and one long-lived
`tc -batch -` process. It records submission and netlink-acknowledged application
times. Outages use a priority-1 matchall drop ahead of the IFB redirect.

Calibration runs once before any measured case. Numbered UDP echoes check base
RTT, ordering, recovery, spikes, and outages. TCP bulk checks every distinct configured
rate in each direction. Controller application must stay within 30 ms of its schedule. `calibration.json` preserves samples, controller timing, and configuration;
a failed calibration prevents the matrix from running. A fixed seed fixes netem
random draws, but variants can consume draws with different packetization.

These measurements use Node ws, Node fetch/undici connection pooling, and the
host Linux TCP stack, including its SYN retry timers and initial window. They do not run WireGuard or DERP, reproduce radio wake or
phone suspension, or measure rendering. Client, server, and controller share host
CPU. A real-device resume log is still needed to validate the reconnect change.

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
