# Reconnect benchmark

Run from a clean, committed worktree on Linux with `vp i`, Node, Python 3, `ip`, `tc`, `ethtool`, `unshare`, `flock`, and OpenSSL available. All state is synthetic and in memory. Nothing reads the installed T3 home. Never overlap independent runners. `run.ts` holds `~/codex-tmp/reconnect-benchmark.lock` for its entire run, including every lane.

```sh
node apps/server/bench/network/run.ts --lifecycle --write-manifest ~/codex-tmp/lifecycle.json
node apps/server/bench/network/run.ts --manifest ~/codex-tmp/lifecycle.json --lanes 1 --out ~/codex-tmp/lifecycle-run
node apps/server/bench/network/run.ts --reduced --dry-run
```

`--write-manifest`, `--dry-run`, and `--help` perform no networking. `--smoke` retains the original eleven-profile/four-workload smoke matrix. `--lifecycle` selects the new lifecycle cells on the good network. `--reduced` selects HTTPS/two routes, both spike onset and the 750 ms phase, all outage durations, n=2 for fixed profiles and n=20 for loss profiles. The extended default includes both schemes, one/two routes, and all eight spike phases. Deterministic cells report min/max spread. Observed p90 requires twenty successful observations; failures are reported separately.

The lifecycle manifest uses two repetitions per cell. It includes long cold idle/replay-50, blackholed short-probe/network-changed, warm-closed/warm-half-open/warm-healthy, preparation in flight before active, healthy-slow, refocus without background, loaded-host config delays of 300/1500 ms, and silent/wrong-environment preferred routes with a healthy alternate. Healthy-slow gives the old flow a separate 1200/1800 ms queue in each direction while replacement flows retain the base profile. Warm-healthy is an idle control; closed/half-open warm cells replay fifty events. Refocus sends `application-active-probe` and records its unnecessary work under the current wakeup classification.

A warm case first synchronizes the real shell and visible thread. The simulated 180-second background advances the injected client's wall clock, expiring its authorization cache, while keeping real monotonic deadlines. It explicitly destroys the idle HTTP pool at the simulated long background while retaining the old WebSocket and shell state. It does not sleep, simulate JS suspension, advance kernel clocks, or recreate the OS. Closed sockets are blackholed before fixture writes, then terminated just before active, so missed events cannot leak to the old client. Half-open sockets receive neither FIN nor RST. `pre-active-retry` first synchronizes an old session, closes it after a simulated long background, then waits for the real automatic retry to issue an HTTP preparation request or start its hello socket before publishing active. Negative stage times belong to work before active; attempt numbers preserve its attribution.

Thread completion requires exact message text, order, run membership, event sequence, and visible conversation items. Shell completion requires its synchronized marker and applied state with eight projects, 160 unique threads, the expected title, and latest run. `totalMs` is thread-live; `shellLiveMs` is shell-live. Lifecycle stages include `active`, `attempt-start`, `old-close`, `release-start/end`, and `stream-cancel:<method>-start/end`. Release and cancellation bracket actual Effect scope cleanup. Each row records the old socket state at active and the peak simultaneous client sockets. Healthy controls record false replacements and extra HTTP/WS/RPC requests.

## Processes, lanes, and reset

Each lane has a private user/network/PID namespace and its own loopback/IFB queues. Every lane calibrates every profile in the manifest and the configured reference throughput caps before measurement. Calibration checks numbered echo order, RTT, outage recovery, spike timing, controller deadlines, and throughput. MTU is 1280 and segmentation offloads are disabled. Optional `--cpus '0,1;2,3'` assigns a CPU set to each lane.

A lane lazily keeps one client worker and server process per variant. Only module imports and executable code survive cases. Persistent mode runs two unmeasured cold idle/replay warmups per variant/lane before collecting rows; `--warmups N` changes this count. Warmup rows and their reset evidence are retained in `warmups.json`. Each client uses one new Layer memo map per case, shared by its driver, authorization, and shell/thread services. A worker-lifetime WeakSet rejects reuse of runtime, driver, pool, or cache objects across cases. Every case reconstructs server listener/TLS context, database, auth sessions/signing keys, environment identity, fixtures, client services/runtime/cache, HTTP dispatcher, and WebSockets. The client uses Node's bundled fetch with a fresh Undici 8.3 dispatcher. The D3 normalization still yields one event-loop turn after body consumption before dispatching the next request. It does not share pools across origins.

Before each case, queue replacement removes every scheduled or flow-specific impairment, and TCP metrics are flushed with `tcp_no_metrics_save=1`. Client sockets are terminated after the measured result, the server destroys remaining sockets, and both sides prove zero open sockets before replying. Isolation records include fresh environment/epoch IDs, initial fixture sequence/count, empty shell cache, explicit hello seed, fresh pool, and socket counts. Server `connection` numbers identify reuse within a case; `connectionId` includes the epoch and never carries across cases. A failed case poisons its lane and aborts the run.

Cases use a reproducible shuffled order. All variants of a matched case run on the same lane in balanced forward/reverse rotations. Repetitions rotate between lanes, preventing a variant from being assigned exclusively to a favorable lane. Worktrees and revisions are checked before/after each row. `--fresh-workers` restarts both processes per case for a reference measurement. Keep all inputs and calibration artifacts when comparing it with persistent mode.

Validate concurrency on the actual host before selecting `--lanes`. `reconnectAA.ts` compares identical serial/parallel cells. Its predeclared rule applies to thread and shell separately: deterministic medians within max(2%, 5 ms); stochastic cells need twenty observations, overlapping 95% bootstrap median intervals, and two-sample KS distance no larger than 1.36 sqrt((n+m)/(nm)). Any failed row, missing/mismatched cell, or changed deterministic false-replacement total fails A/A. Do not pool different cells or hide failures.

## Variant interface

```sh
node apps/server/bench/network/run.ts --manifest ~/codex-tmp/cases.json \
  --variants baseline=/absolute/baseline,bc=/absolute/bc \
  --variant-options ~/codex-tmp/variant-options.json --lanes 1 --out ~/codex-tmp/comparison
```

The options file maps names to objects. The original keys remain `websocketHeaders` and `skipDescriptorOnHttpsReconnect`; `helloHandshake` defaults false and `helloSupportCached` defaults true. The full defaults are:

```json
{
  "websocketHeaders": true,
  "skipDescriptorOnHttpsReconnect": false,
  "helloHandshake": false,
  "helloSupportCached": true
}
```

C supplies `WebSocketConstructorWithHeaders` at its existing service key. D supplies `ExperimentalReconnectOptions.skipDescriptorOnHttpsReconnect`. H supplies `ExperimentalReconnectHello.enabled` and an environment/endpoint-keyed `helloSupport` persistence store; its seed is reconstructed per case. The baseline does not advertise C/D/H support. Requested D/H switches fail if unavailable. Every row records the effective switches. A rebased H server uses its production hello-aware shared upgrade route. The fixture advertises hello support only when the H capability exists. Both the hello milestones and the subsequent production config subscription remain required. Actual handshake use and the support hint present at active are recorded per row. Retry ordering and failing-route cells may legitimately construct two sockets; ordinary reconnect cells still require exactly one. These switches do not enable experimental production behavior on baseline.

Production edits only extract the existing Node listener factory and authenticated `/ws` route. The benchmark calls upstream authorization, resolver, driver, supervisor, session, shell/thread state, HTTP snapshots, subscriptions, projections, and replay. Provider execution, discovery/config contents, persistence adapters on the client, and project enrichment use fixtures. HTTP is the direct LAN/tailnet-IP path; HTTPS is a Node TLS 1.3 stand-in for remote termination. Native mobile pooling, WireGuard/DERP, relay/DPoP auth, radios, rendering, and OS suspension need separate validation. No HTTP/3, WebTransport, Caddy, fork marker batching, or other fork production feature is included.
