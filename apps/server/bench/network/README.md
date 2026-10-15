# Reconnect hello prototype

This branch compares H with B+C. The config gate remains: the driver publishes a
session only after the authenticated config arrives. Benchmarks must run serially
under the existing machine lock.

Pass a JSON file to `--variant-options` with entries matching `--variants` names:

```json
{
  "bc": {
    "websocketHeaders": true,
    "skipDescriptorOnHttpsReconnect": false,
    "helloHandshake": false
  },
  "h": { "websocketHeaders": true, "skipDescriptorOnHttpsReconnect": false, "helloHandshake": true }
}
```

Use distinct clean worktrees containing this implementation for the two variants.
No source edits are needed. H seeds the per-environment, HTTP endpoint and WS
endpoint support cache with the fixture's prior public descriptor. It performs no
warm-up HTTP request. Actual clients learn support on a legacy connection; this
prototype keeps support in memory for the authorization service's lifetime.

Production remains off by default. Advertising support requires
`T3CODE_EXPERIMENTAL_RECONNECT_HELLO=true`; selecting H also requires the optional
`ClientCapabilities.ExperimentalReconnectHello` layer with `enabled: true`.
The bench server advertises support for both comparison variants.

H upgrades `/ws?handshake=hello-v1`, sends public Hello immediately, validates the
client's single BearerAuth frame, sends Authenticated, then adopts the same socket
reader for the existing RPC protocol. Clients verify identity and orchestration
compatibility before sending the bearer, release queued RPC writes as soon as auth
is sent, and consume control frames before delivering RPC data. Resume flags share
a schema with ServerConfig but do not enable early subscriptions.

For a cached thread on one cold HTTPS route, B+C needs one descriptor GET, no
ticket POST, one upgrade, config subscription and thread subscription. H removes
the descriptor GET and its HTTP connection: no HTTP preparation requests and one
upgrade. The analytical prediction is 8 to 5 RTTs to thread live, about 900 ms at
300 ms RTT. This is unmeasured. Shell still fetches its authoritative HTTP snapshot
and retains its existing subscription response flights, predicted 10 to 9 RTTs.
Full thread-and-shell runs therefore still use an HTTP connection alongside WS.

Results include tag-only `hello-received`, `auth-sent`, `authenticated`, `config`,
`subscribe`, `synchronized` and `applied` milestones, HTTP path counts, WS opens,
and distinct observed server connection IDs. No credential frames are stored.

## Security constraints

- Frame auth has no request/cookie input. It uses the existing session verifier,
  scope derivation, revocation, expiry and MCP-audience rejection, and only accepts
  unbound bearer access tokens. Cookie sessions and DPoP/T3 Connect use legacy.
- Hello reveals only the existing public descriptor and resume flags. Full config
  and RPC setup remain behind authentication. Decode diagnostics discard auth
  payloads and arbitrary causes; error frames carry a fixed enum only.
- The deadline is five seconds. The first text frame is limited to 8 KiB, including
  JSON overhead. Buffered pipelined frames are limited to 16 frames and 64 KiB.
  Limits apply during async credential verification too. The Node adapter checks
  message bursts before Effect's read queue and caps fragmented messages at 64 KiB.
  Transport messages above that hard cap close with WS 1009; smaller admission
  failures send a typed HandshakeError and close with 1008.
- H disables permessage-deflate in both directions. The bearer never enters a
  compression context. Legacy compression and limits are unchanged. H retains
  the 64 KiB inbound message cap after authentication, which can reject large RPC
  commands; remove that prototype restriction before general use. Disabled H
  compression can also affect replay throughput, so interpret benchmark deltas
  with that difference in mind.
- Missing/invalid Hello or a pre-H transport failure permits one legacy discovery
  retry after closing the old socket. Explicit identity/protocol/auth rejection
  never permits fallback. Update sessions always force legacy discovery.

Before production: add process-wide/per-source unauthenticated admission and
upgrade-rate limits, negotiate compression without token context takeover, design
framed DPoP proofs, revisit the post-auth payload cap, persist support if cold app
launches should use H, and verify real Node TLS, browsers, React Native and relay
paths. This task runs only deterministic in-memory tests and scoped static checks;
it does not launch a server, browser, device, or benchmark.
