# Catch-up completion benchmark results

Raw results behind the benchmark table in the upstream PR that sends the `synchronized` marker in
the same RPC chunk as the catch-up data.

The harness in `apps/server/bench/network` runs the real server listener, `/ws` route, auth and
client-runtime reconnect code. Each lane runs in its own Linux network namespace, and `tc netem`
adds the delay, jitter and loss for each network profile. `../../README.md` lists the host tools it
needs. The client is Node, not a phone.

## Revisions

Both revisions sit on upstream `42da166d13` and carry the same harness commits.

| Variant | Branch | Commit |
|---|---|---|
| `baseline` | `perf/reconnect-confirm-baseline` | `6ba3b2f34c` |
| `mi` | `perf/reconnect-confirm-mi` | `2f280fdc93` |

`git diff 6ba3b2f34c 2f280fdc93` is the PR's diff. The only difference is one doc comment on
`rpcInitialItems`, which the PR drops. Both ran with every client option in `variant-options.json` off.

## Runs

`run/` has 284 rows across the network profiles and loss cases. `run-loss-ext/` adds 60 more
seeds of the 3% loss, 50 new events case. The PR's 80-seed numbers pool both runs.

Cases run in a shuffled order, and both variants of a case run on the same seed, alternating
which goes first. `rows.json` has one row per variant and case. `totalMs` is the time until the visible thread is current, and `shellLiveMs`
is the time until the thread list is.

Both runs used this command, with `run/` and `run-loss-ext/` each passing its own manifest:

```sh
node apps/server/bench/network/run.ts --manifest run/manifest.json \
  --variants baseline=/abs/baseline-worktree,mi=/abs/mi-worktree \
  --variant-options variant-options.json --lanes 1 --out run
```

The host was an Intel Core Ultra 5 135H on Linux 6.18 with Node 26.3, running one lane.
`environment.json` records the rest.
