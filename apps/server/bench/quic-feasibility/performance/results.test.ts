import { describe, expect, it } from "vite-plus/test";

import { distribution, summarize, type Results } from "./results.ts";

const row = (
  transport: string,
  seed: number,
  operationMs: number,
  extra: Record<string, string | number> = {},
): Results["rows"][number] => ({
  layer: "operations",
  scenario: "loss2",
  workload: "sync-five",
  transport,
  seed,
  repetition: 1,
  timeoutMs: 25_000,
  metrics: {
    phase: "complete",
    operationMs,
    rpcTls: "TLSv1.3",
    rpcCompression:
      transport === "rpc-wss" ? "permessage-deflate" : "deflate-dictionary-frame-v2-level3",
    httpSnapshots: 0,
    linkTxBytes: 1000,
    linkTxPackets: 10,
    ...extra,
  },
});

describe("paired performance evidence", () => {
  it("compares matching seeds and reports absolute as well as relative differences", () => {
    const summary = summarize({
      complete: true,
      rows: [
        row("rpc-wss", 7, 100),
        row("rpc-wss", 9, 300),
        row("rpc-wt", 9, 150),
        row("rpc-wt", 7, 90),
      ],
    });
    const quic = summary.comparisons.find((group) => group.id.endsWith("/rpc-wt"))!;
    expect(quic.operationMs).toEqual({ n: 2, min: 90, median: 120, max: 150 });
    expect(quic.pairedAgainstDirectWss.deltaMs).toEqual({ n: 2, min: -150, median: -80, max: -10 });
    expect(quic.pairedAgainstDirectWss.deltaPercent?.median).toBeCloseTo(-30);
  });

  it("keeps failed correctness checks and unmatched runs out of paired speedups", () => {
    const summary = summarize({
      complete: false,
      rows: [
        row("rpc-wss", 7, 100),
        row("rpc-wt", 7, 1, { outcome: "error: missing event" }),
        row("rpc-wt", 9, 20),
      ],
    });
    const quic = summary.comparisons.find((group) => group.id.endsWith("/rpc-wt"))!;
    expect(summary).toMatchObject({ complete: false, attempted: 3, completed: 2 });
    expect(quic).toMatchObject({
      attempted: 2,
      completed: 1,
      pairedAgainstDirectWss: { n: 0, deltaMs: null },
    });
    expect(quic.failures).toEqual([{ seed: 7, outcome: "error: missing event" }]);
  });

  it("rejects snapshots measured with a different HTTP transport or encoding", () => {
    const snapshot = {
      ...row("rpc-wt", 7, 100, { httpSnapshots: 1, httpProtocol: "h3", httpEncoding: "gzip" }),
      workload: "open-long",
    };
    expect(() => summarize({ complete: true, rows: [snapshot] })).toThrow("HTTP snapshot confound");
    expect(() =>
      summarize({
        complete: true,
        rows: [
          { ...snapshot, metrics: { ...snapshot.metrics, httpProtocol: "h1", httpEncoding: "br" } },
        ],
      }),
    ).toThrow("HTTP snapshot confound");
  });

  it("rejects duplicate cases and unencrypted successful controls", () => {
    const control = row("rpc-wss", 7, 100);
    expect(() => summarize({ complete: true, rows: [control, control] })).toThrow("Duplicate case");
    expect(() =>
      summarize({ complete: true, rows: [row("rpc-wss", 7, 100, { rpcTls: "none" })] }),
    ).toThrow("TLS control");
  });

  it("reports observed min/median/max and sample count without a tail estimate", () => {
    expect(distribution([])).toBeNull();
    expect(distribution([8, 1, 4])).toEqual({ n: 3, min: 1, median: 4, max: 8 });
  });
});
