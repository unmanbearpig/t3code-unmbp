import * as Schema from "effect/Schema";

import { BenchRow } from "../../network/comparison.ts";

export const transports = ["rpc-wss", "rpc-wt", "rpc-wt-multi"] as const;
export const workloads = ["sync-five", "reconnect-replay", "open-long"] as const;
export const scenarios = ["clean", "wan", "loss2"] as const;
export const seeds = [731, 947, 1163, 1381, 1597] as const;

export const Results = Schema.Struct({ complete: Schema.Boolean, rows: Schema.Array(BenchRow) });
export type Results = typeof Results.Type;
type Row = BenchRow;

export function distribution(values: readonly number[]) {
  if (values.length === 0) return null;
  const ordered = values.toSorted((a, b) => a - b);
  return {
    n: ordered.length,
    min: ordered[0]!,
    median:
      (ordered[Math.floor((ordered.length - 1) / 2)]! + ordered[Math.floor(ordered.length / 2)]!) /
      2,
    max: ordered.at(-1)!,
  };
}

const number = (row: Row, key: string) => {
  const value = row.metrics[key];
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
    throw new Error(
      `Missing or invalid ${key} in ${row.scenario}/${row.workload}/${row.transport}`,
    );
  return value;
};

/** A timeout or a failed correctness check never enters the successful latency sample. */
export function summarize(result: Results) {
  const rows = new Map<string, Row>();
  const key = (row: Row) => [row.scenario, row.workload, row.transport, row.seed].join("/");
  for (const row of result.rows) {
    if (rows.has(key(row))) throw new Error(`Duplicate case ${key(row)}`);
    rows.set(key(row), row);
    if (row.metrics.outcome !== undefined) continue;
    if (
      row.layer !== "operations" ||
      row.metrics.phase !== "complete" ||
      row.metrics.rpcTls !== "TLSv1.3" ||
      !transports.includes(row.transport as (typeof transports)[number])
    )
      throw new Error(`Wrong operation, completion, or TLS control in ${key(row)}`);
    if (
      row.metrics.rpcCompression !==
      (row.transport === "rpc-wss" ? "permessage-deflate" : "deflate-dictionary-frame-v2-level3")
    )
      throw new Error(`Wrong compression control in ${key(row)}`);
    if (row.workload === "open-long") {
      if (
        row.metrics.httpProtocol !== "h1" ||
        row.metrics.httpEncoding !== "gzip" ||
        row.metrics.httpSnapshots !== 1
      )
        throw new Error(`HTTP snapshot confound in ${key(row)}`);
    } else if (row.metrics.httpSnapshots !== 0) {
      throw new Error(`Unexpected HTTP snapshot in ${key(row)}`);
    }
    for (const metric of ["operationMs", "linkTxBytes", "linkTxPackets"]) number(row, metric);
  }
  const groups = new Map<string, Row[]>();
  for (const row of result.rows) {
    const id = [row.scenario, row.workload, row.transport].join("/");
    const group = groups.get(id) ?? [];
    group.push(row);
    groups.set(id, group);
  }
  const comparisons = Array.from(groups, ([id, group]) => {
    const successful = group.filter((row) => row.metrics.outcome === undefined);
    const paired = successful.flatMap((row) => {
      const control = rows.get(key({ ...row, transport: "rpc-wss" }));
      if (!control || control.metrics.outcome !== undefined) return [];
      return [{ row, control }];
    });
    return {
      id,
      attempted: group.length,
      completed: successful.length,
      failures: group
        .filter((row) => row.metrics.outcome !== undefined)
        .map((row) => ({ seed: row.seed, outcome: row.metrics.outcome })),
      operationMs: distribution(successful.map((row) => number(row, "operationMs"))),
      linkTxBytes: distribution(successful.map((row) => number(row, "linkTxBytes"))),
      linkTxPackets: distribution(successful.map((row) => number(row, "linkTxPackets"))),
      pairedAgainstDirectWss: {
        n: paired.length,
        deltaMs: distribution(
          paired.map(
            ({ row, control }) => number(row, "operationMs") - number(control, "operationMs"),
          ),
        ),
        deltaPercent: distribution(
          paired.flatMap(({ row, control }) => {
            const baseline = number(control, "operationMs");
            return baseline > 0 ? [100 * (number(row, "operationMs") / baseline - 1)] : [];
          }),
        ),
      },
    };
  });
  return {
    complete: result.complete,
    attempted: result.rows.length,
    completed: result.rows.filter((row) => row.metrics.outcome === undefined).length,
    comparisons,
  };
}
