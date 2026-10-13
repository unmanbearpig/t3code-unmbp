import { assert, it } from "@effect/vitest";

import { compareSuites, type SuiteResults, validateSuiteResults } from "./comparison.ts";
import { presets } from "./impairments.ts";
import { rpcWorkloads } from "./rpcWorkloads.ts";
import { workloadSettings } from "./settings.ts";
import { caseId, makeSuitePlan, suiteDefinition } from "./suite.ts";
import { Recorder, workloads } from "./workloads.ts";

function results(value = 100): SuiteResults {
  const plan = makeSuitePlan();
  return {
    suite: { hash: plan.hash },
    complete: true,
    metadata: { revision: "test", environmentHash: "same-host" },
    rows: plan.cases.map((entry) => ({
      ...entry,
      scenario: entry.scenario.name,
      seed: entry.seed!,
      metrics: { "rtt.p50": value, "rtt.p99": value, "rtt.n": workloadSettings.rpc.count },
    })),
  };
}

it("schedules every registered workload, profile, and transport with three reproducible attempts", () => {
  const plan = makeSuitePlan();
  assert.deepEqual(plan, makeSuitePlan());
  assert.deepEqual(
    workloads.map((w) => w.name),
    [...plan.workloads],
  );
  assert.deepEqual(
    rpcWorkloads.map((w) => w.name),
    [...plan.workloads],
  );
  assert.deepEqual(plan.scenarios, presets);
  assert.equal(plan.cases.length, 1080);
  assert.equal(new Set(plan.cases.map(caseId)).size, 1080);
  for (const layer of suiteDefinition.layers) {
    for (const scenario of presets) {
      for (const workload of plan.workloads) {
        for (const transport of layer.transports) {
          const entries = plan.cases.filter(
            (entry) =>
              entry.layer === layer.name &&
              entry.scenario.name === scenario.name &&
              entry.workload === workload &&
              entry.transport === transport,
          );
          assert.deepEqual(
            entries.map((entry) => entry.repetition),
            [1, 2, 3],
          );
          assert.isTrue(entries.every((entry) => entry.seed === 104729));
        }
      }
    }
  }
});

it("gives the clean high-latency RPC acknowledgement workloads enough time to finish", () => {
  const rtt = 2 * presets.find((scenario) => scenario.name === "high-latency")!.delayMs!;
  assert.isAbove(suiteDefinition.timeoutMs.rpc, workloadSettings.rpc.count * rtt);
  assert.isAbove(suiteDefinition.timeoutMs.stream, workloadSettings.stream.count * rtt);
  assert.isAbove(suiteDefinition.timeoutMs.fanout, workloadSettings.fanout.count * rtt);
});

it("rejects missing, duplicate, changed, and unfinished cases before baseline comparison", () => {
  const valid = results();
  validateSuiteResults(valid);
  assert.throws(() => validateSuiteResults({ ...valid, complete: false }), /incomplete/);
  assert.throws(
    () => validateSuiteResults({ ...valid, suite: { hash: "changed" } }),
    /configuration differs/,
  );
  assert.throws(() => validateSuiteResults({ ...valid, rows: valid.rows.slice(1) }), /incomplete/);
  assert.throws(
    () => validateSuiteResults({ ...valid, rows: [valid.rows[1]!, ...valid.rows.slice(1)] }),
    /duplicate/,
  );
  assert.throws(
    () =>
      validateSuiteResults({
        ...valid,
        rows: [{ ...valid.rows[0]!, seed: 42 }, ...valid.rows.slice(1)],
      }),
    /changed suite case/,
  );
});

it("rejects comparing results from different benchmark environments", () => {
  const valid = results();
  assert.throws(
    () =>
      compareSuites(valid, {
        ...valid,
        metadata: { ...valid.metadata, environmentHash: "another-host" },
      }),
    /environments differ/,
  );
});

it("reports a repeatable latency regression without averaging unrelated cases together", () => {
  const group = compareSuites(results(100), results(130)).find(
    (group) => group.case === "rpc/wan/rpc/rpc-wt-multi",
  )!;
  assert.equal(group.completion, "unchanged");
  assert.deepEqual(
    group.metrics.map((metric) => metric.signal),
    ["regressed", "regressed"],
  );
  assert.closeTo(group.metrics[0]!.percent!, 30, 0.001);
});

it("keeps overlapping repeats inconclusive and warns about low tail sample counts", () => {
  const candidate = results(80);
  const noisy = {
    ...candidate,
    rows: candidate.rows.map((row) => ({
      ...row,
      metrics: { ...row.metrics, "rtt.p50": row.repetition === 3 ? 110 : 80, "rtt.n": 20 },
    })),
  };
  const group = compareSuites(results(100), noisy).find(
    (group) => group.case === "raw/wan/rpc/ws",
  )!;
  assert.equal(group.metrics[0]!.signal, "inconclusive");
  assert.equal(group.metrics[1]!.signal, "inconclusive");
  assert.isFalse(group.metrics[1]!.enoughSamples);
});

it("does not award a speedup when a faster-looking candidate stops completing work", () => {
  const candidate = results(1);
  const failed = {
    ...candidate,
    rows: candidate.rows.map((row) =>
      row.workload === "rpc" && row.repetition === 1
        ? { ...row, metrics: { ...row.metrics, outcome: "timeout" } }
        : row,
    ),
  };
  const group = compareSuites(results(100), failed).find(
    (group) => group.case === "rpc/wan/rpc/rpc-wt-multi",
  )!;
  assert.equal(group.completion, "regressed");
  assert.equal(group.candidateCompleted, 2);
  assert.deepEqual(group.metrics, []);
});

it("rejects an incomplete or corrupted workload rather than treating it as a fast completion", () => {
  const record = new Recorder();
  record.expectSamples("latency", 128);
  record.sample("latency", 1);
  assert.isUndefined(record.validationError(false));
  assert.match(record.validationError(true)!, /expected 128 samples, received 1/);
  record.check(false, "truncated payload");
  assert.equal(record.validationError(false), "truncated payload");
});
