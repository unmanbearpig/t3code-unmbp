import { assert, it } from "@effect/vitest";

import { parseScenario, parseScenarios, validatePacketPhase } from "./impairments.ts";
import { decodeFrame, encodeFrame, Kind, lengthPrefixed, makeFrameSplitter } from "./protocol.ts";
import { Recorder } from "./workloads.ts";

it("decodes frames across split headers, split bodies, and joined messages", () => {
  const frames = [new Uint8Array([1, 2, 3]), new Uint8Array(), new Uint8Array([4, 5])];
  const wire = new Uint8Array(frames.reduce((sum, frame) => sum + frame.length + 4, 0));
  let offset = 0;
  for (const frame of frames) {
    wire.set(lengthPrefixed(frame), offset);
    offset += frame.length + 4;
  }
  for (const chunkSize of [1, 2, 3, 7, wire.length]) {
    const decoded: Uint8Array[] = [];
    const split = makeFrameSplitter((frame) => decoded.push(frame));
    for (let index = 0; index < wire.length; index += chunkSize) {
      // Nonzero byteOffset catches accidentally reading the underlying buffer's first header.
      split(wire.subarray(index, index + chunkSize));
    }
    assert.deepEqual(decoded, frames);
  }
});

it("reassembles a long-thread payload fragmented into network-sized chunks", () => {
  const payload = Uint8Array.from({ length: 4 * 1024 * 1024 }, (_, i) => i % 251);
  const wire = lengthPrefixed(payload);
  const decoded: Uint8Array[] = [];
  const split = makeFrameSplitter((frame) => decoded.push(frame));
  for (let index = 0; index < wire.length; index += 1200) split(wire.subarray(index, index + 1200));
  assert.equal(decoded.length, 1);
  assert.deepEqual(decoded[0], payload);
});

it("round trips frame metadata and payload from a byte-offset view", () => {
  const frame = {
    kind: Kind.ping,
    channel: 8,
    seq: 17,
    sentAt: 1234.5,
    arg: 3,
    payload: new Uint8Array([7, 8, 9]),
  };
  const encoded = encodeFrame(frame);
  const padded = new Uint8Array(encoded.length + 11);
  padded.set(encoded, 11);
  assert.deepEqual(decodeFrame(padded.subarray(11)), frame);
});

it("keeps custom impairment parameters together alongside presets", () => {
  assert.deepEqual(parseScenarios("wan,slow:delay=150,jitter=15,loss=2,nthDelay=3/300,loss5"), [
    parseScenario("wan"),
    { name: "slow", delayMs: 150, jitterMs: 15, loss: "2%", nthDelay: { every: 3, extraMs: 300 } },
    parseScenario("loss5"),
  ]);
});

it("rejects invalid impairments before running tc or iptables", () => {
  for (const spec of [
    "missing",
    "bad:delay=-1",
    "bad:delay=NaN",
    "bad:loss=101",
    "bad:nthDrop=0",
    "bad:nthDrop=2.5",
    "bad:nthDelay=3",
    "bad:nthDelay=3/NaN",
    "bad:delay=",
    "bad:unknown=1",
  ]) {
    assert.throws(() => parseScenario(spec));
  }
});

it("validates all periodic intervals and rejects priming that cannot drain", () => {
  assert.doesNotThrow(() => validatePacketPhase(257, [{ name: "large", nthDrop: 300 }]));
  assert.doesNotThrow(() =>
    validatePacketPhase(19, [{ name: "jitter", nthDrop: 20, jitterMs: 20, delayMs: 25 }]),
  );
  for (const phase of [-1, 0.5, NaN, Infinity])
    assert.throws(() => validatePacketPhase(phase, [{ name: "drop", nthDrop: 20 }]));
  assert.throws(
    () =>
      validatePacketPhase(4, [{ name: "both", nthDrop: 20, nthDelay: { every: 3, extraMs: 300 } }]),
    /every/,
  );
  assert.throws(
    () => validatePacketPhase(1, [{ name: "loss", nthDrop: 20, loss: "100%" }]),
    /stochastic loss/,
  );
  assert.throws(() => validatePacketPhase(0, [{ name: "clean" }]), /periodic interval/);
});

it("retains partial measurements and cancels resources acquired after a deadline", () => {
  const record = new Recorder();
  record.sample("rtt", 50);
  record.sample("rtt", 300);
  let closed = 0;
  record.onAbort(() => closed++);
  record.abort();
  record.onAbort(() => closed++);
  record.abort();
  assert.equal(closed, 2);
  assert.isTrue(record.signal.aborted);
  assert.deepEqual(record.metrics(), {
    "rtt.n": 2,
    "rtt.p50": 300,
    "rtt.p95": 300,
    "rtt.p99": 300,
    "rtt.max": 300,
  });
});
