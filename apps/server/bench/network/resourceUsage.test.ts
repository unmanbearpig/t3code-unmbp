import { assert, it } from "@effect/vitest";

import { makeResourceControl, startProcessMeasurement } from "./resourceUsage.ts";
import { validateLinkMtu } from "./impairments.ts";

it("accepts an IPv6 minimum MTU and rejects impossible or fractional link values", () => {
  assert.equal(validateLinkMtu(1280), 1280);
  assert.equal(validateLinkMtu(1500), 1500);
  for (const mtu of [1279, 65536, 1280.5, NaN, Infinity])
    assert.throws(() => validateLinkMtu(mtu), /--mtu/);
});

it("retains one stopped process measurement and reports CPU in milliseconds", () => {
  const measurement = startProcessMeasurement();
  const cpuStart = process.cpuUsage();
  // Observable work gives the measurement a nonzero CPU cost without timing sleeps.
  const bytes = new Uint8Array(1024 * 1024);
  for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251;
  assert.equal(bytes[1000], 247);
  const metrics = measurement.stop();
  const cpuEnd = process.cpuUsage(cpuStart);
  assert.strictEqual(measurement.stop(), metrics);
  assert.isAtLeast(metrics.cpuUserMs!, 0);
  assert.isAtMost(metrics.cpuUserMs!, cpuEnd.user / 1000 + 10);
  assert.isAtLeast(metrics.rssSampledPeakBytes!, metrics.rssStartBytes!);
  assert.isAtLeast(metrics.rssSampledPeakBytes!, metrics.rssEndBytes!);
  assert.isAtLeast(metrics.heapUsedSampledPeakBytes!, metrics.heapUsedEndBytes!);
  assert.isAtLeast(metrics.wallMs!, 0);
  for (const value of Object.values(metrics)) assert.isTrue(Number.isFinite(value));
  if (metrics.eventLoopDelaySamples! < 100)
    assert.notProperty(metrics, "eventLoopDelayObservedP99Ms");
});

it("does not consume fixture requests or malformed resource commands", () => {
  const control = makeResourceControl();
  for (const message of [{ count: 5 }, { _tag: "unknown" }, null, "ready"])
    assert.isFalse(control(message));
});
