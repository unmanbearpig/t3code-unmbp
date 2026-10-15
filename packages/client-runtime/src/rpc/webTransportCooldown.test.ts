import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { makeRpcWebTransportCooldown } from "./webTransportCooldown.ts";

const environment = EnvironmentId.make("test");
const endpoint = {
  url: "https://host.example/rpc",
  wireVersion: 1,
  orchestrationProtocolVersion: 2,
};

describe("WebTransport endpoint cooldown", () => {
  it.effect("backs off across attempts from 30 seconds through five minutes", () =>
    Effect.gen(function* () {
      const cooldown = yield* makeRpcWebTransportCooldown;
      for (const delay of [30_000, 60_000, 120_000, 240_000, 300_000, 300_000]) {
        const attempt = yield* cooldown.begin(environment, endpoint);
        expect(attempt).toBeDefined();
        yield* cooldown.failed(attempt!);
        yield* cooldown.failed(attempt!);
        yield* TestClock.adjust(delay - 1);
        expect(yield* cooldown.begin(environment, endpoint)).toBeUndefined();
        yield* TestClock.adjust(1);
      }
    }),
  );
  it.effect("resets after Ready and ignores stale success/failure completions", () =>
    Effect.gen(function* () {
      const cooldown = yield* makeRpcWebTransportCooldown;
      const old = (yield* cooldown.begin(environment, endpoint))!;
      const current = (yield* cooldown.begin(environment, endpoint))!;
      yield* cooldown.failed(current);
      yield* cooldown.succeeded(old);
      yield* cooldown.failed(old);
      expect(yield* cooldown.begin(environment, endpoint)).toBeUndefined();
      yield* TestClock.adjust(30_000);
      const connected = (yield* cooldown.begin(environment, endpoint))!;
      yield* cooldown.succeeded(connected);
      yield* cooldown.failed(connected);
      yield* TestClock.adjust(30_000);
      expect(yield* cooldown.begin(environment, endpoint)).toBeDefined();
    }),
  );
  it.effect("separates environments, canonical endpoints and wire versions", () =>
    Effect.gen(function* () {
      const cooldown = yield* makeRpcWebTransportCooldown;
      yield* cooldown.failed((yield* cooldown.begin(environment, endpoint))!);
      expect(
        yield* cooldown.begin(environment, { ...endpoint, url: "https://HOST.EXAMPLE:443/rpc" }),
      ).toBeUndefined();
      expect(yield* cooldown.begin(EnvironmentId.make("other"), endpoint)).toBeDefined();
      expect(
        yield* cooldown.begin(environment, { ...endpoint, url: "https://host.example/new" }),
      ).toBeDefined();
      expect(yield* cooldown.begin(environment, { ...endpoint, wireVersion: 2 })).toBeDefined();
    }),
  );
  it.effect("evicts the oldest entry after 64 endpoints", () =>
    Effect.gen(function* () {
      const cooldown = yield* makeRpcWebTransportCooldown;
      yield* cooldown.failed((yield* cooldown.begin(environment, endpoint))!);
      for (let i = 0; i < 64; i++) {
        yield* cooldown.begin(environment, { ...endpoint, url: `https://host.example/${i}` });
      }
      expect(yield* cooldown.begin(environment, endpoint)).toBeDefined();
    }),
  );
});
