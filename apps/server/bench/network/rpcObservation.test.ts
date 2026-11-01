import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { observeRpcClient } from "./rpcObservation.ts";

it.effect("observes the actual decoded values lazily and preserves stream and effect results", () =>
  Effect.gen(function* () {
    const observed: unknown[] = [];
    let calls = 0;
    const client = {
      echo: (value: string) =>
        Effect.sync(() => {
          calls++;
          return value;
        }),
      updates: () =>
        Stream.make({ kind: "event", sequence: 1 }, { kind: "synchronized", sequence: 1 }),
    };
    const measured = observeRpcClient(client, {
      request: (tag) => observed.push(tag),
      value: (tag, value) => observed.push([tag, value]),
      session: () => {},
    });
    const echo = measured.echo("same payload");
    const updates = measured.updates();
    expect(observed).toEqual([]);
    expect(yield* echo).toBe("same payload");
    expect(yield* Stream.runCollect(updates)).toEqual([
      { kind: "event", sequence: 1 },
      { kind: "synchronized", sequence: 1 },
    ]);
    expect(calls).toBe(1);
    expect(observed).toEqual([
      "echo",
      ["echo", "same payload"],
      "updates",
      ["updates", { kind: "event", sequence: 1 }],
      ["updates", { kind: "synchronized", sequence: 1 }],
    ]);
  }),
);
