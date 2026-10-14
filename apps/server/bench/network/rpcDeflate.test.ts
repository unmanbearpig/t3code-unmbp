import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { RpcSerialization } from "effect/rpc";

import { operationMessageText } from "./operationsFixture.ts";
import { layerDeflatedJson } from "./rpcDeflate.ts";

const codec = Effect.map(RpcSerialization.RpcSerialization, (serialization) =>
  serialization.makeUnsafe(),
);

function binaryBytes(bytes: unknown) {
  if (!(bytes instanceof Uint8Array)) throw new Error("Expected binary RPC framing");
  return bytes;
}

it.effect("delivers exact RPC messages when compressed frames arrive split or joined", () =>
  Effect.gen(function* () {
    const messages = [
      { _tag: "Ping" },
      {
        _tag: "Chunk",
        requestId: "1",
        values: Array.from({ length: 20 }, (_, i) => ({ id: i, text: operationMessageText(i) })),
      },
      { _tag: "Exit", requestId: "1", exit: { _tag: "Success", value: null } },
    ];
    const sender = yield* codec;
    const encoded = messages.map((message) => binaryBytes(sender.encode(message)));
    assert.isBelow(
      encoded[1]!.length,
      new TextEncoder().encode(JSON.stringify(messages[1])).length,
    );
    const joined = new Uint8Array(encoded.reduce((total, bytes) => total + bytes.length, 0));
    let offset = 0;
    for (const bytes of encoded) {
      joined.set(bytes, offset);
      offset += bytes.length;
    }
    for (const size of [1, 3, 1200, joined.length]) {
      const receiver = yield* codec;
      const observed: unknown[] = [];
      for (let index = 0; index < joined.length; index += size)
        observed.push(...receiver.decode(joined.subarray(index, index + size)));
      assert.deepEqual(observed, messages);
    }
  }).pipe(Effect.provide(layerDeflatedJson)),
);

it.effect("decodes a batched RPC response as separate messages and retains unfinished frames", () =>
  Effect.gen(function* () {
    const messages = [
      { _tag: "Chunk", requestId: "1", values: [{ text: operationMessageText(100) }] },
      { _tag: "Chunk", requestId: "2", values: [{ text: operationMessageText(101) }] },
    ];
    const sender = yield* codec;
    const wire = binaryBytes(sender.encode(messages));
    const receiver = yield* codec;
    assert.deepEqual(receiver.decode(wire.subarray(0, -1)), []);
    assert.deepEqual(receiver.decode(wire.subarray(-1)), messages);
  }).pipe(Effect.provide(layerDeflatedJson)),
);
