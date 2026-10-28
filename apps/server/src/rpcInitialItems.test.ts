import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import { Headers } from "effect/http";
import { Rpc, RpcGroup, RpcMessage, RpcServer } from "effect/rpc";

import { rpcInitialItems } from "./rpcInitialItems.ts";

it.effect("holds the legacy completion marker until the data chunk is acknowledged", () =>
  Effect.gen(function* () {
    const group = RpcGroup.make(
      Rpc.make("catchup", { success: Schema.String, stream: true }),
      Rpc.make("barrier", { success: Schema.Void }),
    );
    const messages = yield* Queue.unbounded<RpcMessage.FromServer<RpcGroup.Rpcs<typeof group>>>();
    const server = yield* RpcServer.makeNoSerialization(group, {
      onFromServer: (message) => Queue.offer(messages, message).pipe(Effect.asVoid),
    }).pipe(
      Effect.provide(
        group.toLayer({
          catchup: () => rpcInitialItems(["data"], "marker", true),
          barrier: () => Effect.void,
        }),
      ),
    );
    const id = RpcMessage.RequestId("stream");
    yield* server.write(0, {
      _tag: "Request",
      id,
      tag: "catchup",
      payload: undefined,
      headers: Headers.empty,
    });
    expect(yield* Queue.take(messages)).toMatchObject({ _tag: "Chunk", values: ["data"] });
    // A completed independent request proves the server kept processing while
    // this request awaited Ack. The marker cannot overtake that acknowledgement.
    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("barrier"),
      tag: "barrier",
      payload: undefined,
      headers: Headers.empty,
    });
    expect(yield* Queue.take(messages)).toMatchObject({ _tag: "Exit", requestId: "barrier" });
    expect(yield* Queue.size(messages)).toBe(0);
    yield* server.write(0, { _tag: "Ack", requestId: id });
    expect(yield* Queue.take(messages)).toMatchObject({ _tag: "Chunk", values: ["marker"] });
    yield* server.write(0, { _tag: "Ack", requestId: id });
    expect(yield* Queue.take(messages)).toMatchObject({ _tag: "Exit", requestId: "stream" });
  }),
);
