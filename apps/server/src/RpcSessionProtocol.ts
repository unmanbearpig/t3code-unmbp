import * as Effect from "effect/Effect";
import type * as RpcServer from "effect/rpc/RpcServer";

/** Stops finalizer responses before a transport's native close can acknowledge. */
export function makeRpcSessionProtocol(underlying: RpcServer.Protocol["Service"]) {
  let closed = false;
  const protocol: RpcServer.Protocol["Service"] = {
    ...underlying,
    send: (clientId, message, transferables) =>
      Effect.suspend(() =>
        closed ? Effect.void : underlying.send(clientId, message, transferables),
      ),
    end: (clientId) => Effect.suspend(() => (closed ? Effect.void : underlying.end(clientId))),
  };
  return {
    protocol,
    close: Effect.sync(() => {
      closed = true;
    }),
  };
}
