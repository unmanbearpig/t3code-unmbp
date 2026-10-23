import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Socket from "effect/socket/Socket";

import type { PreparedConnection } from "../connection/model.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";

export const makeWebSocketLayer = Effect.gen(function* () {
  const constructor = yield* Socket.WebSocketConstructor;
  const constructorWithHeaders = yield* Effect.serviceOption(
    ClientCapabilities.WebSocketConstructorWithHeaders,
  );

  return (connection: PreparedConnection, onOpen?: () => void, oldArm = false) => {
    const authorization = connection.httpAuthorization;
    const makeWebSocket =
      !oldArm && authorization?._tag === "Bearer" && Option.isSome(constructorWithHeaders)
        ? (url: string) =>
            constructorWithHeaders.value(url, {
              Authorization: `Bearer ${authorization.token}`,
            })
        : constructor;
    return Socket.layerWebSocket(connection.socketUrl, { openTimeout: "15 seconds" }).pipe(
      Layer.provide(
        Layer.succeed(Socket.WebSocketConstructor, (url) => {
          const socket = makeWebSocket(url);
          if (onOpen !== undefined) {
            if (socket.readyState === 1) onOpen();
            else socket.addEventListener("open", onOpen, { once: true });
          }
          return socket;
        }),
      ),
    );
  };
});
