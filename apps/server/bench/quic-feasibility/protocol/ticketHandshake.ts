import * as Effect from "effect/Effect";

import * as SessionStore from "../../../src/auth/SessionStore.ts";

/** Named Promise adapter for the native HTTP/3 server's CONNECT callback. */
export const ticketRequestHandler =
  (sessions: SessionStore.SessionStore["Service"]) => async (path: string) => {
    const url = new URL(path, "https://localhost");
    const ticket = url.searchParams.get("wsTicket");
    if (url.pathname !== "/rpc" || ticket === null) return { status: 401, path: "/rpc" };
    const status = await Effect.runPromise(
      sessions.verifyWebSocketToken(ticket).pipe(
        Effect.match({
          onSuccess: () => 200,
          onFailure: (error) => (SessionStore.isSessionCredentialInvalidError(error) ? 401 : 500),
        }),
      ),
    );
    return { status, path: "/rpc" };
  };
