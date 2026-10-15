// @effect-diagnostics nodeBuiltinImport:off - Owns the Node listener used by production and transport benchmarks.
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";

import { guardHttpResponseWriteErrors } from "./httpResponseErrorGuard.ts";

/** A different factory can supply TLS while preserving the HTTP and WebSocket settings. */
export const layerNodeHttpServer = (
  address: { readonly host: string; readonly port: number },
  createServer: () => NodeHttp.Server = NodeHttp.createServer,
) =>
  NodeHttpServer.layer(() => guardHttpResponseWriteErrors(createServer()), {
    ...address,
    gracefulShutdownTimeout: 0,
    // Context takeover and ws's default threshold behavior are intentional.
    websocket: { perMessageDeflate: true },
  });
