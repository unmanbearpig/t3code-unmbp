import type { ReliableConnector } from "@t3tools/shared/rpcWebTransport";
import * as Context from "effect/Context";

/** Omitted when the platform or installed native client cannot use WebTransport. */
export class RpcWebTransportConnector extends Context.Service<
  RpcWebTransportConnector,
  ReliableConnector
>()("@t3tools/client-runtime/platform/rpcWebTransport/RpcWebTransportConnector") {}
