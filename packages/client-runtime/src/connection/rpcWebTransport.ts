import type { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import { RPC_WEBTRANSPORT_WIRE_VERSION } from "@t3tools/shared/rpcWebTransport";

import type { ConnectionTarget, PreparedRpcWebTransport } from "./model.ts";

/** Only an HTTPS discovery response on a direct route may nominate an endpoint. */
export function eligibleRpcWebTransport(input: {
  readonly target: ConnectionTarget;
  readonly httpBaseUrl: string;
  readonly descriptor: ExecutionEnvironmentDescriptor;
  readonly connectorAvailable: boolean;
}): PreparedRpcWebTransport | undefined {
  const advertised = input.descriptor.rpcWebTransport;
  if (
    !input.connectorAvailable ||
    (input.target._tag !== "PrimaryConnectionTarget" &&
      input.target._tag !== "BearerConnectionTarget") ||
    advertised?.wireVersion !== RPC_WEBTRANSPORT_WIRE_VERSION ||
    input.descriptor.orchestrationProtocolVersion === undefined
  )
    return undefined;
  try {
    const discovery = new URL(input.httpBaseUrl);
    const endpoint = new URL(advertised.url);
    if (
      discovery.protocol !== "https:" ||
      endpoint.protocol !== "https:" ||
      discovery.hostname.toLowerCase().replace(/\.$/, "") !==
        endpoint.hostname.toLowerCase().replace(/\.$/, "") ||
      endpoint.username ||
      endpoint.password ||
      endpoint.search ||
      endpoint.hash ||
      advertised.url.includes("?") ||
      advertised.url.includes("#")
    )
      return undefined;
    endpoint.hostname = endpoint.hostname.toLowerCase().replace(/\.$/, "");
    return {
      url: endpoint.toString(),
      wireVersion: advertised.wireVersion,
      orchestrationProtocolVersion: input.descriptor.orchestrationProtocolVersion,
    };
  } catch {
    return undefined;
  }
}
