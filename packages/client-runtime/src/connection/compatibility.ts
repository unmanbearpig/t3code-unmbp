import {
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  type ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";

import { ConnectionBlockedError } from "./model.ts";

export function orchestrationProtocolCompatibilityError(
  descriptor: Pick<ExecutionEnvironmentDescriptor, "orchestrationProtocolVersion" | "label"> &
    Partial<ExecutionEnvironmentDescriptor>,
): ConnectionBlockedError | null {
  // Servers shipped before negotiation use the original wire protocol.
  const serverProtocolVersion = descriptor.orchestrationProtocolVersion ?? 1;
  if (serverProtocolVersion === ORCHESTRATION_PROTOCOL_VERSION) {
    return null;
  }
  return serverProtocolVersion > ORCHESTRATION_PROTOCOL_VERSION
    ? new ConnectionBlockedError({
        reason: "unsupported",
        detail: `This client is not supported by this server. Update your app or use a compatible release to connect to ${descriptor.label}.`,
      })
    : new ConnectionBlockedError({
        reason: "unsupported",
        detail: `This client requires a newer server. Update T3 Code on ${descriptor.label} to connect.`,
        ...(descriptor.capabilities !== undefined && canSelfUpdate(descriptor.capabilities)
          ? { serverUpdateRequired: true }
          : {}),
      });
}

/** Whether this client can drive the host's update remotely. */
function canSelfUpdate(capabilities: ExecutionEnvironmentDescriptor["capabilities"]): boolean {
  const { serverSelfUpdate, desktopAppUpdate } = capabilities;
  return (
    serverSelfUpdate !== undefined &&
    (serverSelfUpdate !== "desktop-managed" || desktopAppUpdate === true)
  );
}

export function appendOrchestrationProtocol(socketUrl: string): string {
  const url = new URL(socketUrl);
  url.searchParams.set(ORCHESTRATION_PROTOCOL_QUERY_PARAM, String(ORCHESTRATION_PROTOCOL_VERSION));
  return url.toString();
}
