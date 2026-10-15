import { operationServerConfig } from "./operationsFixture.ts";

/** Public descriptor from a prior connection, used only to seed the opt-in support cache. */
export const reconnectHelloDescriptor = {
  ...operationServerConfig.environment,
  capabilities: { ...operationServerConfig.environment.capabilities, websocketHello: 1 as const },
};
