import { layerFramedJson } from "./rpc.ts";

// The suite fixes operations and network conditions; the target implementation
// can change framing or compression without changing the work delivered.
export const operationRpcSerialization = layerFramedJson;
export const operationRpcCompression = "none";
