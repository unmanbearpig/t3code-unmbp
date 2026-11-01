import { layerDeflatedJson } from "./rpcDeflate.ts";

// The suite fixes operations and network conditions; the target implementation
// can change framing or compression without changing the work delivered.
export const operationRpcSerialization = layerDeflatedJson;
export const operationRpcCompression = "deflate-dictionary-frame-v2-level3";
