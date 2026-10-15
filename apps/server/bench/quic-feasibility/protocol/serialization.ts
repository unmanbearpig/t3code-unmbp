import * as Layer from "effect/Layer";
import { RpcSerialization } from "effect/rpc";

import { makeFramedJson } from "./framedJson.ts";

/** Test-only uncompressed control. It is not wire-compatible with benchmark deflate. */
export const layerFramedJsonV1 = Layer.succeed(RpcSerialization.RpcSerialization, {
  contentType: "application/x-t3-quic-json;version=1;encoding=identity",
  includesFraming: true,
  codecFor: RpcSerialization.json.codecFor,
  makeUnsafe: () => {
    const parser = makeFramedJson();
    return {
      ...parser,
      decode: (bytes) => {
        if (typeof bytes === "string") throw new Error("Binary RPC frame expected");
        return parser.decode(bytes);
      },
    };
  },
});
