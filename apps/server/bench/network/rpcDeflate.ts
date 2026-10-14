// @effect-diagnostics nodeBuiltinImport:off - Benchmark transport codec uses native deflate, like the WebSocket control.
import * as NodeZlib from "node:zlib";
import * as Layer from "effect/Layer";
import { RpcSerialization } from "effect/rpc";

import { lengthPrefixed, makeFrameSplitter } from "./protocol.ts";

/** Compresses substantial RPC frames; tiny control messages keep their original JSON bytes. */
export const layerDeflatedJson = Layer.succeed(RpcSerialization.RpcSerialization)(
  RpcSerialization.RpcSerialization.of({
    contentType: "application/x-length-prefixed-json+deflate",
    includesFraming: true,
    codecFor: RpcSerialization.json.codecFor,
    makeUnsafe: () => {
      const encoder = new TextEncoder();
      const decoder = new TextDecoder();
      let decoded: Array<unknown> = [];
      const split = makeFrameSplitter((frame) => {
        if (frame[0] !== 0 && frame[0] !== 1) throw new Error("Invalid RPC compression frame");
        const bytes =
          frame[0] === 1
            ? NodeZlib.inflateRawSync(frame.subarray(1), { maxOutputLength: 16 * 1024 * 1024 })
            : frame.subarray(1);
        const value: unknown = JSON.parse(decoder.decode(bytes));
        if (Array.isArray(value)) decoded.push(...value);
        else decoded.push(value);
      });
      return {
        decode: (bytes) => {
          split(typeof bytes === "string" ? encoder.encode(bytes) : bytes);
          const values = decoded;
          decoded = [];
          return values;
        },
        encode: (message) => {
          const bytes = encoder.encode(JSON.stringify(message));
          const compressed =
            bytes.length >= 256 ? NodeZlib.deflateRawSync(bytes, { level: 3 }) : bytes;
          const useCompressed = compressed.length < bytes.length;
          const body = useCompressed ? compressed : bytes;
          const frame = new Uint8Array(body.length + 1);
          frame[0] = useCompressed ? 1 : 0;
          frame.set(body, 1);
          return lengthPrefixed(frame);
        },
      };
    },
  }),
);
