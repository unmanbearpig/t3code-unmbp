const HEADER_BYTES = 6;
const WIRE_VERSION = 1;
const JSON_ENCODING = 0;
const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

/** One decoder belongs to one stream. finish must run when that stream receives FIN. */
export function makeFramedJson(maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
  if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 1 || maxFrameBytes > 0xffff_ffff) {
    throw new RangeError("Invalid maximum frame size");
  }
  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const header = new Uint8Array(HEADER_BYTES);
  let headerUsed = 0;
  let payload: Uint8Array | undefined;
  let payloadUsed = 0;
  let failed = false;

  const fail = (message: string): never => {
    failed = true;
    payload = undefined;
    throw new Error(message);
  };

  return {
    encode(message: unknown) {
      const json = JSON.stringify(message);
      if (json === undefined) throw new Error("RPC frame must contain JSON");
      const body = encoder.encode(json);
      if (body.byteLength > maxFrameBytes) throw new Error("RPC frame exceeds maximum size");
      const frame = new Uint8Array(HEADER_BYTES + body.byteLength);
      frame[0] = WIRE_VERSION;
      // Encoding 0 is explicitly uncompressed. Compression needs a negotiated new encoding.
      frame[1] = JSON_ENCODING;
      new DataView(frame.buffer).setUint32(2, body.byteLength);
      frame.set(body, HEADER_BYTES);
      return frame;
    },
    decode(bytes: Uint8Array): unknown[] {
      if (failed) throw new Error("RPC frame decoder has failed");
      const messages: unknown[] = [];
      let offset = 0;
      while (offset < bytes.byteLength) {
        if (payload === undefined) {
          const used = Math.min(HEADER_BYTES - headerUsed, bytes.byteLength - offset);
          header.set(bytes.subarray(offset, offset + used), headerUsed);
          headerUsed += used;
          offset += used;
          if (headerUsed !== HEADER_BYTES) continue;
          if (header[0] !== WIRE_VERSION) fail("Unsupported RPC wire version");
          if (header[1] !== JSON_ENCODING) fail("Unsupported RPC frame encoding");
          const length = new DataView(header.buffer).getUint32(2);
          if (length === 0 || length > maxFrameBytes) fail("Invalid RPC frame size");
          payload = new Uint8Array(length);
          payloadUsed = 0;
        }
        const used = Math.min(payload.byteLength - payloadUsed, bytes.byteLength - offset);
        payload.set(bytes.subarray(offset, offset + used), payloadUsed);
        payloadUsed += used;
        offset += used;
        if (payloadUsed !== payload.byteLength) continue;
        try {
          const value: unknown = JSON.parse(decoder.decode(payload));
          if (Array.isArray(value)) messages.push(...value);
          else messages.push(value);
        } catch {
          fail("Invalid RPC frame JSON or UTF-8");
        }
        payload = undefined;
        payloadUsed = 0;
        headerUsed = 0;
      }
      return messages;
    },
    finish() {
      if (failed) throw new Error("RPC frame decoder has failed");
      if (headerUsed !== 0 || payload !== undefined) fail("Truncated RPC frame");
    },
  };
}
