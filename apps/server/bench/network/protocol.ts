// Wire format shared by the bench server and client. Every message is one frame:
// [u8 kind][u16 channel][u32 seq][f64 sentAt][u32 arg][payload...]. WebSocket sends one frame per
// message; WebTransport streams prefix each frame with a u32 length.

export const HEADER_BYTES = 1 + 2 + 4 + 8 + 4;

export const Kind = {
  /** Client → server: echo the frame back as `pong`. */
  ping: 1,
  pong: 2,
  /** Client → server: stream `seq` messages of `arg` bytes, one every `payload[0]` ms. */
  streamRequest: 3,
  streamMessage: 4,
  /** Client → server: send a single message of `arg` bytes. */
  bulkRequest: 5,
  bulkMessage: 6,
} as const;
export type Kind = (typeof Kind)[keyof typeof Kind];

export interface Frame {
  readonly kind: Kind;
  readonly channel: number;
  readonly seq: number;
  /** Wall-clock ms with sub-ms precision; comparable across processes on one host. */
  readonly sentAt: number;
  readonly arg: number;
  readonly payload: Uint8Array;
}

export const wallClockMs = () => performance.timeOrigin + performance.now();

export function encodeFrame(
  frame: Omit<Frame, "payload"> & { payloadBytes?: number; payload?: Uint8Array },
) {
  const payloadBytes = frame.payload?.byteLength ?? frame.payloadBytes ?? 0;
  const bytes = new Uint8Array(HEADER_BYTES + payloadBytes);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, frame.kind);
  view.setUint16(1, frame.channel);
  view.setUint32(3, frame.seq);
  view.setFloat64(7, frame.sentAt);
  view.setUint32(15, frame.arg);
  if (frame.payload) bytes.set(frame.payload, HEADER_BYTES);
  return bytes;
}

export function decodeFrame(bytes: Uint8Array): Frame {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    kind: view.getUint8(0) as Kind,
    channel: view.getUint16(1),
    seq: view.getUint32(3),
    sentAt: view.getFloat64(7),
    arg: view.getUint32(15),
    payload: bytes.subarray(HEADER_BYTES),
  };
}

export function lengthPrefixed(frame: Uint8Array) {
  const out = new Uint8Array(4 + frame.byteLength);
  new DataView(out.buffer).setUint32(0, frame.byteLength);
  out.set(frame, 4);
  return out;
}

/** Splits a byte stream back into the length-prefixed frames written by `lengthPrefixed`. */
export function makeFrameSplitter(onFrame: (frame: Uint8Array) => void) {
  // Chunks are only joined once a whole frame has arrived, so a 4MB frame delivered in
  // thousands of chunks is copied once instead of once per chunk.
  let chunks: Uint8Array[] = [];
  let buffered = 0;
  let needed = 4;
  let frameLength: number | undefined;
  const take = (count: number) => {
    const out =
      chunks.length === 1 && chunks[0]!.byteLength === count ? chunks[0]! : new Uint8Array(count);
    if (out !== chunks[0]) {
      let offset = 0;
      while (offset < count) {
        const chunk = chunks[0]!;
        const used = Math.min(chunk.byteLength, count - offset);
        out.set(chunk.subarray(0, used), offset);
        offset += used;
        if (used === chunk.byteLength) chunks.shift();
        else chunks[0] = chunk.subarray(used);
      }
    } else {
      chunks = [];
    }
    buffered -= count;
    return out;
  };
  return (chunk: Uint8Array) => {
    chunks.push(chunk);
    buffered += chunk.byteLength;
    while (buffered >= needed) {
      if (frameLength === undefined) {
        const header = take(4);
        frameLength = new DataView(header.buffer, header.byteOffset, 4).getUint32(0);
        needed = frameLength;
      } else {
        onFrame(take(frameLength));
        frameLength = undefined;
        needed = 4;
      }
    }
  };
}

export const ports = { ws: 4401, wss: 4402, wt: 4403 } as const;

/**
 * QUIC receive windows for both ends. The library defaults to a 64KB stream window, which caps a
 * single stream at 64KB per round trip; these match Chromium's limits so results reflect a browser.
 */
export const quicFlowControl = {
  initialStreamFlowControlWindow: 6 * 1024 * 1024,
  streamFlowControlWindowSizeLimit: 6 * 1024 * 1024,
  initialSessionFlowControlWindow: 15 * 1024 * 1024,
  sessionFlowControlWindowSizeLimit: 15 * 1024 * 1024,
} as const;
