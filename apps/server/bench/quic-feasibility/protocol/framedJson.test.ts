import { describe, expect, it } from "@effect/vitest";

import { makeFramedJson } from "./framedJson.ts";

const join = (...chunks: Uint8Array[]) => {
  const result = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
};

describe("portable, bounded RPC framing", () => {
  it("delivers UTF-8 messages and batches for every byte boundary", () => {
    const messages = [{ _tag: "Ping" }, { text: "acción 日本語 🐈" }, { value: 42 }];
    const sender = makeFramedJson();
    const wire = join(sender.encode(messages[0]), sender.encode(messages.slice(1)));
    for (let size = 1; size <= wire.byteLength; size++) {
      const receiver = makeFramedJson();
      const actual: unknown[] = [];
      for (let index = 0; index < wire.byteLength; index += size) {
        actual.push(...receiver.decode(wire.subarray(index, index + size)));
      }
      receiver.finish();
      expect(actual).toEqual(messages);
    }
  });

  it("keeps interleaved streams independent through a large incomplete response", () => {
    const large = { text: "large response ".repeat(300_000) };
    const leftWire = makeFramedJson().encode(large);
    const rightWire = makeFramedJson().encode({ small: true });
    const left = makeFramedJson();
    const right = makeFramedJson();
    expect(left.decode(leftWire.subarray(0, -1))).toEqual([]);
    expect(right.decode(rightWire)).toEqual([{ small: true }]);
    right.finish();
    expect(left.decode(leftWire.subarray(-1))).toEqual([large]);
    left.finish();
  });

  it("rejects oversized declarations before waiting for their payload", () => {
    const receiver = makeFramedJson(32);
    const header = new Uint8Array([1, 0, 255, 255, 255, 255]);
    expect(() => receiver.decode(header)).toThrow("Invalid RPC frame size");
    expect(() => receiver.decode(new Uint8Array())).toThrow("decoder has failed");
    expect(() => makeFramedJson(32).encode({ text: "a".repeat(33) })).toThrow("maximum size");
  });

  it("rejects unknown versions, encodings, empty frames, malformed UTF-8 and JSON", () => {
    for (const wire of [
      [2, 0, 0, 0, 0, 2, 123, 125],
      [1, 1, 0, 0, 0, 2, 123, 125],
      [1, 0, 0, 0, 0, 0],
      [1, 0, 0, 0, 0, 1, 255],
      [1, 0, 0, 0, 0, 1, 123],
    ]) {
      expect(() => makeFramedJson().decode(new Uint8Array(wire))).toThrow();
    }
  });

  it("detects every truncated header or body at FIN", () => {
    const wire = makeFramedJson().encode({ requestId: "123", text: "hello" });
    for (let end = 1; end < wire.byteLength; end++) {
      const receiver = makeFramedJson();
      receiver.decode(wire.subarray(0, end));
      expect(() => receiver.finish()).toThrow("Truncated RPC frame");
    }
    expect(() => makeFramedJson().finish()).not.toThrow();
  });
});
