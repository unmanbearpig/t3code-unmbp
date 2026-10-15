import { expect, it } from "@effect/vitest";
import type {
  StandardWebTransportSession,
  StandardWebTransportStream,
} from "@t3tools/shared/rpcWebTransport";
import { diagnosticError, makeReconnectDiagnostics } from "./reconnectDiagnostics.ts";

it("preserves native read/write promise identity and records final data after an earlier closed notification", async () => {
  const closed = Promise.withResolvers<void>();
  const read = Promise.withResolvers<{ done: boolean; value: Uint8Array }>();
  const write = Promise.withResolvers<void>();
  const reader = {
    closed: closed.promise,
    read: () => read.promise,
    cancel: async () => {},
    releaseLock: () => {},
  };
  const native: StandardWebTransportStream = {
    readable: { getReader: () => reader },
    writable: {
      getWriter: () => ({
        write: () => write.promise,
        close: async () => {},
        abort: async () => {},
        releaseLock: () => {},
      }),
    },
  };
  const session: StandardWebTransportSession = {
    closed: new Promise(() => {}),
    incomingBidirectionalStreams: {
      getReader: () => ({
        read: async () => ({ done: true }),
        cancel: async () => {},
        releaseLock: () => {},
      }),
    },
    createBidirectionalStream: async () => native,
    close: () => {},
  };
  const trace = makeReconnectDiagnostics("client");
  const stream = await trace.session(session).createBidirectionalStream();
  const pending = stream.readable.getReader({ mode: "byob" }).read(new Uint8Array(20));
  expect(pending).toBe(read.promise);
  const frame = new Uint8Array([84, 51, 81, 1, 1, 1, 1, 0, 0, 0, 3, 0, 0, 0, 3, 7, 8, 9]);
  expect(stream.writable.getWriter().write(frame)).toBe(write.promise);
  write.resolve();
  closed.resolve();
  await closed.promise;
  read.resolve({ done: false, value: frame.subarray(5) });
  await pending;
  const state = trace.snapshot().streams[0]!;
  expect(state.reads).toMatchObject({
    bytes: 13,
    records: 1,
    messages: 1,
    bodyRemaining: 0,
    invalid: false,
  });
  expect(state.writes).toMatchObject({ ...state.reads, bytes: 18 });
  expect(state.events.findIndex((item) => item.event === "reader-closed")).toBeLessThan(
    state.events.findIndex((item) => item.event === "reader-read-result"),
  );
});

it("retains nested failure reasons while removing credentials from stacks and admission URLs", () => {
  const cause = new Error(
    "https://example.invalid/rpc?wsTicket=private-ticket Bearer private-token",
  );
  const error = new Error("outer", { cause });
  const diagnostic = JSON.stringify(diagnosticError(error));
  expect(diagnostic).toContain("outer");
  expect(diagnostic).toContain("https://example.invalid/rpc");
  expect(diagnostic).not.toContain("private-ticket");
  expect(diagnostic).not.toContain("private-token");
});

it.each(["client", "server"] as const)(
  "counts %s framing with a one-way preface and preserves the original BYOB request size",
  async (side) => {
    const record = new Uint8Array([1, 1, 0, 0, 0, 3, 0, 0, 0, 3, 7, 8, 9]);
    const prefaced = new Uint8Array([84, 51, 81, 1, 1, ...record]);
    const native: StandardWebTransportStream = {
      readable: new ReadableStream({
        type: "bytes",
        start(controller) {
          controller.enqueue((side === "server" ? prefaced : record).slice());
          controller.close();
        },
      }),
      writable: {
        getWriter: () => ({
          write: async () => {},
          close: async () => {},
          abort: async () => {},
          releaseLock: () => {},
        }),
      },
    };
    const trace = makeReconnectDiagnostics(side);
    const stream = await trace
      .session({
        closed: new Promise(() => {}),
        incomingBidirectionalStreams: {
          getReader: () => ({
            read: async () => ({ done: true }),
            cancel: async () => {},
            releaseLock: () => {},
          }),
        },
        createBidirectionalStream: async () => native,
        close: () => {},
      })
      .createBidirectionalStream();
    const view = new Uint8Array(20);
    const pending = stream.readable.getReader({ mode: "byob" }).read(view);
    expect(view.byteLength).toBe(0);
    await pending;
    await stream.writable.getWriter().write(side === "client" ? prefaced : record);
    const state = trace.snapshot().streams[0]!;
    expect(state.reads).toMatchObject({
      bytes: side === "server" ? 18 : 13,
      records: 1,
      messages: 1,
      invalid: false,
      bodyRemaining: 0,
    });
    expect(state.writes).toMatchObject({
      bytes: side === "client" ? 18 : 13,
      records: 1,
      messages: 1,
      invalid: false,
      bodyRemaining: 0,
    });
    expect(state.events.find((event) => event.event === "reader-read")?.requested).toBe(20);
  },
);
