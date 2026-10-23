import { describe, expect, it } from "@effect/vitest";
import type {
  StandardWebTransportSession,
  StandardWebTransportStream,
} from "@t3tools/shared/rpcWebTransport";
import { browserRpcWebTransportConnector } from "./browserWebTransport";

function deferred<A>() {
  let resolve!: (value: A) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<A>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

class ChromiumStreamError {
  readonly streamErrorCode: number;
  readonly source = "stream";
  constructor(init: { readonly streamErrorCode: number; readonly message: string }) {
    this.streamErrorCode = init.streamErrorCode;
  }
}
class W3cStreamError {
  readonly streamErrorCode: number;
  readonly source: string;
  constructor(
    _message: unknown,
    options?: { readonly streamErrorCode: number; readonly source: string },
  ) {
    this.streamErrorCode = options?.streamErrorCode ?? 0;
    this.source = options?.source ?? "session";
  }
}

function fixture() {
  const ready = deferred<void>();
  const reasons: unknown[] = [];
  const reset = deferred<unknown>();
  const transports: string[] = [];
  let closed = 0;
  const stream: StandardWebTransportStream = {
    readable: new ReadableStream<Uint8Array>(),
    writable: {
      getWriter: () => ({
        write: () => Promise.resolve(),
        close: () => Promise.resolve(),
        abort: (reason) => {
          reasons.push(reason);
          reset.resolve(reason);
          return Promise.resolve();
        },
        releaseLock: () => {},
      }),
    },
  };
  class Transport implements StandardWebTransportSession {
    readonly ready = ready.promise;
    readonly closed = new Promise<void>(() => {});
    readonly incomingBidirectionalStreams = new ReadableStream<StandardWebTransportStream>();
    constructor(url: string) {
      transports.push(url);
    }
    createBidirectionalStream() {
      return Promise.resolve(stream);
    }
    close() {
      closed++;
    }
  }
  return { ready, reasons, transports, Transport, stream, reset, closed: () => closed };
}

describe("browser RPC WebTransport connector", () => {
  it("detects client support without a local server addon or optional stats/reliability APIs", async () => {
    const test = fixture();
    const connector = browserRpcWebTransportConnector({
      WebTransport: test.Transport,
      WebTransportError: ChromiumStreamError,
    });
    expect(connector).toBeDefined();
    expect(browserRpcWebTransportConnector({})).toBeUndefined();
    const pending = connector!.connect({
      url: "https://host.example/rpc",
      signal: new AbortController().signal,
    });
    test.ready.resolve(undefined);
    const session = await pending;
    expect(test.transports).toEqual(["https://host.example/rpc"]);
    session.close(0);
    expect(test.closed()).toBe(1);
  });
  it("owns blocked CONNECT immediately and closes after cancellation even if ready completes late", async () => {
    const test = fixture();
    const connector = browserRpcWebTransportConnector({
      WebTransport: test.Transport,
      WebTransportError: ChromiumStreamError,
    })!;
    const abort = new AbortController();
    const pending = connector.connect({ url: "https://host.example/rpc", signal: abort.signal });
    const rejected = expect(pending).rejects.toMatchObject({ category: "aborted" });
    abort.abort();
    await rejected;
    expect(test.closed()).toBe(1);
    test.ready.resolve(undefined);
    await test.ready.promise;
    expect(test.closed()).toBe(1);
  });
  it("cleans up a native stream that completes after its open was cancelled", async () => {
    const test = fixture();
    const opening = deferred<StandardWebTransportStream>();
    test.Transport.prototype.createBidirectionalStream = () => opening.promise;
    const connector = browserRpcWebTransportConnector({
      WebTransport: test.Transport,
      WebTransportError: ChromiumStreamError,
    })!;
    const connected = connector.connect({
      url: "https://host.example/rpc",
      signal: new AbortController().signal,
    });
    test.ready.resolve(undefined);
    const session = await connected;
    const abort = new AbortController();
    const stream = session.openBidirectionalStream(abort.signal);
    const rejected = expect(stream).rejects.toMatchObject({ category: "aborted" });
    abort.abort();
    await rejected;
    opening.resolve(test.stream);
    expect(await test.reset.promise).toMatchObject({ streamErrorCode: 1, source: "stream" });
    expect(test.closed()).toBe(1);
  });
  it("closes a rejected native CONNECT without waiting for closed", async () => {
    const test = fixture();
    const connector = browserRpcWebTransportConnector({
      WebTransport: test.Transport,
      WebTransportError: ChromiumStreamError,
    })!;
    const pending = connector.connect({
      url: "https://host.example/rpc",
      signal: new AbortController().signal,
    });
    const rejected = expect(pending).rejects.toMatchObject({ category: "network" });
    test.ready.reject(new Error("SessionRejected"));
    await rejected;
    expect(test.closed()).toBe(1);
  });
  it.each([ChromiumStreamError, W3cStreamError])(
    "constructs actual stream cancellation errors for %s",
    async (WebTransportError) => {
      const test = fixture();
      const connector = browserRpcWebTransportConnector({
        WebTransport: test.Transport,
        WebTransportError,
      })!;
      const pending = connector.connect({
        url: "https://host.example/rpc",
        signal: new AbortController().signal,
      });
      test.ready.resolve(undefined);
      const session = await pending;
      const stream = await session.openBidirectionalStream(new AbortController().signal);
      stream.reset(19);
      expect(test.reasons[0]).toMatchObject({ streamErrorCode: 19, source: "stream" });
      session.close(0);
    },
  );
});
