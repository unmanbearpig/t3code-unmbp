import { RpcWebTransportConnector } from "@t3tools/client-runtime/platform";
import {
  RpcWebTransportError,
  wrapStandardWebTransportSession,
  type ReliableConnector,
  type StandardWebTransportSession,
} from "@t3tools/shared/rpcWebTransport";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

interface BrowserTransport extends StandardWebTransportSession {
  readonly ready: Promise<unknown>;
}
interface BrowserStreamError {
  readonly streamErrorCode?: number | null;
  readonly source?: string;
}
interface BrowserTransportGlobals {
  readonly WebTransport?: new (url: string) => BrowserTransport;
  readonly WebTransportError?:
    | (new (init: {
        readonly streamErrorCode: number;
        readonly message: string;
      }) => BrowserStreamError)
    | (new (
        message: string,
        options: { readonly source: "stream"; readonly streamErrorCode: number },
      ) => BrowserStreamError);
}

export function browserRpcWebTransportConnector(
  platform: BrowserTransportGlobals = globalThis as unknown as BrowserTransportGlobals,
): ReliableConnector | undefined {
  const Transport = platform.WebTransport;
  const StreamError = platform.WebTransportError as
    | {
        new (init: {
          readonly streamErrorCode: number;
          readonly message: string;
        }): BrowserStreamError;
        new (
          message: string,
          options: { readonly source: "stream"; readonly streamErrorCode: number },
        ): BrowserStreamError;
      }
    | undefined;
  if (Transport === undefined || StreamError === undefined) return undefined;
  const streamError = (code: number) => {
    const message = "T3 RPC stream closed";
    try {
      const error = new StreamError({ streamErrorCode: code, message });
      if (error.streamErrorCode === code && error.source === "stream") return error;
    } catch {
      /* Older platforms may require the two-argument constructor. */
    }
    return new StreamError(message, { source: "stream", streamErrorCode: code });
  };
  return {
    connect: ({ url, signal }) =>
      new Promise((resolve, reject) => {
        if (signal.aborted) {
          reject(new RpcWebTransportError("aborted", "RPC connection cancelled"));
          return;
        }
        // Own the raw transport before awaiting ready, including a blocked CONNECT.
        let raw: BrowserTransport;
        try {
          raw = new Transport(url);
        } catch (cause) {
          reject(new RpcWebTransportError("network", "Could not create RPC transport", { cause }));
          return;
        }
        let session: ReturnType<typeof wrapStandardWebTransportSession>;
        try {
          session = wrapStandardWebTransportSession(raw, { streamError });
        } catch (cause) {
          try {
            raw.close({ closeCode: 0 });
          } catch {
            /* Advisory native cleanup. */
          }
          reject(new RpcWebTransportError("network", "Could not wrap RPC transport", { cause }));
          return;
        }
        let settled = false;
        const abort = () => {
          session.close(0);
          if (settled) return;
          settled = true;
          signal.removeEventListener("abort", abort);
          reject(new RpcWebTransportError("aborted", "RPC connection cancelled"));
        };
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) {
          abort();
          return;
        }
        raw.ready.then(
          () => {
            if (settled) {
              session.close(0);
              return;
            }
            settled = true;
            signal.removeEventListener("abort", abort);
            resolve(session);
          },
          (cause: unknown) => {
            if (settled) return;
            settled = true;
            signal.removeEventListener("abort", abort);
            session.close(0);
            reject(new RpcWebTransportError("network", "RPC transport CONNECT failed", { cause }));
          },
        );
      }),
  };
}

export const layer = Layer.effectContext(
  Effect.sync(() => {
    const connector = browserRpcWebTransportConnector();
    return connector === undefined
      ? Context.empty()
      : Context.make(RpcWebTransportConnector, connector);
  }),
);
