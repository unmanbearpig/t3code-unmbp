import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HttpClient from "effect/http/HttpClient";

import type {
  ConnectionAttemptError,
  ConnectionAttemptStage,
  ConnectionTargetKind,
} from "./model.ts";
import type { ConnectionWakeup } from "./wakeups.ts";

export interface DiagnosticRoute {
  readonly targetKind: ConnectionTargetKind;
  readonly scheme: "https" | "http" | "unknown";
  readonly hostType: "ts.net" | "tailnet-ip" | "lan" | "loopback" | "relay" | "public" | "unknown";
}

/** Deliberately excludes hostnames, IP addresses, credentials, and arbitrary error text. */
export function diagnosticRoute(
  targetKind: ConnectionTargetKind,
  address: string,
): DiagnosticRoute {
  try {
    const url = new URL(address);
    const host = url.hostname.toLowerCase();
    const octets = host.split(".").map(Number);
    const ipv4 =
      octets.length === 4 &&
      octets.every((part) => Number.isInteger(part) && part >= 0 && part <= 255);
    const hostType =
      targetKind === "RelayConnectionTarget"
        ? "relay"
        : host.endsWith(".ts.net")
          ? "ts.net"
          : (ipv4 && octets[0] === 100 && octets[1]! >= 64 && octets[1]! <= 127) ||
              host.startsWith("[fd7a:115c:a1e0:")
            ? "tailnet-ip"
            : host === "localhost" || host === "[::1]" || (ipv4 && octets[0] === 127)
              ? "loopback"
              : host.endsWith(".local") ||
                  (!host.includes(".") && !host.includes(":")) ||
                  (ipv4 &&
                    (octets[0] === 10 ||
                      (octets[0] === 192 && octets[1] === 168) ||
                      (octets[0] === 172 && octets[1]! >= 16 && octets[1]! <= 31) ||
                      (octets[0] === 169 && octets[1] === 254))) ||
                  /^\[f[cd]|^\[fe[89ab]/.test(host)
                ? "lan"
                : "public";
    return {
      targetKind,
      scheme:
        url.protocol === "https:" || url.protocol === "wss:"
          ? "https"
          : url.protocol === "http:" || url.protocol === "ws:"
            ? "http"
            : "unknown",
      hostType,
    };
  } catch {
    return {
      targetKind,
      scheme: "unknown",
      hostType: targetKind === "RelayConnectionTarget" ? "relay" : "unknown",
    };
  }
}

/** Only known static endpoint paths survive. Unknown paths may themselves contain secrets. */
export function diagnosticHttpPath(address: string): string {
  try {
    const path = new URL(address).pathname;
    if (
      [
        "/.well-known/t3/environment",
        "/api/auth/websocket-ticket",
        "/api/auth/session",
        "/oauth/token",
        "/v1/client/dpop-token",
      ].includes(path)
    )
      return path;
    if (/^\/v1\/environments\/[^/]+\/(connect|status)$/.test(path))
      return path.replace(/\/environments\/[^/]+\//, "/environments/:environmentId/");
    return "/:redacted";
  } catch {
    return "/:redacted";
  }
}

export type ConnectionDiagnosticDetail =
  | {
      readonly kind: "wakeup";
      readonly reason: ConnectionWakeup;
      readonly hasSession: boolean;
      readonly desired: boolean;
    }
  | { readonly kind: "attempt-start" }
  | {
      readonly kind: "stage";
      readonly stage: ConnectionAttemptStage;
      readonly route?: DiagnosticRoute;
    }
  | {
      readonly kind: "attempt-end";
      readonly outcome: "connected" | "failed" | "interrupted";
      readonly error: ConnectionAttemptError["reason"] | null;
    }
  | {
      readonly kind: "state";
      readonly phase: "offline" | "backoff" | "blocked";
      readonly error: ConnectionAttemptError["reason"] | null;
    }
  | { readonly kind: "session-closed" }
  | {
      readonly kind: "probe";
      readonly result: "started" | "answered" | "failed" | "timeout" | "interrupted";
      readonly route?: DiagnosticRoute;
    }
  | {
      readonly kind: "http";
      readonly path: string;
      readonly durationMs: number;
      readonly status: number | null;
      readonly outcome: "response" | "failed" | "interrupted";
    }
  | {
      readonly kind: "thread-sync";
      readonly threadId: string;
      readonly mode: "replay" | "snapshot";
      readonly milestone: "started" | "live" | "failed";
      readonly completion: "marker" | "legacy";
      readonly error?: "stream" | "missing";
    };

export type ConnectionDiagnosticEvent = ConnectionDiagnosticDetail & {
  readonly environmentId: string;
};

/** Optional platform-owned, local diagnostics. Observers must be cheap and must not throw. */
export class ConnectionDiagnostics extends Context.Reference<{
  readonly record: (event: ConnectionDiagnosticEvent) => void;
} | null>("@t3tools/client-runtime/connection/ConnectionDiagnostics", {
  defaultValue: () => null,
}) {}

/** Associates concurrent preparation HTTP requests with their owning connection attempt. */
export class PreparationDiagnosticContext extends Context.Reference<{
  readonly record: (event: Extract<ConnectionDiagnosticEvent, { kind: "http" }>) => void;
  readonly environmentId: string;
} | null>("@t3tools/client-runtime/connection/PreparationDiagnosticContext", {
  defaultValue: () => null,
}) {}

export const observePreparationHttp = (client: HttpClient.HttpClient): HttpClient.HttpClient =>
  HttpClient.transform(client, (response, request) =>
    Effect.gen(function* () {
      const context = yield* PreparationDiagnosticContext;
      if (context === null) return yield* response;
      const started = yield* Clock.monotonicTimeNanos;
      return yield* response.pipe(
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            const ended = yield* Clock.monotonicTimeNanos;
            context.record({
              environmentId: context.environmentId,
              kind: "http",
              path: diagnosticHttpPath(request.url),
              durationMs: Number(ended - started) / 1_000_000,
              status: Exit.isSuccess(exit) ? exit.value.status : null,
              outcome: Exit.isSuccess(exit)
                ? "response"
                : exit.cause.reasons.every((reason) => reason._tag === "Interrupt")
                  ? "interrupted"
                  : "failed",
            });
          }),
        ),
      );
    }),
  );
