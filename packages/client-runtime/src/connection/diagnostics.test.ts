import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import {
  diagnosticHttpPath,
  diagnosticRoute,
  observePreparationHttp,
  observeSessionScope,
  PreparationDiagnosticContext,
  type ConnectionDiagnosticEvent,
} from "./diagnostics.ts";

describe("connection diagnostic redaction", () => {
  it("classifies routes without retaining any address or credential", () => {
    const cases = [
      ["https://user:secret@my-phone.private.ts.net:443/?token=secret", "ts.net", "https"],
      ["http://100.64.0.1:3000?token=secret", "tailnet-ip", "http"],
      ["https://100.127.255.254", "tailnet-ip", "https"],
      ["http://192.168.1.2", "lan", "http"],
      ["http://172.16.2.3", "lan", "http"],
      ["https://[fd7a:115c:a1e0::1]", "tailnet-ip", "https"],
      ["http://localhost:3000", "loopback", "http"],
      ["https://[2001:db8::1]", "public", "https"],
      ["https://[fe80::1]", "lan", "https"],
      ["https://example.com", "public", "https"],
      ["invalid secret", "unknown", "unknown"],
    ] as const;
    for (const [url, hostType, scheme] of cases) {
      const route = diagnosticRoute("BearerConnectionTarget", url);
      expect(route).toEqual({ targetKind: "BearerConnectionTarget", hostType, scheme });
      expect(JSON.stringify(route)).not.toMatch(/secret|my-phone|192\.168|100\.64/);
    }
    expect(
      diagnosticRoute("RelayConnectionTarget", "https://relay.private.example/token").hostType,
    ).toBe("relay");
  });

  it("retains static preparation paths and removes queries, credentials and dynamic paths", () => {
    expect(
      diagnosticHttpPath(
        "https://user:password@private.ts.net/.well-known/t3/environment?token=secret#secret",
      ),
    ).toBe("/.well-known/t3/environment");
    expect(diagnosticHttpPath("https://host/api/auth/websocket-ticket?ticket=secret")).toBe(
      "/api/auth/websocket-ticket",
    );
    expect(diagnosticHttpPath("https://host/v1/environments/private-id/connect?token=secret")).toBe(
      "/v1/environments/:environmentId/connect",
    );
    expect(diagnosticHttpPath("https://host/private-token/secret")).toBe("/:redacted");
    expect(diagnosticHttpPath("Bearer secret")).toBe("/:redacted");
  });
});

it.effect(
  "observes only preparation HTTP requests, preserving response and redacting the URL",
  () =>
    Effect.gen(function* () {
      const events: ConnectionDiagnosticEvent[] = [];
      const client = observePreparationHttp(
        HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 201 }))),
        ),
      );
      yield* client.get("https://host/api/auth/session?token=secret");
      expect(events).toEqual([]);
      const result = yield* client
        .get("https://user:password@host/api/auth/websocket-ticket?ticket=secret")
        .pipe(
          Effect.provideService(PreparationDiagnosticContext, {
            environmentId: "environment",
            record: (event) => events.push(event),
          }),
        );
      expect(result.status).toBe(201);
      expect(events).toEqual([
        {
          kind: "http",
          environmentId: "environment",
          path: "/api/auth/websocket-ticket",
          durationMs: expect.any(Number),
          status: 201,
          outcome: "response",
        },
      ]);
      expect(JSON.stringify(events)).not.toMatch(/secret|password|user|host/);
    }),
);

it.effect.each([0, 37])(
  "measures %s ms of session release without including session lifetime",
  (duration) =>
    Effect.gen(function* () {
      const events: ConnectionDiagnosticEvent[] = [];
      const using = yield* Deferred.make<void>();
      const finishUse = yield* Deferred.make<void>();
      const releasing = yield* Deferred.make<void>();
      const finishRelease = yield* Deferred.make<void>();
      const fiber = yield* observeSessionScope(
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(releasing, undefined).pipe(
              Effect.andThen(Deferred.await(finishRelease)),
            ),
          );
          yield* Deferred.succeed(using, undefined);
          yield* Deferred.await(finishUse);
          return 42;
        }),
        { record: (event) => events.push(event) },
        "environment",
      ).pipe(Effect.forkChild);
      yield* Deferred.await(using);
      yield* TestClock.adjust(1000);
      const beforeRelease = [...events];
      yield* Deferred.succeed(finishUse, undefined);
      yield* Deferred.await(releasing);
      const duringRelease = [...events];
      yield* TestClock.adjust(duration);
      yield* Deferred.succeed(finishRelease, undefined);
      expect(yield* Fiber.join(fiber)).toBe(42);
      expect(beforeRelease).toEqual([]);
      expect(duringRelease).toMatchObject([
        { kind: "cleanup", resource: "session", milestone: "started", durationMs: null },
      ]);
      expect(events.at(-1)).toMatchObject({
        milestone: "finished",
        durationMs: duration,
        outcome: "completed",
      });
    }),
);

it.effect("retains interruption and waits for session finalizers before reporting release", () =>
  Effect.gen(function* () {
    const events: ConnectionDiagnosticEvent[] = [];
    const using = yield* Deferred.make<void>();
    const releasing = yield* Deferred.make<void>();
    const finishRelease = yield* Deferred.make<void>();
    const fiber = yield* observeSessionScope(
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(releasing, undefined).pipe(
            Effect.andThen(Deferred.await(finishRelease)),
          ),
        );
        yield* Deferred.succeed(using, undefined);
        return yield* Effect.never;
      }),
      { record: (event) => events.push(event) },
      "environment",
    ).pipe(Effect.forkChild);
    yield* Deferred.await(using);
    const interrupt = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
    yield* Deferred.await(releasing);
    yield* TestClock.adjust(25);
    const duringRelease = [...events];
    yield* Deferred.succeed(finishRelease, undefined);
    yield* Fiber.join(interrupt);
    expect(duringRelease).toHaveLength(1);
    expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
    expect(events.at(-1)).toMatchObject({
      milestone: "finished",
      durationMs: 25,
      outcome: "interrupted",
    });
  }),
);
