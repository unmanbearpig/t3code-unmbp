import { describe, expect, it, vi } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  EnvironmentId,
  WS_METHODS,
  type AuthSessionState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";

import { PrimaryConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as RpcHttp from "../rpc/http.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createCommandPermissions } from "./commandPermissions.ts";
import { createEnvironmentSessionAtoms } from "./session.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("session-environment"),
  label: "Session environment",
  httpBaseUrl: "https://environment.test",
  wsBaseUrl: "wss://environment.test",
});
const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: `${TARGET.wsBaseUrl}/ws`,
  httpAuthorization: null,
  target: TARGET,
};
const SESSION: AuthSessionState = {
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test-session",
  },
  scopes: [AuthOrchestrationOperateScope],
  permissions: [AuthOrchestrationOperateScope],
};

const makeHarness = Effect.fnUntraced(function* (
  reply: () => Response | Promise<Response>,
  connected = true,
) {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  yield* Effect.addFinalizer(() => Effect.sync(() => vi.useRealTimers()));
  const prepared = yield* SubscriptionRef.make(Option.some(PREPARED));
  const session = yield* SubscriptionRef.make<Option.Option<RpcSession>>(
    connected ? Option.some({} as RpcSession) : Option.none(),
  );
  const supervisor = {
    prepared,
    session,
  } as EnvironmentSupervisor.EnvironmentSupervisor["Service"];
  const environmentRegistry = {
    followStream: (_environmentId, stream) =>
      Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
  } as EnvironmentRegistry.EnvironmentRegistry["Service"];
  let requests = 0;
  const fetchFn: typeof fetch = async () => {
    requests++;
    return reply();
  };
  const runtime = Atom.runtime(
    Layer.merge(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
      RpcHttp.layerRemoteHttpClient(fetchFn),
    ),
  );
  const atoms = createEnvironmentSessionAtoms(runtime);
  const atom = atoms.sessionStateAtom(TARGET.environmentId);
  const permissions = createCommandPermissions(runtime, WS_METHODS.scheduledTasksDelete);
  const permission = permissions.permissionAtom(TARGET.environmentId);
  const registry = AtomRegistry.make();
  registry.mount(permission);
  yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
  const read = AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
  return { registry, atom, read, permission, prepared, session, requests: () => requests };
});

describe("environment session permissions", () => {
  it.effect("recovers from a failed lookup while the socket stays connected", () =>
    Effect.gen(function* () {
      let online = false;
      const h = yield* makeHarness(() => {
        if (!online) throw new TypeError("Network request failed");
        return Response.json(SESSION);
      });
      expect((yield* h.read.pipe(Effect.flip))._tag).toBe("RemoteEnvironmentAuthFetchError");
      expect(h.registry.get(h.permission)).toBe(false);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5_000));
      expect(h.requests()).toBe(2);
      expect(h.registry.get(h.permission)).toBe(false);
      online = true;
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5_000));

      expect(h.registry.get(h.atom)).toMatchObject({ _tag: "Success", value: SESSION });
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(3);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(30_000));
      expect(h.requests()).toBe(3);
    }).pipe(Effect.scoped),
  );

  it.effect("reloads permissions when a socket reconnects with the same prepared connection", () =>
    Effect.gen(function* () {
      let online = false;
      const h = yield* makeHarness(() => {
        if (!online) throw new TypeError("Network request failed");
        return Response.json(SESSION);
      });
      yield* h.read.pipe(Effect.flip);
      online = true;
      yield* SubscriptionRef.set(h.session, Option.some({} as RpcSession));
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));

      expect(h.registry.get(h.atom)).toMatchObject({ _tag: "Success", value: SESSION });
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("waits for socket authentication before loading permissions", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness(() => Response.json(SESSION), false);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
      expect(h.requests()).toBe(0);
      expect(h.registry.get(h.permission)).toBe(false);

      yield* SubscriptionRef.set(h.session, Option.some({} as RpcSession));
      expect(yield* h.read).toEqual(SESSION);
      expect(h.requests()).toBe(1);
      expect(h.registry.get(h.permission)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("recovers a timed-out lookup without restarting the app", () =>
    Effect.gen(function* () {
      let online = false;
      const requested = Promise.withResolvers<void>();
      const pending = Promise.withResolvers<Response>();
      const h = yield* makeHarness(() => {
        requested.resolve();
        return online ? Response.json(SESSION) : pending.promise;
      });
      const lookup = yield* h.read.pipe(Effect.flip, Effect.forkChild);
      yield* Effect.promise(() => requested.promise);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(6_000));
      expect((yield* Fiber.join(lookup))._tag).toBe("RemoteEnvironmentAuthTimeoutError");
      expect(h.registry.get(h.permission)).toBe(false);

      online = true;
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5_000));
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(2);
      pending.resolve(Response.json(SESSION));
    }).pipe(Effect.scoped),
  );

  it.effect("keeps permission revocations when the session is explicitly refreshed", () =>
    Effect.gen(function* () {
      let granted = true;
      const h = yield* makeHarness(() =>
        Response.json(granted ? SESSION : { ...SESSION, permissions: [] }),
      );
      yield* h.read;
      expect(h.registry.get(h.permission)).toBe(true);

      granted = false;
      h.registry.refresh(h.atom);
      yield* h.read;
      expect(h.registry.get(h.permission)).toBe(false);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(30_000));
      expect(h.requests()).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect.each([
    {
      name: "an unavailable endpoint",
      response: () => new Response("Unavailable", { status: 503 }),
    },
    {
      name: "an internal server error",
      response: () =>
        Response.json(
          {
            _tag: "EnvironmentInternalError",
            code: "internal_error",
            reason: "internal_error",
            traceId: "trace-internal",
          },
          { status: 500 },
        ),
    },
  ])("recovers after $name", ({ response }) =>
    Effect.gen(function* () {
      let healthy = false;
      const h = yield* makeHarness(() => (healthy ? Response.json(SESSION) : response()));
      yield* h.read.pipe(Effect.flip);
      healthy = true;
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5_000));
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect.each([
    {
      name: "expired credentials",
      status: 401,
      error: {
        _tag: "EnvironmentAuthInvalidError",
        code: "auth_invalid",
        reason: "invalid_credential",
        traceId: "trace-auth",
      },
    },
    {
      name: "a permission denial",
      status: 403,
      error: {
        _tag: "EnvironmentScopeRequiredError",
        code: "insufficient_scope",
        requiredScope: "orchestration:read",
        traceId: "trace-scope",
      },
    },
  ])("does not retry $name as a network failure", ({ status, error }) =>
    Effect.gen(function* () {
      const h = yield* makeHarness(() => Response.json(error, { status }));
      expect(yield* h.read.pipe(Effect.flip)).toMatchObject({
        _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
        status,
      });
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(30_000));
      expect(h.requests()).toBe(1);
      expect(h.registry.get(h.permission)).toBe(false);
    }).pipe(Effect.scoped),
  );
});
