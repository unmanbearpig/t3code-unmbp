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
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import {
  ConnectionBlockedError,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as RemoteAuthorization from "../authorization/service.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
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
  reply: (init: RequestInit) => Response | Promise<Response>,
  connected = true,
  dpop = false,
) {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  yield* Effect.addFinalizer(() => Effect.sync(() => vi.useRealTimers()));
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
    Option.some(
      dpop
        ? {
            ...PREPARED,
            target: new RelayConnectionTarget({
              environmentId: TARGET.environmentId,
              label: TARGET.label,
            }),
            httpAuthorization: {
              _tag: "Dpop",
              accessToken: "test-token",
              expiresAtEpochMs: 3_600_000,
            },
          }
        : PREPARED,
    ),
  );
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
  const fetchFn: typeof fetch = async (_request, init) => {
    requests++;
    return reply(init ?? {});
  };
  const relay = { blocked: false, authorizations: 0 };
  const runtime = Atom.runtime(
    Layer.mergeAll(
      Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
      RpcHttp.layerRemoteHttpClient(fetchFn),
      Layer.succeed(RemoteAuthorization.RemoteEnvironmentAuthorization, {
        authorizeBearer: () => Effect.die("Unexpected bearer preparation"),
        authorizeDpop: () => Effect.die("Unexpected socket preparation"),
        authorizeDpopHttp: () =>
          Effect.suspend(() => {
            relay.authorizations++;
            return relay.blocked
              ? Effect.fail(
                  new ConnectionBlockedError({ reason: "permission", detail: "Access revoked" }),
                )
              : Effect.succeed({
                  environmentId: TARGET.environmentId,
                  label: TARGET.label,
                  httpBaseUrl: PREPARED.httpBaseUrl,
                  httpAuthorization: {
                    _tag: "Dpop" as const,
                    accessToken: "test-token",
                    expiresAtEpochMs: 3_600_000,
                  },
                });
          }),
      }),
      Layer.succeed(ManagedRelay.ManagedRelayDpopSigner, {
        thumbprint: Effect.succeed("test-thumbprint"),
        createProof: () => Effect.succeed("test-proof"),
      }),
    ),
  );
  const atoms = createEnvironmentSessionAtoms(runtime);
  const atom = atoms.sessionStateAtom(TARGET.environmentId);
  const permissions = createCommandPermissions(runtime, WS_METHODS.scheduledTasksDelete);
  const permission = permissions.permissionAtom(TARGET.environmentId);
  const registry = AtomRegistry.make();
  const unmount = registry.mount(permission);
  yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
  const read = AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
  const authorize = permissions.authorize(registry, TARGET.environmentId);
  return {
    registry,
    atom,
    read,
    permission,
    prepared,
    session,
    authorize,
    relay,
    unmount,
    requests: () => requests,
  };
});

describe("environment session permissions", () => {
  it.effect("caps retry delays and stops the retry worker when the registry is disposed", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness(() => {
        throw new TypeError("Offline");
      });
      yield* h.read.pipe(Effect.flip);
      for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
        const requests = h.requests();
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(delay - 1));
        expect(h.requests()).toBe(requests);
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(1));
        expect(h.requests()).toBe(requests + 1);
      }
      h.registry.dispose();
      const requests = h.requests();
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(60_000));
      expect(h.requests()).toBe(requests);
    }).pipe(Effect.scoped),
  );

  it.effect("ends unobserved retries after the session cache expires", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness(() => {
        throw new TypeError("Offline");
      });
      yield* h.read.pipe(Effect.flip);
      h.unmount();
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(5 * 60_000 + 1_000));
      const requests = h.requests();
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(60_000));
      expect(h.requests()).toBe(requests);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "cancels an obsolete request and ignores its late response after an endpoint changes",
    () =>
      Effect.gen(function* () {
        const requested = Promise.withResolvers<void>();
        const aborted = Promise.withResolvers<void>();
        const obsolete = Promise.withResolvers<Response>();
        let hold = false;
        let replaced = false;
        const h = yield* makeHarness((init) => {
          if (replaced) return Response.json({ ...SESSION, permissions: [] });
          if (!hold) return Response.json(SESSION);
          init.signal?.addEventListener("abort", () => aborted.resolve(), { once: true });
          requested.resolve();
          return obsolete.promise;
        });
        yield* h.read;
        hold = true;
        h.registry.refresh(h.atom);
        yield* Effect.promise(() => requested.promise);
        replaced = true;
        yield* SubscriptionRef.set(
          h.prepared,
          Option.some({ ...PREPARED, httpBaseUrl: "https://new.test" }),
        );
        yield* Effect.promise(() => aborted.promise);
        yield* h.read;
        obsolete.resolve(Response.json(SESSION));
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.registry.get(h.permission)).toBe(false);
        expect(h.requests()).toBe(3);
      }).pipe(Effect.scoped),
  );

  it.effect("does not restore a rejected grant when a later refresh fails transiently", () =>
    Effect.gen(function* () {
      let mode = "healthy";
      const h = yield* makeHarness(() => {
        if (mode === "offline") throw new TypeError("Network request failed");
        return mode === "rejected"
          ? new Response("Unauthorized", { status: 401 })
          : Response.json(SESSION);
      });
      yield* h.read;
      mode = "rejected";
      h.registry.refresh(h.atom);
      yield* h.read.pipe(Effect.flip);
      expect(h.registry.get(h.permission)).toBe(false);
      mode = "offline";
      h.registry.refresh(h.atom);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
      expect(h.registry.get(h.permission)).toBe(false);
      expect((yield* h.authorize.pipe(Effect.flip))._tag).toBe("EnvironmentAuthorizationError");
      mode = "healthy";
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(1_000));
      expect(h.registry.get(h.permission)).toBe(true);
    }).pipe(Effect.scoped),
  );
  it.effect("recovers from a failed lookup while the socket stays connected", () =>
    Effect.gen(function* () {
      let online = false;
      const h = yield* makeHarness(() => {
        if (!online) throw new TypeError("Network request failed");
        return Response.json(SESSION);
      });
      expect((yield* h.read.pipe(Effect.flip))._tag).toBe("RemoteEnvironmentAuthFetchError");
      expect(h.registry.get(h.permission)).toBe(false);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(1_000));
      expect(h.requests()).toBe(2);
      expect(h.registry.get(h.permission)).toBe(false);
      online = true;
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(2_000));

      expect(h.registry.get(h.atom)).toMatchObject({ _tag: "Success", value: SESSION });
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(3);
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(30_000));
      expect(h.requests()).toBe(3);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps confirmed permissions and command dispatch available through a transient refresh failure",
    () =>
      Effect.gen(function* () {
        let healthy = true;
        const h = yield* makeHarness(() => {
          if (!healthy) throw new TypeError("Network request failed");
          return Response.json(SESSION);
        });
        yield* h.read;
        healthy = false;
        h.registry.refresh(h.atom);
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.registry.get(h.atom)).toMatchObject({ _tag: "Success", waiting: true });
        expect(h.registry.get(h.permission)).toBe(true);
        yield* h.authorize;

        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(3_000));
        expect(h.requests()).toBe(4);
        healthy = true;
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(4_000));
        expect(h.registry.get(h.atom)).toMatchObject({ _tag: "Success", waiting: false });
        expect(h.registry.get(h.permission)).toBe(true);
      }).pipe(Effect.scoped),
  );

  it.effect.each(["socket", "credential", "endpoint"] as const)(
    "does not reuse grants while a changed %s is being confirmed",
    (change) =>
      Effect.gen(function* () {
        const response = Promise.withResolvers<Response>();
        let refreshing = false;
        const h = yield* makeHarness(() =>
          refreshing ? response.promise : Response.json(SESSION),
        );
        yield* h.read;
        refreshing = true;
        if (change === "socket")
          yield* SubscriptionRef.set(h.session, Option.some({} as RpcSession));
        else
          yield* SubscriptionRef.set(
            h.prepared,
            Option.some({
              ...PREPARED,
              ...(change === "credential"
                ? { httpAuthorization: { _tag: "Bearer" as const, token: "new-token" } }
                : { httpBaseUrl: "https://other-environment.test" }),
            }),
          );
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(0));
        expect(h.registry.get(h.permission)).toBe(false);
        expect(Option.isNone(AsyncResult.value(h.registry.get(h.atom)))).toBe(true);
        const authorization = yield* h.authorize.pipe(Effect.result, Effect.forkChild);
        response.resolve(Response.json({ ...SESSION, permissions: [] }));
        expect((yield* Fiber.join(authorization))._tag).toBe("Failure");
        expect(h.registry.get(h.permission)).toBe(false);
      }).pipe(Effect.scoped),
  );

  it.effect.each([
    {
      name: "an interrupted response body",
      response: () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.error(new TypeError("Connection reset"));
            },
          }),
          { headers: { "content-type": "application/json" } },
        ),
    },
    {
      name: "a malformed response",
      response: () => new Response("{", { headers: { "content-type": "application/json" } }),
    },
  ])("recovers from $name on the initial lookup", ({ response }) =>
    Effect.gen(function* () {
      let healthy = false;
      const h = yield* makeHarness(() => (healthy ? Response.json(SESSION) : response()));
      expect((yield* h.read.pipe(Effect.flip))._tag).toBe("RemoteEnvironmentAuthInvalidJsonError");
      healthy = true;
      yield* Effect.promise(() => vi.advanceTimersByTimeAsync(1_000));
      expect(h.registry.get(h.permission)).toBe(true);
      expect(h.requests()).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect.each(["relay rejection", "renewed credential rejection"])(
    "drops confirmed grants and stops retrying after %s",
    (failure) =>
      Effect.gen(function* () {
        let accepted = true;
        const h = yield* makeHarness(
          () => Response.json(accepted ? SESSION : { authenticated: false, auth: SESSION.auth }),
          true,
          true,
        );
        yield* h.read;
        if (failure === "relay rejection") h.relay.blocked = true;
        else accepted = false;
        h.registry.refresh(h.atom);
        expect((yield* h.read.pipe(Effect.flip))._tag).toBe("RemoteEnvironmentAuthBlockedError");
        expect(h.registry.get(h.permission)).toBe(false);
        expect((yield* h.authorize.pipe(Effect.flip))._tag).toBe("EnvironmentAuthorizationError");
        const authorizations = h.relay.authorizations;
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(60_000));
        expect(h.relay.authorizations).toBe(authorizations);
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
