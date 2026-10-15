import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import * as Socket from "effect/socket/Socket";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as TokenStore from "../authorization/tokenStore.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import * as RpcHttp from "../rpc/http.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import * as RpcSession from "../rpc/session.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  type ConnectionCatalogEntry,
  type ConnectionRoute,
} from "./catalog.ts";
import * as Connectivity from "./connectivity.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import * as ConnectionDriver from "./driver.ts";
import { BearerConnectionTarget, type SupervisorConnectionState } from "./model.ts";
import * as ConnectionProfileStore from "./profileStore.ts";
import * as ConnectionResolver from "./resolver.ts";
import * as EnvironmentSupervisor from "./supervisor.ts";
import * as ConnectionWakeups from "./wakeups.ts";

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");

function bearerRoute(connectionId: string, httpBaseUrl: string): ConnectionRoute {
  const target = new BearerConnectionTarget({
    environmentId: ENVIRONMENT_ID,
    label: "Test environment",
    connectionId,
  });
  return {
    target,
    profile: Option.some(
      new BearerConnectionProfile({
        connectionId,
        environmentId: target.environmentId,
        label: target.label,
        httpBaseUrl,
        wsBaseUrl: httpBaseUrl.replace(/^http/, "ws"),
      }),
    ),
  };
}

const PREFERRED_ROUTE = bearerRoute("bearer:preferred", "http://192.168.1.10:3773");
const FALLBACK_ROUTE = bearerRoute("bearer:fallback", "https://fallback.example.test");
const ENTRY: ConnectionCatalogEntry = {
  ...PREFERRED_ROUTE,
  alternateRoutes: [FALLBACK_ROUTE],
  enabled: true,
};

const makeHarness = Effect.fn("TestRoutePreflight.makeHarness")(function* (options?: {
  readonly sessions?: RpcSession.RpcSessionFactory["Service"];
}) {
  let preferredReachable = true;
  let preferredAccepted = false;
  let preferredEnvironmentId = ENVIRONMENT_ID;
  let socketCount = 0;
  const requests: Array<Request> = [];
  const constructSocket = () => {
    socketCount += 1;
    throw new Error("Preflight must not construct a socket.");
  };
  const fetchFn = ((input, init) => {
    const request = new Request(input, init);
    requests.push(request);
    const url = new URL(request.url);
    const preferred = url.hostname === "192.168.1.10";
    if (preferred && !preferredReachable) {
      return Promise.reject(new Error("Preferred route is unreachable."));
    }
    switch (url.pathname) {
      case "/.well-known/t3/environment":
        return Promise.resolve(
          Response.json({
            environmentId: preferred ? preferredEnvironmentId : ENVIRONMENT_ID,
            label: "Test environment",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.0-test",
            orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
            capabilities: { repositoryIdentity: true },
          }),
        );
      case "/api/auth/websocket-ticket":
        return Promise.resolve(
          preferred && !preferredAccepted
            ? Response.json(
                {
                  _tag: "EnvironmentAuthInvalidError",
                  code: "auth_invalid",
                  reason: "invalid_credential",
                  traceId: "trace-revoked-preferred-bearer",
                },
                { status: 401 },
              )
            : Response.json({ ticket: "valid-ticket", expiresAt: "2026-10-15T01:00:00Z" }),
        );
      default:
        return Promise.reject(new Error(`Unexpected request to ${request.url}`));
    }
  }) satisfies typeof fetch;
  const unused = () => Effect.die("Unexpected operation in bearer route preflight.");
  const infrastructure = Layer.mergeAll(
    RpcHttp.layerRemoteHttpClient(fetchFn),
    Layer.succeed(Socket.WebSocketConstructor, constructSocket),
    Layer.succeed(ClientCapabilities.WebSocketConstructorWithHeaders, constructSocket),
    Layer.succeed(ClientCapabilities.ClientPresentation, {
      metadata: { label: "Test phone", deviceType: "mobile", surface: "mobile", os: "ios" },
    }),
    Layer.succeed(ClientCapabilities.PrimaryEnvironmentAuth, { bearerToken: Effect.succeedNone }),
    Layer.succeed(ClientCapabilities.CloudSession, {
      identity: Effect.succeedNone,
      clerkToken: unused(),
    }),
    Layer.succeed(ClientCapabilities.RelayDeviceIdentity, { deviceId: Effect.succeedNone }),
    Layer.succeed(ClientCapabilities.SshEnvironmentGateway, {
      provision: unused,
      prepare: unused,
      disconnect: unused,
    }),
    Layer.succeed(ConnectionProfileStore.ConnectionProfileStore, {
      get: unused,
      put: unused,
      remove: unused,
    }),
    Layer.succeed(ConnectionCredentialStore.ConnectionCredentialStore, {
      get: (connectionId) =>
        Effect.succeedSome(new BearerConnectionCredential({ token: `${connectionId}-token` })),
      put: unused,
      remove: unused,
    }),
    TokenStore.layer({ get: unused, put: unused, remove: unused }),
    Layer.succeed(ManagedRelay.ManagedRelayDpopSigner, {
      thumbprint: unused(),
      createProof: unused,
    }),
    Layer.succeed(ManagedRelay.ManagedRelayClient, {
      relayUrl: "https://relay.example.test",
      listEnvironments: unused,
      listDevices: unused,
      createEnvironmentLinkChallenge: unused,
      linkEnvironment: unused,
      unlinkEnvironment: unused,
      getEnvironmentStatus: unused,
      connectEnvironment: unused,
      registerDevice: unused,
      unregisterDevice: unused,
      registerLiveActivity: unused,
      getAgentActivitySnapshot: unused,
      resetTokenCache: unused(),
    }),
  );
  const authorization = RemoteEnvironmentAuthorization.layer.pipe(
    Layer.provideMerge(infrastructure),
  );
  const resolver = ConnectionResolver.layer.pipe(Layer.provideMerge(authorization));
  const sessions =
    options?.sessions === undefined
      ? RpcSession.layer({}).pipe(Layer.provide(infrastructure))
      : Layer.succeed(RpcSession.RpcSessionFactory, options.sessions);
  const driver = yield* ConnectionDriver.make.pipe(Effect.provide(Layer.merge(resolver, sessions)));
  return {
    driver,
    resolver: yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(resolver)),
    requests,
    socketCount: () => socketCount,
    ticketRequests: () =>
      requests.filter((request) => new URL(request.url).pathname === "/api/auth/websocket-ticket"),
    setPreferredReachable: (value: boolean) => Effect.sync(() => (preferredReachable = value)),
    setPreferredAccepted: (value: boolean) => Effect.sync(() => (preferredAccepted = value)),
    setPreferredEnvironmentId: (value: EnvironmentId) =>
      Effect.sync(() => (preferredEnvironmentId = value)),
  };
});

function awaitGeneration(
  state: SubscriptionRef.SubscriptionRef<SupervisorConnectionState>,
  generation: number,
) {
  return SubscriptionRef.changes(state).pipe(
    Stream.filter((value) => value.phase === "connected" && value.generation === generation),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
}

describe("preferred bearer route preflight", () => {
  it.effect("updates through the valid fallback when the first route bearer is revoked", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const selected = yield* Effect.firstSuccessOf(
        [PREFERRED_ROUTE, FALLBACK_ROUTE].map((route) =>
          harness.resolver.prepareForUpdate({
            ...ENTRY,
            target: route.target,
            profile: route.profile,
          }),
        ),
      );
      expect(selected.prepared.target).toEqual(FALLBACK_ROUTE.target);
      expect(harness.ticketRequests()).toHaveLength(2);
      expect(harness.socketCount()).toBe(0);
    }),
  );
  it.effect(
    "rejects a revoked bearer after a correct public descriptor without opening a socket",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness();

        expect(yield* harness.driver.preflight(ENTRY, PREFERRED_ROUTE)).toBe(false);

        const [ticket] = harness.ticketRequests();
        expect(ticket?.method).toBe("POST");
        expect(ticket?.headers.get("authorization")).toBe("Bearer bearer:preferred-token");
        expect(harness.requests[0]?.headers.get("authorization")).toBeNull();
        expect(harness.socketCount()).toBe(0);
      }),
  );

  it.effect("does not send a bearer to a preferred route serving another environment", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.setPreferredEnvironmentId(EnvironmentId.make("another-environment"));

      expect(yield* harness.driver.preflight(ENTRY, PREFERRED_ROUTE)).toBe(false);
      expect(harness.ticketRequests()).toHaveLength(0);
      expect(harness.requests.every((request) => !request.headers.has("authorization"))).toBe(true);
      expect(harness.socketCount()).toBe(0);
    }),
  );

  it.effect.each([
    {
      accepted: false,
      scenario: "keeps the healthy fallback session when the preferred bearer is revoked",
    },
    {
      accepted: true,
      scenario: "promotes a valid preferred bearer route while native reconnects stay ticketless",
    },
  ])("$scenario", ({ accepted }) =>
    Effect.gen(function* () {
      const sessionCount = yield* Ref.make(0);
      const releaseCount = yield* Ref.make(0);
      const preflightCompleted = yield* Deferred.make<boolean>();
      const harness = yield* makeHarness({
        sessions: {
          connect: () =>
            Effect.acquireRelease(
              Ref.update(sessionCount, (count) => count + 1).pipe(
                Effect.as({
                  client: {} as WsRpcProtocolClient,
                  initialConfig: Effect.die("Config is not read in this scenario."),
                  subscribeServerConfig: () => {
                    throw new Error("Config is not subscribed in this scenario.");
                  },
                  ready: Effect.void,
                  probe: Effect.void,
                  closed: Effect.never,
                }),
              ),
              () => Ref.update(releaseCount, (count) => count + 1),
            ),
        },
      });
      yield* harness.setPreferredReachable(false);
      const supervisor = yield* EnvironmentSupervisor.make(ENTRY, {
        initiallyDesired: true,
      }).pipe(
        Effect.provideService(ConnectionDriver.ConnectionDriver, {
          ...harness.driver,
          preflight: (entry, route) =>
            harness.driver
              .preflight(entry, route)
              .pipe(Effect.tap((passed) => Deferred.succeed(preflightCompleted, passed))),
        }),
        Effect.provideService(Connectivity.Connectivity, {
          status: Effect.succeed("online" as const),
          changes: Stream.never,
        }),
        Effect.provideService(ConnectionWakeups.ConnectionWakeups, { changes: Stream.never }),
      );
      const before = yield* awaitGeneration(supervisor.state, 1);
      const session = Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session));
      expect(Option.getOrThrow(yield* SubscriptionRef.get(supervisor.prepared)).target).toEqual(
        FALLBACK_ROUTE.target,
      );
      expect(harness.ticketRequests()).toHaveLength(0);

      yield* harness.setPreferredReachable(true);
      yield* harness.setPreferredAccepted(accepted);
      yield* TestClock.adjust("60 seconds");
      const passed = yield* Deferred.await(preflightCompleted);

      if (accepted) {
        yield* awaitGeneration(supervisor.state, 2);
        expect(Option.getOrThrow(yield* SubscriptionRef.get(supervisor.prepared)).target).toEqual(
          PREFERRED_ROUTE.target,
        );
        expect(Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session))).not.toBe(session);
        expect(yield* Ref.get(sessionCount)).toBe(2);
        expect(yield* Ref.get(releaseCount)).toBe(1);
      } else {
        // Drain the current virtual instant after the rejected preflight completes.
        yield* TestClock.adjust(0);
        expect(yield* SubscriptionRef.get(supervisor.state)).toEqual(before);
        expect(Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session))).toBe(session);
        expect(yield* Ref.get(sessionCount)).toBe(1);
        expect(yield* Ref.get(releaseCount)).toBe(0);
      }
      expect(passed).toBe(accepted);
      expect(harness.ticketRequests()).toHaveLength(1);
      expect(harness.socketCount()).toBe(0);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});
