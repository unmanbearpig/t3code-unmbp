import {
  AuthAdministrativeScopes,
  AuthStandardClientScopes,
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_VERSION,
} from "@t3tools/contracts";
import {
  RelayEnvironmentConnectScope,
  type RelayEnvironmentConnectResponse,
} from "@t3tools/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  SshConnectionProfile,
  type ConnectionCatalogEntry,
  type ConnectionRoute,
} from "../connection/catalog.ts";
import * as ConnectionCredentialStore from "../connection/credentialStore.ts";
import * as ConnectionDriver from "../connection/driver.ts";
import * as ConnectionProfileStore from "../connection/profileStore.ts";
import * as ConnectionResolver from "../connection/resolver.ts";
import {
  BearerConnectionTarget,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as RpcSession from "../rpc/session.ts";
import { DPOP_UNKNOWN_HINT } from "../relay/errorPresentation.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import * as RpcHttp from "../rpc/http.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as RemoteEnvironmentAuthorization from "./service.ts";
import * as TokenStore from "./tokenStore.ts";

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");
const ENDPOINT = {
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
  providerKind: "cloudflare_tunnel" as const,
};
const DESCRIPTOR = {
  environmentId: ENVIRONMENT_ID,
  label: "Remote environment",
  platform: {
    os: "linux",
    arch: "x64",
  },
  serverVersion: "0.0.0-test",
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  capabilities: {
    repositoryIdentity: true,
  },
};
const BOOTSTRAP: RelayEnvironmentConnectResponse = {
  environmentId: ENVIRONMENT_ID,
  endpoint: ENDPOINT,
  credential: "relay-bootstrap",
  expiresAt: "2026-06-06T01:00:00.000Z",
};

type RecordedResponse = Response | ((init: RequestInit) => Response);

function recordedFetch(responses: ReadonlyArray<RecordedResponse>) {
  const calls: Array<readonly [RequestInfo | URL, RequestInit]> = [];
  let responseIndex = 0;
  const fetchFn = ((input, init) => {
    calls.push([input, init ?? {}]);
    const response = responses[responseIndex++];
    return response === undefined
      ? Promise.reject(new Error(`Unexpected fetch call to ${String(input)}`))
      : Promise.resolve(typeof response === "function" ? response(init ?? {}) : response);
  }) satisfies typeof fetch;
  return { calls, fetchFn };
}

const websocketTicket = (ticket: string) =>
  Response.json({
    ticket,
    expiresAt: "2026-06-06T01:00:00.000Z",
  });

const layerWebSocketWithHeaders = Layer.succeed(
  ClientCapabilities.WebSocketConstructorWithHeaders,
  () => {
    throw new Error("Authorization must not open the WebSocket.");
  },
);

const accessToken = (token: string, scope = AuthStandardClientScopes.join(" ")) =>
  Response.json({
    access_token: token,
    issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
    token_type: "DPoP",
    expires_in: 3_600,
    scope,
  });

const authInvalid = () =>
  Response.json(
    {
      _tag: "EnvironmentAuthInvalidError",
      code: "auth_invalid",
      reason: "invalid_credential",
      traceId: "trace-auth-invalid",
    },
    { status: 401 },
  );

const persistedToken = (
  input: {
    readonly environmentId?: EnvironmentId;
    readonly accessToken?: string;
    readonly expiresAtEpochMs?: number;
    readonly dpopThumbprint?: string;
  } = {},
) =>
  new TokenStore.RemoteDpopAccessToken({
    environmentId: input.environmentId ?? ENVIRONMENT_ID,
    accountId: "account-1",
    label: DESCRIPTOR.label,
    endpoint: ENDPOINT,
    accessToken: input.accessToken ?? "cached-access-token",
    expiresAtEpochMs: input.expiresAtEpochMs ?? Number.MAX_SAFE_INTEGER,
    dpopThumbprint: input.dpopThumbprint ?? "thumbprint-1",
  });

const makeHarness = Effect.fn("TestRemoteAuthorization.makeHarness")(function* (input: {
  readonly initialToken?: TokenStore.RemoteDpopAccessToken;
  readonly responses: ReadonlyArray<RecordedResponse>;
  readonly bootstrap?: RelayEnvironmentConnectResponse;
  readonly beforeBootstrap?: Effect.Effect<void, ManagedRelay.ManagedRelayClientError>;
  readonly beforePut?: Effect.Effect<void>;
  readonly clerkToken?: ClientCapabilities.CloudSession["Service"]["clerkToken"];
  readonly httpClient?: Layer.Layer<HttpClient.HttpClient>;
}) {
  const tokens = yield* Ref.make(
    new Map(
      input.initialToken === undefined
        ? []
        : [[input.initialToken.environmentId, input.initialToken]],
    ),
  );
  const bootstrapCalls = yield* Ref.make(0);
  const relayInputs = yield* Ref.make<
    ReadonlyArray<Parameters<ManagedRelay.ManagedRelayClient["Service"]["connectEnvironment"]>[0]>
  >([]);
  const session = yield* Ref.make<Option.Option<ClientCapabilities.CloudSessionIdentity>>(
    Option.some({ accountId: "account-1" }),
  );
  const thumbprint = yield* Ref.make("thumbprint-1");
  const tokenReads = yield* Queue.unbounded<EnvironmentId>();
  const proofInputs = yield* Ref.make<
    ReadonlyArray<{
      readonly method: string;
      readonly url: string;
      readonly accessToken?: string;
    }>
  >([]);
  const fetch = recordedFetch(input.responses);

  const tokenStore = TokenStore.RemoteDpopAccessTokenStore.of({
    get: (environmentId) =>
      Ref.get(tokens).pipe(
        Effect.map((current) => Option.fromUndefinedOr(current.get(environmentId))),
        Effect.tap(() => Queue.offer(tokenReads, environmentId)),
      ),
    put: (token) =>
      (input.beforePut ?? Effect.void).pipe(
        Effect.andThen(
          Ref.update(tokens, (current) => {
            const next = new Map(current);
            next.set(token.environmentId, token);
            return next;
          }),
        ),
      ),
    remove: (environmentId) =>
      Ref.update(tokens, (current) => {
        const next = new Map(current);
        next.delete(environmentId);
        return next;
      }),
  });
  const signer = ManagedRelay.ManagedRelayDpopSigner.of({
    thumbprint: Ref.get(thumbprint),
    createProof: (proofInput) =>
      Ref.update(proofInputs, (current) => [...current, proofInput]).pipe(
        Effect.as(`proof:${proofInput.url}`),
      ),
  });
  const unexpected = () => Effect.die("Unexpected relay request");
  const relay = ManagedRelay.ManagedRelayClient.of({
    relayUrl: "https://relay.example.test",
    listEnvironments: unexpected,
    listDevices: unexpected,
    createEnvironmentLinkChallenge: unexpected,
    linkEnvironment: unexpected,
    unlinkEnvironment: unexpected,
    getEnvironmentStatus: unexpected,
    connectEnvironment: (request) =>
      Effect.gen(function* () {
        yield* Ref.update(bootstrapCalls, (count) => count + 1);
        yield* Ref.update(relayInputs, (current) => [...current, request]);
        yield* input.beforeBootstrap ?? Effect.void;
        return input.bootstrap ?? BOOTSTRAP;
      }),
    registerDevice: unexpected,
    unregisterDevice: unexpected,
    registerLiveActivity: unexpected,
    getAgentActivitySnapshot: unexpected,
    resetTokenCache: Effect.void,
  });
  const dependencies = Layer.mergeAll(
    input.httpClient ?? RpcHttp.layerRemoteHttpClient(fetch.fetchFn),
    Layer.succeed(ManagedRelay.ManagedRelayDpopSigner, signer),
    Layer.succeed(ManagedRelay.ManagedRelayClient, relay),
    Layer.succeed(ClientCapabilities.CloudSession, {
      identity: Ref.get(session),
      clerkToken: input.clerkToken ?? Effect.succeed("clerk-session"),
    }),
    Layer.succeed(ClientCapabilities.RelayDeviceIdentity, {
      deviceId: Effect.succeedSome("device-1"),
    }),
    Layer.succeed(TokenStore.RemoteDpopAccessTokenStore, tokenStore),
    Layer.succeed(
      ClientCapabilities.ClientPresentation,
      ClientCapabilities.ClientPresentation.of({
        metadata: {
          label: "T3 Code Test",
          deviceType: "mobile",
          os: "test",
        },
      }),
    ),
  );
  const layer = RemoteEnvironmentAuthorization.layer.pipe(Layer.provide(dependencies));
  return {
    layer,
    dependencies,
    tokens,
    bootstrapCalls,
    proofInputs,
    fetch,
    relayInputs,
    session,
    thumbprint,
    tokenReads,
  };
});

function bearerRoute(connectionId: string, httpBaseUrl = ENDPOINT.httpBaseUrl): ConnectionRoute {
  return {
    target: new BearerConnectionTarget({
      environmentId: ENVIRONMENT_ID,
      label: "Saved",
      connectionId,
    }),
    profile: Option.some(
      new BearerConnectionProfile({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId,
        httpBaseUrl,
        wsBaseUrl: httpBaseUrl.replace(/^http/, "ws"),
      }),
    ),
  };
}

function connectionEntry(routes: ReadonlyArray<ConnectionRoute>): ConnectionCatalogEntry {
  const [first, ...alternateRoutes] = routes;
  if (first === undefined) throw new Error("A test connection needs a route.");
  return { ...first, enabled: true, alternateRoutes };
}

const SSH_TARGET = { alias: "test", hostname: "ssh.example.test", username: "test", port: 22 };

const makeConnectionHarness = Effect.fn("TestConnectionDescriptors.makeHarness")(function* (
  input: Parameters<typeof makeHarness>[0] & {
    readonly primaryBearerToken?: string;
    readonly skipDescriptorOnHttpsReconnect?: boolean;
  },
) {
  const harness = yield* makeHarness(input);
  const opened: Array<PreparedConnection> = [];
  const dependencies = Layer.mergeAll(
    harness.dependencies,
    input.skipDescriptorOnHttpsReconnect === undefined
      ? Layer.empty
      : Layer.succeed(ClientCapabilities.ExperimentalReconnectOptions, {
          skipDescriptorOnHttpsReconnect: input.skipDescriptorOnHttpsReconnect,
        }),
    Layer.succeed(ConnectionCredentialStore.ConnectionCredentialStore, {
      get: () => Effect.succeedSome(new BearerConnectionCredential({ token: "bearer-token" })),
      put: () => Effect.void,
      remove: () => Effect.void,
    }),
    Layer.succeed(ConnectionProfileStore.ConnectionProfileStore, {
      get: () => Effect.succeedNone,
      put: () => Effect.void,
      remove: () => Effect.void,
    }),
    Layer.succeed(ClientCapabilities.PrimaryEnvironmentAuth, {
      bearerToken: Effect.succeed(Option.fromUndefinedOr(input.primaryBearerToken)),
    }),
    Layer.succeed(ClientCapabilities.SshEnvironmentGateway, {
      provision: () => Effect.die("unused"),
      prepare: () =>
        Effect.succeed({
          bootstrap: { ...ENDPOINT, target: SSH_TARGET, pairingToken: null },
          bearerToken: "ssh-bearer",
        }),
      disconnect: () => Effect.void,
    }),
    Layer.succeed(RpcSession.RpcSessionFactory, {
      connect: (prepared) =>
        Effect.sync(() => {
          opened.push(prepared);
          return {
            client: {} as RpcSession.RpcSession["client"],
            initialConfig: Effect.die("unused"),
            subscribeServerConfig: () => Stream.die("unused"),
            ready: Effect.void,
            probe: Effect.void,
            closed: Effect.never,
          };
        }),
    }),
  );
  const layer = ConnectionDriver.layer.pipe(
    Layer.provideMerge(ConnectionResolver.layer),
    Layer.provideMerge(harness.layer),
    Layer.provide(dependencies),
  );
  return { ...harness, layer, opened };
});

function requestSequence(harness: {
  readonly fetch: Pick<ReturnType<typeof recordedFetch>, "calls">;
}) {
  return harness.fetch.calls.map(([url, init]) => `${init.method} ${String(url)}`);
}

describe("Connection descriptor reuse", () => {
  it.effect.each([true, false])(
    "tries a slow route last and reuses or cancels its pending descriptor when fallback succeeds: %s",
    (fallbackSucceeds) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const fallbackTried = yield* Deferred.make<void>();
        const finished = yield* Deferred.make<void>();
        const calls: Array<string> = [];
        const other = "https://other.example.test";
        let confirmed = false;
        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            calls.push(`${request.method} ${request.url}`);
            if (request.url === `${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`) {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release).pipe(
                Effect.ensuring(Deferred.succeed(finished, undefined)),
              );
              confirmed = true;
              return HttpClientResponse.fromWeb(request, Response.json(DESCRIPTOR));
            }
            if (request.url.endsWith("/.well-known/t3/environment")) {
              return HttpClientResponse.fromWeb(request, Response.json(DESCRIPTOR));
            }
            if (request.url.startsWith(other)) {
              yield* Deferred.succeed(fallbackTried, undefined);
              return HttpClientResponse.fromWeb(
                request,
                fallbackSucceeds ? websocketTicket("fallback") : authInvalid(),
              );
            }
            expect(confirmed).toBe(true);
            return HttpClientResponse.fromWeb(request, websocketTicket("slow-route"));
          }),
        );
        const harness = yield* makeConnectionHarness({
          responses: [],
          httpClient: Layer.succeed(HttpClient.HttpClient, client),
        });
        yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          const pending = yield* driver
            .connect(
              connectionEntry([bearerRoute("slow"), bearerRoute("fallback", other)]),
              () => Effect.void,
            )
            .pipe(Effect.scoped, Effect.forkChild);
          yield* Deferred.await(started);
          yield* TestClock.adjust("2500 millis");
          yield* Deferred.await(fallbackTried);
          if (!fallbackSucceeds) yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(pending);
          yield* Deferred.await(finished);
        }).pipe(Effect.provide(harness.layer));

        expect(calls).toEqual([
          `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
          `GET ${other}/.well-known/t3/environment`,
          `POST ${other}/api/auth/websocket-ticket`,
          ...(!fallbackSucceeds ? [`POST ${ENDPOINT.httpBaseUrl}/api/auth/websocket-ticket`] : []),
        ]);
        expect(harness.opened[0]?.httpBaseUrl).toBe(
          fallbackSucceeds ? other : ENDPOINT.httpBaseUrl,
        );
      }),
  );

  it.effect.each([1, 2])(
    "fetches each bearer route once during a %s-route reconnect",
    (routeCount) =>
      Effect.gen(function* () {
        const routes = [
          bearerRoute("first"),
          bearerRoute("second", "https://other.example.test"),
        ].slice(0, routeCount);
        const checked = yield* Deferred.make<void>();
        const calls: ReturnType<typeof recordedFetch>["calls"] = [];
        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            calls.push([request.url, { method: request.method, headers: request.headers }]);
            if (request.method === "GET") {
              if (calls.length === routeCount) yield* Deferred.succeed(checked, undefined);
              yield* Deferred.await(checked);
            }
            return HttpClientResponse.fromWeb(
              request,
              request.method === "GET" ? Response.json(DESCRIPTOR) : websocketTicket("ticket"),
            );
          }),
        );
        const harness = yield* makeConnectionHarness({
          responses: [],
          httpClient: Layer.succeed(HttpClient.HttpClient, client),
        });
        yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          yield* driver.connect(connectionEntry(routes), () => Effect.void).pipe(Effect.scoped);
        }).pipe(Effect.provide(harness.layer));

        expect(requestSequence({ fetch: { calls } })).toEqual([
          ...routes.map((route) => {
            const profile = Option.getOrThrow(route.profile);
            if (profile._tag !== "BearerConnectionProfile")
              throw new Error("Expected a bearer route.");
            return `GET ${profile.httpBaseUrl}/.well-known/t3/environment`;
          }),
          `POST ${ENDPOINT.httpBaseUrl}/api/auth/websocket-ticket`,
        ]);
        expect(harness.opened).toHaveLength(1);
        expect(harness.opened[0]?.socketUrl).toContain(
          `orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}`,
        );
        for (const [, init] of calls.filter(([, init]) => init.method === "GET")) {
          expect(new Headers(init.headers).has("authorization")).toBe(false);
        }
        expect(
          new Headers(calls.find(([, init]) => init.method === "POST")?.[1].headers).get(
            "authorization",
          ),
        ).toBe("Bearer bearer-token");
      }),
  );

  it.effect.each([1, 2])(
    "rejects mismatched environments without credentials on %s saved routes",
    (routeCount) =>
      Effect.gen(function* () {
        const routes = [
          bearerRoute("first"),
          bearerRoute("second", "https://other.example.test"),
        ].slice(0, routeCount);
        const harness = yield* makeConnectionHarness({
          responses: routes.map(() =>
            Response.json({ ...DESCRIPTOR, environmentId: "wrong-environment" }),
          ),
        });
        const failure = yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          return yield* driver
            .connect(connectionEntry(routes), () => Effect.void)
            .pipe(Effect.scoped, Effect.flip);
        }).pipe(Effect.provide(harness.layer));

        expect(failure).toMatchObject({
          reason: "configuration",
          detail: "Connected environment wrong-environment does not match environment-1.",
        });
        expect(harness.fetch.calls).toHaveLength(routeCount);
        expect(
          harness.fetch.calls.every(
            ([, init]) => init.method === "GET" && !new Headers(init.headers).has("authorization"),
          ),
        ).toBe(true);
        expect(harness.opened).toHaveLength(0);
      }),
  );

  it.effect("checks the exact alternate URL before sending it the bearer token", () =>
    Effect.gen(function* () {
      const other = "https://other.example.test";
      const harness = yield* makeConnectionHarness({
        responses: [
          Response.json({ ...DESCRIPTOR, environmentId: "wrong-environment" }),
          Response.json(DESCRIPTOR),
          websocketTicket("alternate-ticket"),
        ],
      });
      yield* Effect.gen(function* () {
        const driver = yield* ConnectionDriver.ConnectionDriver;
        yield* driver
          .connect(
            connectionEntry([bearerRoute("first"), bearerRoute("second", other)]),
            () => Effect.void,
          )
          .pipe(Effect.scoped);
      }).pipe(Effect.provide(harness.layer));

      expect(requestSequence(harness)).toEqual([
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        `GET ${other}/.well-known/t3/environment`,
        `POST ${other}/api/auth/websocket-ticket`,
      ]);
      expect(harness.opened[0]?.httpBaseUrl).toBe(other);
    }),
  );

  it.effect("rechecks identity on the next reconnect even while the bearer cache is fresh", () =>
    Effect.gen(function* () {
      const harness = yield* makeConnectionHarness({
        responses: [
          Response.json(DESCRIPTOR),
          websocketTicket("ticket"),
          Response.json({ ...DESCRIPTOR, environmentId: "wrong-environment" }),
        ],
      });
      const failure = yield* Effect.gen(function* () {
        const driver = yield* ConnectionDriver.ConnectionDriver;
        const entry = connectionEntry([bearerRoute("saved")]);
        yield* driver.connect(entry, () => Effect.void).pipe(Effect.scoped);
        return yield* driver.connect(entry, () => Effect.void).pipe(Effect.scoped, Effect.flip);
      }).pipe(Effect.provide(harness.layer));

      expect(failure.reason).toBe("configuration");
      expect(requestSequence(harness)).toEqual([
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        `POST ${ENDPOINT.httpBaseUrl}/api/auth/websocket-ticket`,
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
      ]);
      expect(harness.opened).toHaveLength(1);
    }),
  );

  it.effect("shares preflight discovery with authorization and compatibility", () =>
    Effect.gen(function* () {
      const harness = yield* makeConnectionHarness({
        responses: [Response.json(DESCRIPTOR), websocketTicket("ticket")],
      });
      const route = bearerRoute("saved");
      const accepted = yield* Effect.gen(function* () {
        const driver = yield* ConnectionDriver.ConnectionDriver;
        return yield* driver.preflight(connectionEntry([route]), route);
      }).pipe(Effect.provide(harness.layer));

      expect(accepted).toBe(true);
      expect(harness.fetch.calls).toHaveLength(2);
      expect(harness.opened).toHaveLength(0);
    }),
  );

  it.effect.each([undefined, ORCHESTRATION_PROTOCOL_VERSION + 1])(
    "preserves compatibility rejection for protocol %s",
    (version) =>
      Effect.gen(function* () {
        const harness = yield* makeConnectionHarness({
          responses: [
            Response.json({ ...DESCRIPTOR, orchestrationProtocolVersion: version }),
            websocketTicket("ticket"),
          ],
        });
        const failure = yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          return yield* driver
            .connect(connectionEntry([bearerRoute("saved")]), () => Effect.void)
            .pipe(Effect.scoped, Effect.flip);
        }).pipe(Effect.provide(harness.layer));

        expect(failure.reason).toBe("unsupported");
        expect(failure.message).toContain(
          version === undefined
            ? "Update T3 Code on Remote environment"
            : "This client is not supported",
        );
        expect(harness.fetch.calls).toHaveLength(2);
        expect(harness.opened).toHaveLength(0);
      }),
  );

  it.effect.each([
    "primary",
    "primary-bearer",
    "ssh",
    "direct-dpop",
    "relay-cached",
    "relay-bootstrap",
  ] as const)("fetches one descriptor for a %s reconnect", (kind) =>
    Effect.gen(function* () {
      const bearer = bearerRoute("saved");
      const route: ConnectionRoute =
        kind === "ssh"
          ? {
              target: new SshConnectionTarget({
                environmentId: ENVIRONMENT_ID,
                connectionId: "ssh",
                label: "SSH",
              }),
              profile: Option.some(
                new SshConnectionProfile({
                  environmentId: ENVIRONMENT_ID,
                  connectionId: "ssh",
                  label: "SSH",
                  target: SSH_TARGET,
                }),
              ),
            }
          : kind.startsWith("relay")
            ? {
                target: new RelayConnectionTarget({
                  environmentId: ENVIRONMENT_ID,
                  label: "Relay",
                }),
                profile: Option.none(),
              }
            : kind.startsWith("primary")
              ? {
                  target: new PrimaryConnectionTarget({
                    environmentId: ENVIRONMENT_ID,
                    label: "Primary",
                    ...ENDPOINT,
                  }),
                  profile: Option.none(),
                }
              : {
                  ...bearer,
                  profile: Option.some(
                    new BearerConnectionProfile({
                      connectionId: "saved",
                      environmentId: ENVIRONMENT_ID,
                      label: "Direct DPoP",
                      ...ENDPOINT,
                      authorization: "t3-connect",
                    }),
                  ),
                };
      const harness = yield* makeConnectionHarness({
        ...(kind === "primary-bearer" ? { primaryBearerToken: "primary-token" } : {}),
        ...(kind === "relay-cached" || kind === "direct-dpop"
          ? { initialToken: persistedToken() }
          : {}),
        responses: [
          Response.json(DESCRIPTOR),
          ...(kind === "relay-bootstrap" ? [accessToken("new-token")] : []),
          ...(kind === "primary" ? [] : [websocketTicket("ticket")]),
        ],
      });
      yield* Effect.gen(function* () {
        const driver = yield* ConnectionDriver.ConnectionDriver;
        yield* driver.connect(connectionEntry([route]), () => Effect.void).pipe(Effect.scoped);
      }).pipe(Effect.provide(harness.layer));

      expect(requestSequence(harness)).toEqual([
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        ...(kind === "relay-bootstrap" ? [`POST ${ENDPOINT.httpBaseUrl}/oauth/token`] : []),
        ...(kind === "primary" ? [] : [`POST ${ENDPOINT.httpBaseUrl}/api/auth/websocket-ticket`]),
      ]);
      expect(harness.opened).toHaveLength(1);
    }),
  );
});

describe("Experimental HTTPS reconnect", () => {
  it.effect.each([
    { name: "enabled HTTPS", enabled: true, httpBaseUrl: ENDPOINT.httpBaseUrl, skip: true },
    { name: "disabled HTTPS", enabled: false, httpBaseUrl: ENDPOINT.httpBaseUrl, skip: false },
    { name: "default HTTPS", enabled: undefined, httpBaseUrl: ENDPOINT.httpBaseUrl, skip: false },
    { name: "enabled HTTP", enabled: true, httpBaseUrl: "http://100.64.0.1:3773", skip: false },
    {
      name: "enabled HTTPS with insecure WebSocket",
      enabled: true,
      httpBaseUrl: ENDPOINT.httpBaseUrl,
      wsBaseUrl: "ws://environment.example.test",
      skip: false,
    },
    {
      name: "enabled learned HTTPS",
      enabled: true,
      httpBaseUrl: ENDPOINT.httpBaseUrl,
      learned: true,
      skip: false,
    },
  ])("keeps the expected request sequence for $name", (scenario) =>
    Effect.gen(function* () {
      const route = bearerRoute("saved", scenario.httpBaseUrl);
      const profile = Option.getOrThrow(route.profile);
      if (profile._tag !== "BearerConnectionProfile") throw new Error("Expected bearer profile.");
      const harness = yield* makeConnectionHarness({
        ...(scenario.enabled === undefined
          ? {}
          : { skipDescriptorOnHttpsReconnect: scenario.enabled }),
        responses: [
          ...(scenario.skip ? [] : [Response.json(DESCRIPTOR)]),
          websocketTicket("first"),
          ...(scenario.skip ? [] : [Response.json(DESCRIPTOR)]),
          websocketTicket("reconnect"),
        ],
      });
      yield* Effect.gen(function* () {
        const driver = yield* ConnectionDriver.ConnectionDriver;
        const entry = connectionEntry([
          {
            ...route,
            profile: Option.some(
              new BearerConnectionProfile({
                ...profile,
                ...("wsBaseUrl" in scenario ? { wsBaseUrl: scenario.wsBaseUrl } : {}),
                ...("learned" in scenario ? { learned: true } : {}),
              }),
            ),
          },
        ]);
        yield* driver.connect(entry, () => Effect.void).pipe(Effect.scoped);
        yield* driver.connect(entry, () => Effect.void).pipe(Effect.scoped);
      }).pipe(Effect.provide(harness.layer));

      const attempt = [
        ...(scenario.skip ? [] : [`GET ${scenario.httpBaseUrl}/.well-known/t3/environment`]),
        `POST ${scenario.httpBaseUrl}/api/auth/websocket-ticket`,
      ];
      expect(requestSequence(harness)).toEqual([...attempt, ...attempt]);
      expect(harness.opened).toHaveLength(2);
      const socketUrl = new URL(harness.opened[1]!.socketUrl);
      expect(socketUrl.searchParams.get("wsTicket")).toBe("reconnect");
      expect(socketUrl.searchParams.get("orchestrationProtocol")).toBe(
        String(ORCHESTRATION_PROTOCOL_VERSION),
      );
      expect(new Headers(harness.fetch.calls.at(-1)?.[1].headers).get("authorization")).toBe(
        "Bearer bearer-token",
      );
    }),
  );

  it.effect.each([
    { mismatch: false, incompatible: false },
    { mismatch: true, incompatible: false },
    { mismatch: false, incompatible: true },
  ])(
    "keeps multi-route checks with mismatch=$mismatch and incompatible=$incompatible",
    ({ mismatch, incompatible }) =>
      Effect.gen(function* () {
        const other = "https://other.example.test";
        const checked = yield* Deferred.make<void>();
        const calls: ReturnType<typeof recordedFetch>["calls"] = [];
        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            calls.push([request.url, { method: request.method, headers: request.headers }]);
            if (request.method === "GET") {
              if (calls.length === 2) yield* Deferred.succeed(checked, undefined);
              yield* Deferred.await(checked);
            }
            return HttpClientResponse.fromWeb(
              request,
              request.method === "GET"
                ? Response.json({
                    ...DESCRIPTOR,
                    orchestrationProtocolVersion:
                      ORCHESTRATION_PROTOCOL_VERSION + (incompatible ? 1 : 0),
                    ...(mismatch && request.url.startsWith(ENDPOINT.httpBaseUrl)
                      ? { environmentId: "wrong-environment" }
                      : {}),
                  })
                : websocketTicket("ticket"),
            );
          }),
        );
        const harness = yield* makeConnectionHarness({
          skipDescriptorOnHttpsReconnect: true,
          responses: [],
          httpClient: Layer.succeed(HttpClient.HttpClient, client),
        });
        const result = yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          return yield* driver
            .connect(
              connectionEntry([bearerRoute("first"), bearerRoute("second", other)]),
              () => Effect.void,
            )
            .pipe(Effect.scoped, Effect.result);
        }).pipe(Effect.provide(harness.layer));

        const selected = mismatch ? other : ENDPOINT.httpBaseUrl;
        expect(requestSequence({ fetch: { calls } })).toEqual([
          `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
          `GET ${other}/.well-known/t3/environment`,
          `POST ${selected}/api/auth/websocket-ticket`,
        ]);
        if (incompatible) {
          expect(result).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "ConnectionBlockedError", reason: "unsupported" },
          });
          expect(harness.opened).toHaveLength(0);
        } else {
          expect(result._tag).toBe("Success");
          expect(harness.opened[0]?.httpBaseUrl).toBe(selected);
        }
      }),
  );

  it.effect("keeps descriptor checks when preparing an outdated host update", () =>
    Effect.gen(function* () {
      const harness = yield* makeConnectionHarness({
        skipDescriptorOnHttpsReconnect: true,
        responses: [
          Response.json({ ...DESCRIPTOR, orchestrationProtocolVersion: undefined }),
          websocketTicket("ticket"),
        ],
      });
      const result = yield* Effect.gen(function* () {
        const resolver = yield* ConnectionResolver.ConnectionResolver;
        return yield* resolver.prepareForUpdate(connectionEntry([bearerRoute("saved")]));
      }).pipe(Effect.provide(harness.layer));

      expect(result.descriptor.environmentId).toBe(ENVIRONMENT_ID);
      expect(result.prepared.socketUrl).not.toContain("orchestrationProtocol");
      expect(requestSequence(harness)).toEqual([
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        `POST ${ENDPOINT.httpBaseUrl}/api/auth/websocket-ticket`,
      ]);
    }),
  );
});

describe("Connection descriptor authorization boundaries", () => {
  it.effect.each(["cached", "bootstrap"] as const)(
    "rejects a mismatched relay endpoint before %s DPoP credentials are sent",
    (kind) =>
      Effect.gen(function* () {
        const harness = yield* makeConnectionHarness({
          ...(kind === "cached" ? { initialToken: persistedToken() } : {}),
          responses: [Response.json({ ...DESCRIPTOR, environmentId: "wrong-environment" })],
        });
        const failure = yield* Effect.gen(function* () {
          const resolver = yield* ConnectionResolver.ConnectionResolver;
          return yield* resolver
            .prepare(
              connectionEntry([
                {
                  target: new RelayConnectionTarget({
                    environmentId: ENVIRONMENT_ID,
                    label: "Relay",
                  }),
                  profile: Option.none(),
                },
              ]),
            )
            .pipe(Effect.flip);
        }).pipe(Effect.provide(harness.layer));

        expect(failure.reason).toBe("configuration");
        expect(requestSequence(harness)).toEqual([
          `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        ]);
        expect(yield* Ref.get(harness.proofInputs)).toHaveLength(0);
      }),
  );

  it.effect("checks both the learned direct URL and the bootstrap URL once", () =>
    Effect.gen(function* () {
      const direct = "http://192.168.1.10:3773";
      const harness = yield* makeConnectionHarness({
        responses: [
          Response.json(DESCRIPTOR),
          Response.json(DESCRIPTOR),
          accessToken("token"),
          websocketTicket("ticket"),
        ],
      });
      const prepared = yield* Effect.gen(function* () {
        const resolver = yield* ConnectionResolver.ConnectionResolver;
        return yield* resolver.prepare(
          connectionEntry([
            {
              ...bearerRoute("direct", direct),
              profile: Option.some(
                new BearerConnectionProfile({
                  connectionId: "direct",
                  environmentId: ENVIRONMENT_ID,
                  label: "Direct",
                  httpBaseUrl: direct,
                  wsBaseUrl: direct.replace(/^http/, "ws"),
                  authorization: "t3-connect",
                }),
              ),
            },
          ]),
        );
      }).pipe(Effect.provide(harness.layer));

      expect(prepared.httpBaseUrl).toBe(direct);
      expect(requestSequence(harness)).toEqual([
        `GET ${direct}/.well-known/t3/environment`,
        `GET ${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
        `POST ${ENDPOINT.httpBaseUrl}/oauth/token`,
        `POST ${direct}/api/auth/websocket-ticket`,
      ]);
    }),
  );

  it.effect("reuses the descriptor for an incompatible host's update connection", () =>
    Effect.gen(function* () {
      const harness = yield* makeConnectionHarness({
        responses: [
          Response.json({ ...DESCRIPTOR, orchestrationProtocolVersion: undefined }),
          websocketTicket("ticket"),
        ],
      });
      const result = yield* Effect.gen(function* () {
        const resolver = yield* ConnectionResolver.ConnectionResolver;
        return yield* resolver.prepareForUpdate(connectionEntry([bearerRoute("saved")]));
      }).pipe(Effect.provide(harness.layer));

      expect(result.descriptor.environmentId).toBe(ENVIRONMENT_ID);
      expect(result.prepared.socketUrl).not.toContain("orchestrationProtocol");
      expect(harness.fetch.calls).toHaveLength(2);
    }),
  );

  it.effect(
    "keeps failed descriptor probes silent and reuses their failures without credentials",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeConnectionHarness({
          responses: [new Response(null, { status: 503 }), new Response(null, { status: 503 })],
        });
        const failure = yield* Effect.gen(function* () {
          const driver = yield* ConnectionDriver.ConnectionDriver;
          return yield* driver
            .connect(
              connectionEntry([
                bearerRoute("first"),
                bearerRoute("second", "https://other.example.test"),
              ]),
              () => Effect.void,
            )
            .pipe(Effect.scoped, Effect.flip);
        }).pipe(Effect.provide(harness.layer));

        expect(failure).toMatchObject({
          _tag: "ConnectionTransientError",
          reason: "remote-unavailable",
        });
        expect(harness.fetch.calls).toHaveLength(2);
        expect(harness.fetch.calls.every(([, init]) => init.method === "GET")).toBe(true);
        expect(harness.opened).toHaveLength(0);
      }),
  );
});

describe("RemoteEnvironmentAuthorization", () => {
  it.effect("skips bearer tickets when the platform provides an upgrade header constructor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ responses: [Response.json(DESCRIPTOR)] });
      const [first, second] = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const authorize = () =>
          remote.authorizeBearer({
            expectedEnvironmentId: ENVIRONMENT_ID,
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            wsBaseUrl: `${ENDPOINT.wsBaseUrl}/ws?wsTicket=stale-ticket`,
            bearerToken: "secret-bearer-token",
            connectionMethod: "direct",
          });
        return [yield* authorize(), yield* authorize()] as const;
      }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(layerWebSocketWithHeaders))));

      expect(first.socketUrl).toBe(
        "wss://environment.example.test/ws?clientDeviceType=phone&clientOs=test&connectionMethod=direct",
      );
      expect(second.socketUrl).toBe(first.socketUrl);
      expect(first.httpAuthorization).toEqual({ _tag: "Bearer", token: "secret-bearer-token" });
      expect(harness.fetch.calls.map(([url]) => String(url))).toEqual([
        "https://environment.example.test/.well-known/t3/environment",
      ]);
    }),
  );

  it.effect("checks the environment identity before preparing a bearer header connection", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        responses: [
          Response.json({
            ...DESCRIPTOR,
            environmentId: EnvironmentId.make("another-environment"),
          }),
        ],
      });
      const rejected = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote
          .authorizeBearer({
            expectedEnvironmentId: ENVIRONMENT_ID,
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            wsBaseUrl: ENDPOINT.wsBaseUrl,
            bearerToken: "secret-bearer-token",
            connectionMethod: "direct",
          })
          .pipe(Effect.flip);
      }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(layerWebSocketWithHeaders))));

      expect(rejected).toMatchObject({ _tag: "ConnectionBlockedError", reason: "configuration" });
      expect(harness.fetch.calls).toHaveLength(1);
    }),
  );

  it.effect("reuses a validated bearer descriptor while issuing fresh websocket tickets", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        responses: [
          Response.json(DESCRIPTOR),
          websocketTicket("first-ticket"),
          websocketTicket("second-ticket"),
        ],
      });

      const [first, second] = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const authorize = () =>
          remote.authorizeBearer({
            expectedEnvironmentId: ENVIRONMENT_ID,
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            wsBaseUrl: ENDPOINT.wsBaseUrl,
            bearerToken: "bearer-token",
            connectionMethod: "direct",
          });
        return [yield* authorize(), yield* authorize()] as const;
      }).pipe(Effect.provide(harness.layer));

      expect(first.socketUrl).toContain("wsTicket=first-ticket");
      expect(second.socketUrl).toContain("wsTicket=second-ticket");
      expect(
        harness.fetch.calls.filter(([url]) => String(url).endsWith("/.well-known/t3/environment")),
      ).toHaveLength(1);
      expect(
        harness.fetch.calls.filter(([url]) => String(url).endsWith("/api/auth/websocket-ticket")),
      ).toHaveLength(2);
    }),
  );

  it.effect("revalidates a bearer descriptor after the cache expires", () =>
    Effect.gen(function* () {
      const reassignedEnvironmentId = EnvironmentId.make("environment-2");
      const harness = yield* makeHarness({
        responses: [
          Response.json(DESCRIPTOR),
          websocketTicket("first-ticket"),
          Response.json({
            ...DESCRIPTOR,
            environmentId: reassignedEnvironmentId,
          }),
        ],
      });

      const failure = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const authorize = () =>
          remote.authorizeBearer({
            expectedEnvironmentId: ENVIRONMENT_ID,
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            wsBaseUrl: ENDPOINT.wsBaseUrl,
            bearerToken: "bearer-token",
            connectionMethod: "direct",
          });

        yield* authorize();
        yield* TestClock.adjust("10 seconds");
        return yield* authorize().pipe(Effect.flip);
      }).pipe(Effect.provide(Layer.merge(harness.layer, TestClock.layer())));

      expect(failure).toEqual(
        expect.objectContaining({
          _tag: "ConnectionBlockedError",
          reason: "configuration",
          detail: `Connected environment ${reassignedEnvironmentId} does not match ${ENVIRONMENT_ID}.`,
        }),
      );
      expect(
        harness.fetch.calls.filter(([url]) => String(url).endsWith("/.well-known/t3/environment")),
      ).toHaveLength(2);
    }),
  );

  it.effect("keeps DPoP tickets even when the platform supports upgrade headers", () =>
    Effect.gen(function* () {
      const cached = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "cached-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: cached,
        responses: [websocketTicket("cached-ticket")],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(layerWebSocketWithHeaders))));

      expect(authorized.socketUrl).toContain("wsTicket=cached-ticket");
      expect(authorized.socketUrl).toContain("connectionMethod=relay");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(0);
      expect(harness.fetch.calls).toHaveLength(1);
      expect(String(harness.fetch.calls[0]?.[0])).toBe(
        "https://environment.example.test/api/auth/websocket-ticket",
      );
    }),
  );

  it.effect("uses the T3 Connect token on a learned direct address without the relay", () =>
    Effect.gen(function* () {
      const cached = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "cached-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: cached,
        responses: [Response.json(DESCRIPTOR), websocketTicket("lan-ticket")],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
          directEndpoint: {
            httpBaseUrl: "http://192.168.1.10:3773/",
            wsBaseUrl: "ws://192.168.1.10:3773/",
          },
        });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized.httpBaseUrl).toBe("http://192.168.1.10:3773/");
      expect(authorized.socketUrl).toMatch(/^ws:\/\/192\.168\.1\.10:3773\/ws\?/);
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(0);
      expect(harness.fetch.calls.map(([url]) => String(url))).toEqual([
        "http://192.168.1.10:3773/.well-known/t3/environment",
        "http://192.168.1.10:3773/api/auth/websocket-ticket",
      ]);
    }),
  );

  it.effect("sends no token to a direct address that answers as another machine", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        initialToken: persistedToken({ accessToken: "cached-access-token" }),
        responses: [
          Response.json({ ...DESCRIPTOR, environmentId: EnvironmentId.make("someone-else") }),
        ],
      });

      const error = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote
          .authorizeDpop({
            expectedEnvironmentId: ENVIRONMENT_ID,
            directEndpoint: {
              httpBaseUrl: "http://192.168.1.10:3773/",
              wsBaseUrl: "ws://192.168.1.10:3773/",
            },
          })
          .pipe(Effect.flip);
      }).pipe(Effect.provide(harness.layer));

      expect(error).toMatchObject({ _tag: "ConnectionBlockedError", reason: "configuration" });
      expect(harness.fetch.calls.map(([url]) => String(url))).toEqual([
        "http://192.168.1.10:3773/.well-known/t3/environment",
      ]);
      // The token stays saved for the T3 Connect route.
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.accessToken).toBe(
        "cached-access-token",
      );
    }),
  );

  it.effect("replaces a token a learned direct address rejects", () =>
    Effect.gen(function* () {
      const cached = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "revoked-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: cached,
        responses: [
          Response.json(DESCRIPTOR),
          authInvalid(),
          Response.json(DESCRIPTOR),
          accessToken("replacement-access-token"),
          websocketTicket("lan-ticket"),
        ],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
          directEndpoint: {
            httpBaseUrl: "http://192.168.1.10:3773/",
            wsBaseUrl: "ws://192.168.1.10:3773/",
          },
        });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized.socketUrl).toContain("wsTicket=lan-ticket");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.accessToken).toBe(
        "replacement-access-token",
      );
    }),
  );

  it.effect("refreshes and persists an expired environment token", () =>
    Effect.gen(function* () {
      const expired = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "expired-access-token",
        expiresAtEpochMs: 0,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: expired,
        responses: [
          Response.json(DESCRIPTOR),
          accessToken("fresh-access-token"),
          websocketTicket("fresh-ticket"),
        ],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized.socketUrl).toContain("wsTicket=fresh-ticket");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)).toEqual(
        expect.objectContaining({
          accessToken: "fresh-access-token",
          dpopThumbprint: "thumbprint-1",
        }),
      );
      expect(harness.fetch.calls).toHaveLength(3);
    }),
  );

  it.effect.each([
    { name: "read-only", grantScopes: ["orchestration:read"] },
    { name: "administrative", grantScopes: AuthAdministrativeScopes },
  ] as const)(
    "inherits $name pairing grant scopes for websocket authorization and HTTP renewal",
    ({ grantScopes }) =>
      Effect.gen(function* () {
        const grantedScope = grantScopes.join(" ");
        let exchangeCount = 0;
        const tokenFields = (init: RequestInit) =>
          new URLSearchParams(
            init.body instanceof Uint8Array
              ? new TextDecoder().decode(init.body)
              : String(init.body),
          );
        const exchangeGrant = (init: RequestInit) => {
          const fields = tokenFields(init);
          expect(fields.get("scope")).toBeNull();
          const scope = fields.get("scope") ?? grantedScope;
          const scopes = scope.split(" ");
          if (scopes.some((requested) => !grantScopes.some((grant) => grant === requested))) {
            return authInvalid();
          }
          return accessToken(`access-token:${++exchangeCount}:${scopes.join(",")}`, scope);
        };
        const harness = yield* makeHarness({
          responses: [
            Response.json(DESCRIPTOR),
            exchangeGrant,
            websocketTicket("granted-ticket"),
            Response.json(DESCRIPTOR),
            exchangeGrant,
          ],
        });

        const [authorized, refreshed] = yield* Effect.gen(function* () {
          const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
          const first = yield* remote.authorizeDpop({
            expectedEnvironmentId: ENVIRONMENT_ID,
          });
          yield* TestClock.adjust("1 hour");
          const refreshed = yield* remote.authorizeDpopHttp({
            expectedEnvironmentId: ENVIRONMENT_ID,
          });
          return [first, refreshed] as const;
        }).pipe(Effect.provide(Layer.merge(harness.layer, TestClock.layer())));

        expect(authorized.socketUrl).toContain("wsTicket=granted-ticket");
        expect(authorized.httpAuthorization).toMatchObject({
          _tag: "Dpop",
          accessToken: `access-token:1:${grantScopes.join(",")}`,
        });
        expect(refreshed.httpAuthorization).toMatchObject({
          _tag: "Dpop",
          accessToken: `access-token:2:${grantScopes.join(",")}`,
        });
        expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)).toMatchObject({
          accessToken: `access-token:2:${grantScopes.join(",")}`,
          dpopThumbprint: "thumbprint-1",
        });
        expect(yield* Ref.get(harness.bootstrapCalls)).toBe(2);
        const exchanges = harness.fetch.calls.filter(([url]) =>
          String(url).endsWith("/oauth/token"),
        );
        expect(exchanges).toHaveLength(2);
        for (const [, init] of exchanges) {
          expect(Object.fromEntries(tokenFields(init))).toMatchObject({
            subject_token: BOOTSTRAP.credential,
            client_label: "T3 Code Test",
            client_device_type: "mobile",
            client_os: "test",
          });
        }
      }),
  );

  it.effect("evicts an auth-invalid cached token and obtains a fresh bootstrap", () =>
    Effect.gen(function* () {
      const cached = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "invalid-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: cached,
        responses: [
          authInvalid(),
          Response.json(DESCRIPTOR),
          accessToken("replacement-access-token"),
          websocketTicket("replacement-ticket"),
        ],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized.socketUrl).toContain("wsTicket=replacement-ticket");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)).toEqual(
        expect.objectContaining({
          accessToken: "replacement-access-token",
        }),
      );
      expect(harness.fetch.calls).toHaveLength(4);
    }),
  );

  it.effect("presents clock skew as one possible cause for a generic DPoP rejection", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        responses: [Response.json(DESCRIPTOR), authInvalid()],
      });

      const failure = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer), Effect.flip);

      expect(failure).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "authentication",
        detail: `The environment credential is invalid. ${DPOP_UNKNOWN_HINT}`,
        traceId: "trace-auth-invalid",
      });
    }),
  );

  it.effect("refreshes a cached endpoint after its first transient failure", () =>
    Effect.gen(function* () {
      const cached = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        accountId: "account-1",
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "cached-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: cached,
        responses: [
          new Response("endpoint unavailable", { status: 503 }),
          Response.json(DESCRIPTOR),
          accessToken("replacement-access-token"),
          websocketTicket("replacement-ticket"),
        ],
      });

      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized.socketUrl).toContain("wsTicket=replacement-ticket");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)).toEqual(
        expect.objectContaining({
          accessToken: "replacement-access-token",
        }),
      );
      expect(harness.fetch.calls).toHaveLength(4);
    }),
  );

  it.effect("removes a newly refreshed token when websocket ticket authorization fails", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        responses: [
          Response.json(DESCRIPTOR),
          accessToken("unusable-access-token"),
          new Response("endpoint unavailable", { status: 503 }),
        ],
      });

      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpop({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
      }).pipe(Effect.provide(harness.layer), Effect.flip);

      expect((yield* Ref.get(harness.tokens)).has(ENVIRONMENT_ID)).toBe(false);
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect(harness.fetch.calls).toHaveLength(3);
    }),
  );

  it.effect("uses the current cached HTTP token without a relay request or websocket ticket", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ initialToken: persistedToken(), responses: [] });
      const authorized = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
      }).pipe(Effect.provide(harness.layer));

      expect(authorized).toEqual({
        environmentId: ENVIRONMENT_ID,
        label: DESCRIPTOR.label,
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "cached-access-token",
          expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        },
      });
      expect(harness.fetch.calls).toHaveLength(0);
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(0);
      expect(yield* Ref.get(harness.proofInputs)).toEqual([]);
    }),
  );

  it.effect(
    "renews HTTP credentials near expiry through the current cloud account and device",
    () =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const harness = yield* makeHarness({
          initialToken: persistedToken({ expiresAtEpochMs: now + 61_000 }),
          responses: [Response.json(DESCRIPTOR), accessToken("fresh-access-token")],
        });
        yield* Effect.gen(function* () {
          const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
          const before = yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
          expect(before.httpAuthorization.accessToken).toBe("cached-access-token");
          yield* TestClock.adjust("1 second");
          const after = yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
          expect(after.httpAuthorization.accessToken).toBe("fresh-access-token");
          expect(after.httpAuthorization.expiresAtEpochMs).toBe(now + 3_601_000);
        }).pipe(Effect.provide(harness.layer));

        expect(yield* Ref.get(harness.relayInputs)).toEqual([
          {
            environmentId: ENVIRONMENT_ID,
            clerkToken: "clerk-session",
            scopes: [RelayEnvironmentConnectScope],
            deviceId: "device-1",
          },
        ]);
        expect(harness.fetch.calls.map(([url]) => String(url))).toEqual([
          `${ENDPOINT.httpBaseUrl}/.well-known/t3/environment`,
          `${ENDPOINT.httpBaseUrl}/oauth/token`,
        ]);
        expect(yield* Ref.get(harness.proofInputs)).toEqual([
          {
            method: "POST",
            url: `${ENDPOINT.httpBaseUrl}/oauth/token`,
          },
        ]);
      }),
  );

  it.effect("does not evict a replacement token when a late request rejects the old one", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        initialToken: persistedToken(),
        responses: [Response.json(DESCRIPTOR), accessToken("replacement-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const initial = yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
        const retry = {
          expectedEnvironmentId: ENVIRONMENT_ID,
          rejectedAccessToken: initial.httpAuthorization.accessToken,
        };
        const first = yield* remote.authorizeDpopHttp(retry);
        const late = yield* remote.authorizeDpopHttp(retry);
        expect(first.httpAuthorization.accessToken).toBe("replacement-token");
        expect(late).toEqual(first);
      }).pipe(Effect.provide(harness.layer));

      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect(harness.fetch.calls).toHaveLength(2);
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.accessToken).toBe(
        "replacement-token",
      );
    }),
  );

  it.effect("shares one in-flight renewal between concurrent HTTP requests", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        initialToken: persistedToken({ expiresAtEpochMs: 0 }),
        beforeBootstrap: Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        ),
        responses: [Response.json(DESCRIPTOR), accessToken("shared-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const requests = yield* Effect.all(
          Array.from({ length: 6 }, () =>
            remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID }),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.forkChild);
        yield* Queue.takeN(harness.tokenReads, 6);
        yield* Deferred.await(started);
        expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
        yield* Deferred.succeed(release, undefined);
        const results = yield* Fiber.join(requests);
        expect(results.map((result) => result.httpAuthorization.accessToken)).toEqual(
          Array(6).fill("shared-token"),
        );
      }).pipe(Effect.provide(harness.layer));

      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect(harness.fetch.calls).toHaveLength(2);
    }),
  );

  it.effect(
    "shares a failed renewal and allows a later attempt instead of caching its failure",
    () =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void, ManagedRelay.ManagedRelayClientError>();
        const failure = new ManagedRelay.ManagedRelayRequestTimeoutError({
          activity: "Relay environment connection",
          timeoutMs: ManagedRelay.MANAGED_RELAY_REQUEST_TIMEOUT_MS,
          traceId: null,
        });
        const harness = yield* makeHarness({
          responses: [],
          beforeBootstrap: Deferred.await(release),
        });
        yield* Effect.gen(function* () {
          const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
          const requests = yield* Effect.all(
            Array.from({ length: 6 }, () =>
              remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID }).pipe(Effect.flip),
            ),
            { concurrency: "unbounded" },
          ).pipe(Effect.forkChild);
          yield* Queue.takeN(harness.tokenReads, 6);
          yield* Deferred.fail(release, failure);
          const failures = yield* Fiber.join(requests);
          expect(failures).toEqual(
            Array(6).fill(
              expect.objectContaining({
                _tag: "ConnectionTransientError",
                reason: "timeout",
              }),
            ),
          );
          expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
          yield* remote
            .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
            .pipe(Effect.flip);
          expect(yield* Ref.get(harness.bootstrapCalls)).toBe(2);
        }).pipe(Effect.provide(harness.layer));
        expect(harness.fetch.calls).toHaveLength(0);
      }),
  );

  it.effect("does not block another environment behind a pending renewal", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const otherEnvironmentId = EnvironmentId.make("environment-2");
      const harness = yield* makeHarness({
        initialToken: persistedToken({
          environmentId: otherEnvironmentId,
          accessToken: "other-token",
        }),
        beforeBootstrap: Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        ),
        responses: [Response.json(DESCRIPTOR), accessToken("fresh-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const pending = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const other = yield* remote.authorizeDpopHttp({
          expectedEnvironmentId: otherEnvironmentId,
        });
        expect(other.httpAuthorization.accessToken).toBe("other-token");
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(pending);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("does not return or persist credentials after logout during renewal", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        beforeBootstrap: Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        ),
        responses: [],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const pending = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(started);
        yield* Ref.set(harness.session, Option.none());
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(pending)).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "authentication",
        });
      }).pipe(Effect.provide(harness.layer));
      expect((yield* Ref.get(harness.tokens)).size).toBe(0);
      expect(harness.fetch.calls).toHaveLength(0);
    }),
  );

  it.effect("removes an obsolete credential if logout overlaps the persistent write", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        beforePut: Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        ),
        responses: [Response.json(DESCRIPTOR), accessToken("obsolete-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const pending = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(started);
        yield* Ref.set(harness.session, Option.none());
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(pending)).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "authentication",
        });
      }).pipe(Effect.provide(harness.layer));
      expect((yield* Ref.get(harness.tokens)).size).toBe(0);
    }),
  );

  it.effect(
    "renews another account's persisted token after the authorization service restarts",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          initialToken: persistedToken(),
          responses: [Response.json(DESCRIPTOR), accessToken("account-2-token")],
        });
        const authorize = Effect.gen(function* () {
          const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
          return yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
        }).pipe(Effect.provide(harness.layer));

        expect((yield* authorize).httpAuthorization.accessToken).toBe("cached-access-token");
        yield* Ref.set(harness.session, Option.some({ accountId: "account-2" }));
        expect((yield* authorize).httpAuthorization.accessToken).toBe("account-2-token");
        expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.accountId).toBe("account-2");

        yield* Ref.set(harness.session, Option.some({ accountId: "account-2" }));
        expect((yield* authorize).httpAuthorization.accessToken).toBe("account-2-token");
        expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
        expect(harness.fetch.calls).toHaveLength(2);
      }),
  );

  it.effect("renews a legacy unowned token once and reuses its account-bound replacement", () =>
    Effect.gen(function* () {
      const legacy = new TokenStore.RemoteDpopAccessToken({
        environmentId: ENVIRONMENT_ID,
        label: DESCRIPTOR.label,
        endpoint: ENDPOINT,
        accessToken: "legacy-access-token",
        expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        dpopThumbprint: "thumbprint-1",
      });
      const harness = yield* makeHarness({
        initialToken: legacy,
        responses: [Response.json(DESCRIPTOR), accessToken("account-bound-token")],
      });
      const authorize = Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
      }).pipe(Effect.provide(harness.layer));

      expect((yield* authorize).httpAuthorization.accessToken).toBe("account-bound-token");
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.accountId).toBe("account-1");
      expect((yield* authorize).httpAuthorization.accessToken).toBe("account-bound-token");
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
      expect(harness.fetch.calls).toHaveLength(2);
    }),
  );

  it.effect("requires fresh credentials after signing back into the same account", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        initialToken: persistedToken(),
        responses: [Response.json(DESCRIPTOR), accessToken("new-session-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
        yield* Ref.set(harness.session, Option.none());
        const signedOut = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.flip);
        expect(signedOut).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "authentication",
        });
        yield* Ref.set(harness.session, Option.some({ accountId: "account-1" }));
        const signedIn = yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
        expect(signedIn.httpAuthorization.accessToken).toBe("new-session-token");
      }).pipe(Effect.provide(harness.layer));
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
    }),
  );

  it.effect("renews a cached credential bound to a different signing key", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        initialToken: persistedToken(),
        responses: [Response.json(DESCRIPTOR), accessToken("new-key-token")],
      });
      yield* Ref.set(harness.thumbprint, "thumbprint-2");
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const authorized = yield* remote.authorizeDpopHttp({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
        expect(authorized.httpAuthorization.accessToken).toBe("new-key-token");
      }).pipe(Effect.provide(harness.layer));
      expect((yield* Ref.get(harness.tokens)).get(ENVIRONMENT_ID)?.dpopThumbprint).toBe(
        "thumbprint-2",
      );
    }),
  );

  it.effect("rejects a relay bootstrap for a different environment before contacting it", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        responses: [],
        bootstrap: { ...BOOTSTRAP, environmentId: EnvironmentId.make("wrong-environment") },
      });
      const failure = yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        return yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
      }).pipe(Effect.provide(harness.layer), Effect.flip);
      expect(failure).toMatchObject({ _tag: "ConnectionBlockedError", reason: "configuration" });
      expect(harness.fetch.calls).toHaveLength(0);
    }),
  );

  it.effect("keeps a shared renewal alive when one waiting request is interrupted", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        beforeBootstrap: Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        ),
        responses: [Response.json(DESCRIPTOR), accessToken("fresh-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const pending = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const other = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.forkChild);
        yield* Queue.takeN(harness.tokenReads, 2);
        yield* Fiber.interrupt(pending);
        yield* Deferred.succeed(release, undefined);
        const authorized = yield* Fiber.join(other);
        expect(authorized.httpAuthorization.accessToken).toBe("fresh-token");
        const cached = yield* remote.authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID });
        expect(cached).toEqual(authorized);
      }).pipe(Effect.provide(harness.layer));
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
    }),
  );

  it.effect("times out a stalled cloud token read so later requests can renew again", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const reads = yield* Ref.make(0);
      const harness = yield* makeHarness({
        clerkToken: Effect.gen(function* () {
          const read = yield* Ref.updateAndGet(reads, (value) => value + 1);
          if (read === 1) {
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never;
          }
          return "clerk-session";
        }),
        responses: [Response.json(DESCRIPTOR), accessToken("fresh-token")],
      });
      yield* Effect.gen(function* () {
        const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
        const pending = yield* remote
          .authorizeDpopHttp({ expectedEnvironmentId: ENVIRONMENT_ID })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(started);
        yield* TestClock.adjust("30 seconds");
        expect(yield* Fiber.join(pending)).toMatchObject({
          _tag: "ConnectionTransientError",
          reason: "timeout",
        });
        const authorized = yield* remote.authorizeDpopHttp({
          expectedEnvironmentId: ENVIRONMENT_ID,
        });
        expect(authorized.httpAuthorization.accessToken).toBe("fresh-token");
      }).pipe(Effect.provide(harness.layer));
      expect(yield* Ref.get(reads)).toBe(2);
      expect(yield* Ref.get(harness.bootstrapCalls)).toBe(1);
    }),
  );
});
