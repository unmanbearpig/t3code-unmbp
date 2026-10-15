import {
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_VERSION,
  type DesktopSshEnvironmentTarget,
} from "@t3tools/contracts";
import { RelayClientTracer } from "@t3tools/shared/relayTracing";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Redacted from "effect/Redacted";
import { RpcWebTransportConnector } from "../platform/rpcWebTransport.ts";
import * as Tracer from "effect/Tracer";

import * as ConnectionResolver from "./resolver.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  type ConnectionCatalogEntry,
  SshConnectionProfile,
  type ConnectionCredential,
  type ConnectionProfile,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import {
  BearerConnectionTarget,
  ConnectionTransientError,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
  type ConnectionTarget,
} from "./model.ts";
import * as ConnectionProfileStore from "./profileStore.ts";
import * as RpcHttp from "../rpc/http.ts";
import {
  GitHubRoutingPermissions,
  gitHubRoutingConnectionKey,
  makeGitHubRoutingPermissions,
} from "./githubRoutingPermissions.ts";

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");
const ENDPOINT = {
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
};
const SSH_TARGET: DesktopSshEnvironmentTarget = {
  alias: "development",
  hostname: "development.example.test",
  username: "developer",
  port: 22,
};

function catalogEntry(
  target: ConnectionTarget,
  profile: Option.Option<ConnectionProfile> = Option.none(),
): ConnectionCatalogEntry {
  return { target, profile, enabled: true };
}

function collectingTracer(spans: Array<string>): Tracer.Tracer {
  return Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        end(endTime, exit);
        spans.push(span.name);
      };
      return span;
    },
  });
}

const makeDependencies = Effect.fn("TestConnectionResolver.makeDependencies")((options?: {
  readonly profiles?: ReadonlyArray<ConnectionProfile>;
  readonly profileStore?: ConnectionProfileStore.ConnectionProfileStore["Service"];
  readonly credentials?: ReadonlyArray<readonly [string, ConnectionCredential]>;
  readonly authorizeBearer?: RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]["authorizeBearer"];
  readonly authorizeDpop?: RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]["authorizeDpop"];
  readonly primaryBearerToken?: string;
  readonly prepareSsh?: ClientCapabilities.SshEnvironmentGateway["Service"]["prepare"];
  readonly descriptorProtocolVersion?: number | null | undefined;
  readonly advertisement?: { readonly url: string; readonly wireVersion: number };
  readonly connectorAvailable?: boolean;
  readonly cookieTicketFailure?: boolean;
  readonly onRequest?: (url: string, init: RequestInit) => void;
}) => {
  const profiles = new Map(
    (options?.profiles ?? []).map((profile) => [profile.connectionId, profile]),
  );
  const credentials = new Map(options?.credentials ?? []);

  const profileStore = ConnectionProfileStore.ConnectionProfileStore.of({
    get: (connectionId) => Effect.succeed(Option.fromNullishOr(profiles.get(connectionId))),
    put: (profile) => Effect.sync(() => void profiles.set(profile.connectionId, profile)),
    remove: (connectionId) => Effect.sync(() => void profiles.delete(connectionId)),
  });
  const credentialStore = ConnectionCredentialStore.ConnectionCredentialStore.of({
    get: (connectionId) => Effect.succeed(Option.fromNullishOr(credentials.get(connectionId))),
    put: (connectionId, credential) =>
      Effect.sync(() => void credentials.set(connectionId, credential)),
    remove: (connectionId) => Effect.sync(() => void credentials.delete(connectionId)),
  });
  const remote = RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization.of({
    authorizeBearer:
      options?.authorizeBearer ??
      ((input) =>
        Effect.succeed({
          environmentId: input.expectedEnvironmentId,
          label: "Authorized bearer environment",
          httpBaseUrl: input.httpBaseUrl,
          socketUrl: "wss://authorized.example.test/ws?wsTicket=bearer",
          httpAuthorization: {
            _tag: "Bearer" as const,
            token: input.bearerToken,
          },
        })),
    authorizeDpop:
      options?.authorizeDpop ??
      ((input) =>
        Effect.succeed({
          environmentId: input.expectedEnvironmentId,
          label: "Authorized relay environment",
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          socketUrl: "wss://authorized.example.test/ws?wsTicket=dpop",
          httpAuthorization: {
            _tag: "Dpop" as const,
            accessToken: "dpop-access-token",
            expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
          },
        })),
    authorizeDpopHttp: () => Effect.die("unused"),
  });
  const ssh = ClientCapabilities.SshEnvironmentGateway.of({
    provision: () => Effect.die("unused"),
    prepare:
      options?.prepareSsh ??
      (() =>
        Effect.succeed({
          bootstrap: {
            target: SSH_TARGET,
            httpBaseUrl: "http://127.0.0.1:4010",
            wsBaseUrl: "ws://127.0.0.1:4010",
            pairingToken: null,
          },
          bearerToken: "ssh-bearer",
        })),
    disconnect: () => Effect.void,
  });

  const layerDependencies = Layer.mergeAll(
    options?.connectorAvailable
      ? Layer.succeed(RpcWebTransportConnector, {
          connect: () => Promise.reject(new Error("unused")),
        })
      : Layer.empty,
    RpcHttp.layerRemoteHttpClient(((url, init) => {
      options?.onRequest?.(String(url), init ?? {});
      if (init?.method === "POST")
        return Promise.resolve(
          options?.cookieTicketFailure
            ? new Response(null, { status: 401 })
            : Response.json({ ticket: "cookie-rpc-ticket", expiresAt: "2099-01-01T00:00:00.000Z" }),
        );
      return Promise.resolve(
        Response.json({
          environmentId: ENVIRONMENT_ID,
          label: "Compatible environment",
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.0.0-test",
          ...(options?.descriptorProtocolVersion === undefined
            ? { orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION }
            : options.descriptorProtocolVersion === null
              ? {}
              : { orchestrationProtocolVersion: options.descriptorProtocolVersion }),
          capabilities: { repositoryIdentity: true },
          ...(options?.advertisement === undefined
            ? {}
            : { rpcWebTransport: options.advertisement }),
        }),
      );
    }) satisfies typeof fetch),
    Layer.succeed(
      ConnectionProfileStore.ConnectionProfileStore,
      options?.profileStore ?? profileStore,
    ),
    Layer.succeed(ConnectionCredentialStore.ConnectionCredentialStore, credentialStore),
    Layer.succeed(
      ClientCapabilities.PrimaryEnvironmentAuth,
      ClientCapabilities.PrimaryEnvironmentAuth.of({
        bearerToken: Effect.succeed(Option.fromNullishOr(options?.primaryBearerToken)),
      }),
    ),
    Layer.succeed(
      ClientCapabilities.ClientPresentation,
      ClientCapabilities.ClientPresentation.of({
        metadata: { label: "Test Client", deviceType: "desktop", surface: "web" },
      }),
    ),
    Layer.succeed(RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization, remote),
    Layer.succeed(ClientCapabilities.SshEnvironmentGateway, ssh),
  );

  return Effect.succeed(ConnectionResolver.layer.pipe(Layer.provide(layerDependencies)));
});

describe("ConnectionResolver", () => {
  it.effect("blocks an old host during discovery before opening orchestration RPC", () =>
    Effect.gen(function* () {
      const layerBroker = yield* makeDependencies({ descriptorProtocolVersion: null });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      const error = yield* Effect.flip(broker.prepare(catalogEntry(target)));

      expect(error).toMatchObject({ reason: "unsupported" });
      expect(error.message).toContain("Update T3 Code on Compatible environment");
    }),
  );

  it.effect("blocks an incompatible host during discovery before opening orchestration RPC", () =>
    Effect.gen(function* () {
      const layerBroker = yield* makeDependencies({
        descriptorProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION + 1,
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      const error = yield* Effect.flip(broker.prepare(catalogEntry(target)));

      expect(error).toMatchObject({ reason: "unsupported" });
      expect(error.message).toContain("This client is not supported");
    }),
  );

  it.effect("prepares a primary environment without remote capabilities", () =>
    Effect.gen(function* () {
      const layerBroker = yield* makeDependencies();
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      expect(yield* broker.prepare(catalogEntry(target))).toEqual({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        socketUrl:
          "ws://127.0.0.1:3777/ws?clientSurface=web&clientDeviceType=desktop&connectionMethod=direct&orchestrationProtocol=2",
        httpAuthorization: null,
        target,
      });
    }),
  );

  it.effect("authorizes a desktop primary environment with its platform bearer token", () =>
    Effect.gen(function* () {
      const bearerInputs = yield* Ref.make<ReadonlyArray<{ token: string; method: string }>>([]);
      const layerBroker = yield* makeDependencies({
        primaryBearerToken: "desktop-bearer",
        authorizeBearer: (input) =>
          Ref.update(bearerInputs, (values) => [
            ...values,
            { token: input.bearerToken, method: input.connectionMethod },
          ]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Primary",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "ws://127.0.0.1:3777/ws?wsTicket=desktop",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      expect(yield* broker.prepare(catalogEntry(target))).toMatchObject({
        socketUrl: "ws://127.0.0.1:3777/ws?wsTicket=desktop&orchestrationProtocol=2",
        httpAuthorization: { _tag: "Bearer", token: "desktop-bearer" },
        target,
      });
      expect(yield* Ref.get(bearerInputs)).toEqual([{ token: "desktop-bearer", method: "direct" }]);
    }),
  );

  it.effect("uses the registered bearer profile without re-reading the profile store", () =>
    Effect.gen(function* () {
      const bearerInputs = yield* Ref.make<ReadonlyArray<{ token: string; method: string }>>([]);
      const target = new BearerConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId: "saved-1",
      });
      const profile = new BearerConnectionProfile({
        connectionId: "saved-1",
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        wsBaseUrl: ENDPOINT.wsBaseUrl,
      });
      const layerBroker = yield* makeDependencies({
        credentials: [["saved-1", new BearerConnectionCredential({ token: "secret-bearer" })]],
        authorizeBearer: (input) =>
          Ref.update(bearerInputs, (values) => [
            ...values,
            { token: input.bearerToken, method: input.connectionMethod },
          ]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Saved",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=ticket",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));

      expect(
        (yield* broker.prepare(catalogEntry(target, Option.some(profile)))).socketUrl,
      ).toContain("wsTicket=ticket");
      expect(yield* Ref.get(bearerInputs)).toEqual([{ token: "secret-bearer", method: "direct" }]);
    }),
  );

  it.effect("prepares relay connections with the authorized endpoint and credentials", () =>
    Effect.gen(function* () {
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const layerBroker = yield* makeDependencies();
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));

      expect(yield* broker.prepare(catalogEntry(target))).toEqual({
        environmentId: ENVIRONMENT_ID,
        label: "Authorized relay environment",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        socketUrl: `wss://authorized.example.test/ws?wsTicket=dpop&orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}`,
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "dpop-access-token",
          expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        },
        target,
      });
    }),
  );

  it.effect("exports the complete relay authorization flow through the product tracer", () =>
    Effect.gen(function* () {
      const userSpans: Array<string> = [];
      const productSpans: Array<string> = [];
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const layerBroker = yield* makeDependencies({
        authorizeDpop: (input) =>
          Effect.succeed({
            environmentId: input.expectedEnvironmentId,
            label: "Cloud",
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            socketUrl: "wss://environment.example.test/ws?wsTicket=dpop",
            httpAuthorization: {
              _tag: "Dpop" as const,
              accessToken: "dpop-access-token",
              expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
            },
          }).pipe(Effect.withSpan("test.remote.authorizeDpop")),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));

      yield* broker
        .prepare(catalogEntry(target))
        .pipe(
          Effect.provideService(RelayClientTracer, Option.some(collectingTracer(productSpans))),
          Effect.withTracer(collectingTracer(userSpans)),
        );

      expect(productSpans).toContain("clientRuntime.connection.broker.relay");
      expect(productSpans).toContain("test.remote.authorizeDpop");
      expect(userSpans).toContain("clientRuntime.connection.broker.prepare");
      expect(userSpans).not.toContain("test.remote.authorizeDpop");
    }),
  );

  it.effect("delegates SSH launch to the platform gateway before remote authorization", () =>
    Effect.gen(function* () {
      const preparedTargets = yield* Ref.make<ReadonlyArray<DesktopSshEnvironmentTarget>>([]);
      const connectionMethods = yield* Ref.make<ReadonlyArray<string>>([]);
      const target = new SshConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "SSH",
        connectionId: "ssh-1",
      });
      const profile = new SshConnectionProfile({
        connectionId: "ssh-1",
        environmentId: ENVIRONMENT_ID,
        label: "SSH",
        target: SSH_TARGET,
      });
      const layerBroker = yield* makeDependencies({
        prepareSsh: (input) =>
          Ref.update(preparedTargets, (values) => [...values, input.target]).pipe(
            Effect.as({
              bootstrap: {
                target: input.target,
                httpBaseUrl: "http://127.0.0.1:4010",
                wsBaseUrl: "ws://127.0.0.1:4010",
                pairingToken: null,
              },
              bearerToken: "ssh-bearer",
            }),
          ),
        authorizeBearer: (input) =>
          Ref.update(connectionMethods, (methods) => [...methods, input.connectionMethod]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "SSH",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=bearer",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));

      expect(
        (yield* broker.prepare(catalogEntry(target, Option.some(profile)))).socketUrl,
      ).toContain("wsTicket=bearer");
      expect(yield* Ref.get(preparedTargets)).toEqual([SSH_TARGET]);
      expect(yield* Ref.get(connectionMethods)).toEqual(["ssh"]);
    }),
  );

  it.effect.each(["unchanged", "changed", "revocation-failed"] as const)(
    "handles %s SSH routing consent before saving or authorizing",
    (scenario) =>
      Effect.gen(function* () {
        const calls: string[] = [];
        const target = new SshConnectionTarget({
          environmentId: ENVIRONMENT_ID,
          label: "SSH",
          connectionId: "ssh-1",
        });
        const profile = new SshConnectionProfile({
          connectionId: target.connectionId,
          environmentId: ENVIRONMENT_ID,
          label: "SSH",
          target: SSH_TARGET,
        });
        const entry = catalogEntry(target, Option.some(profile));
        const preparedTarget =
          scenario === "unchanged"
            ? SSH_TARGET
            : { ...SSH_TARGET, hostname: "replacement.example.test" };
        const failure = new ConnectionTransientError({
          reason: "remote-unavailable",
          detail: "Could not persist routing consent.",
        });
        const permissions = yield* makeGitHubRoutingPermissions({
          read: Effect.succeed([
            {
              environmentId: ENVIRONMENT_ID,
              connectionKey: gitHubRoutingConnectionKey(entry)!,
              permission: "read-write",
            },
          ]),
          write: () =>
            Effect.sync(() => {
              calls.push("revoke");
            }).pipe(
              Effect.andThen(scenario === "revocation-failed" ? Effect.fail(failure) : Effect.void),
            ),
        });
        let savedProfile: ConnectionProfile = profile;
        const layerBroker = yield* makeDependencies({
          profileStore: {
            get: () => Effect.sync(() => Option.some(savedProfile)),
            put: (value) =>
              Effect.sync(() => {
                calls.push("profile");
                savedProfile = value;
              }),
            remove: () => Effect.die("unused"),
          },
          prepareSsh: () =>
            Effect.succeed({
              bootstrap: {
                target: preparedTarget,
                httpBaseUrl: "http://127.0.0.1:4010",
                wsBaseUrl: "ws://127.0.0.1:4010",
                pairingToken: null,
              },
              bearerToken: "ssh-bearer",
            }),
          authorizeBearer: (input) =>
            Effect.sync(() => {
              calls.push("authorize");
              return {
                environmentId: input.expectedEnvironmentId,
                label: "SSH",
                httpBaseUrl: input.httpBaseUrl,
                socketUrl: "ws://127.0.0.1:4010/ws?wsTicket=ssh",
                httpAuthorization: { _tag: "Bearer" as const, token: input.bearerToken },
              };
            }),
        });
        const broker = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(layerBroker),
        );
        const prepare = broker
          .prepare(entry)
          .pipe(Effect.provideService(GitHubRoutingPermissions, permissions));
        if (scenario === "revocation-failed") {
          expect(yield* Effect.flip(prepare)).toBe(failure);
          expect(savedProfile).toBe(profile);
          expect(calls).toEqual(["revoke"]);
        } else {
          expect((yield* prepare).socketUrl).toContain("wsTicket=ssh");
          expect(savedProfile).toMatchObject({ target: preparedTarget });
          expect(calls).toEqual(
            scenario === "unchanged"
              ? ["profile", "authorize"]
              : ["revoke", "profile", "authorize"],
          );
        }
        expect(yield* permissions.get(entry)).toBe(scenario === "changed" ? "off" : "read-write");
      }),
  );

  it.effect("preserves relay authorization failure classification and trace details", () =>
    Effect.gen(function* () {
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const authorizationError = new ConnectionTransientError({
        reason: "timeout",
        detail: "Relay environment connection timed out.",
        traceId: "relay-trace",
      });
      const layerBroker = yield* makeDependencies({
        authorizeDpop: () => Effect.fail(authorizationError),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(layerBroker));
      const error = yield* Effect.flip(broker.prepare(catalogEntry(target)));

      expect(error).toBe(authorizationError);
    }),
  );
});

describe("ConnectionResolver WebTransport preparation", () => {
  const advertisement = { url: "https://environment.example.test:4443/rpc", wireVersion: 1 };
  const primary = new PrimaryConnectionTarget({
    environmentId: ENVIRONMENT_ID,
    label: "Primary",
    ...ENDPOINT,
  });

  it.effect(
    "issues a cookie ticket only after eligible discovery, retaining the existing WSS URL",
    () =>
      Effect.gen(function* () {
        const calls: Array<{ url: string; init: RequestInit }> = [];
        const dependencies = yield* makeDependencies({
          advertisement,
          connectorAvailable: true,
          onRequest: (url, init) => {
            calls.push({ url, init });
          },
        });
        const resolver = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(dependencies),
        );
        const prepared = yield* resolver.prepare(catalogEntry(primary));
        expect(calls.map((call) => new URL(call.url).pathname)).toEqual([
          "/.well-known/t3/environment",
          "/api/auth/websocket-ticket",
        ]);
        expect(calls[1]!.init.credentials).toBe("include");
        expect(calls[1]!.init.headers).not.toHaveProperty("authorization");
        expect(prepared.socketUrl).not.toContain("wsTicket");
        expect(Redacted.value(prepared.rpcTicket!.value)).toBe("cookie-rpc-ticket");
        expect(prepared.rpcTicket!.expiresAtEpochMs).toBe(Date.parse("2099-01-01T00:00:00.000Z"));
        expect(prepared.rpcWebTransport).toMatchObject({
          ...advertisement,
          orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        });
      }),
  );

  it.effect.each(["old-server", "no-connector", "http", "update"] as const)(
    "adds no cookie dependency for %s",
    (scenario) =>
      Effect.gen(function* () {
        const methods: string[] = [];
        const dependencies = yield* makeDependencies({
          ...(scenario === "old-server" ? {} : { advertisement }),
          connectorAvailable: scenario !== "no-connector",
          onRequest: (_url, init) => {
            methods.push(init.method!);
          },
        });
        const resolver = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(dependencies),
        );
        const entry = catalogEntry(
          scenario === "http"
            ? new PrimaryConnectionTarget({
                ...primary,
                httpBaseUrl: "http://environment.example.test",
                wsBaseUrl: "ws://environment.example.test",
              })
            : primary,
        );
        const prepared =
          scenario === "update"
            ? (yield* resolver.prepareForUpdate(entry)).prepared
            : yield* resolver.prepare(entry);
        expect(methods).toEqual(["GET"]);
        expect(prepared.rpcWebTransport).toBeUndefined();
      }),
  );

  it.effect("keeps WSS usable if the primary cookie ticket route rejects", () =>
    Effect.gen(function* () {
      const dependencies = yield* makeDependencies({
        advertisement,
        connectorAvailable: true,
        cookieTicketFailure: true,
      });
      const resolver = yield* ConnectionResolver.ConnectionResolver.pipe(
        Effect.provide(dependencies),
      );
      const prepared = yield* resolver.prepare(catalogEntry(primary));
      expect(prepared.rpcTicket).toBeUndefined();
      expect(prepared.rpcWebTransport).toBeUndefined();
      expect(prepared.socketUrl).toContain("wss://environment.example.test/ws?");
    }),
  );

  it.effect.each(["bearer", "t3-connect"] as const)(
    "reuses the %s admission ticket on a direct saved route",
    (authorization) =>
      Effect.gen(function* () {
        const rpcTicket = {
          value: Redacted.make("issued-not-parsed-from-socket"),
          expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        };
        const target = new BearerConnectionTarget({
          environmentId: ENVIRONMENT_ID,
          label: "Direct",
          connectionId: "saved",
        });
        const profile = new BearerConnectionProfile({
          environmentId: target.environmentId,
          connectionId: target.connectionId,
          label: target.label,
          ...ENDPOINT,
          ...(authorization === "t3-connect" ? { authorization } : {}),
        });
        const authorized = {
          environmentId: ENVIRONMENT_ID,
          label: "Direct",
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          socketUrl: "wss://environment.example.test/ws?wsTicket=other",
          httpAuthorization: { _tag: "Bearer" as const, token: "secret" },
          rpcTicket,
        };
        const methods: string[] = [];
        const dependencies = yield* makeDependencies({
          advertisement,
          connectorAvailable: true,
          credentials: [["saved", new BearerConnectionCredential({ token: "secret" })]],
          authorizeBearer: () => Effect.succeed(authorized),
          authorizeDpop: () => Effect.succeed(authorized),
          onRequest: (_url, init) => {
            methods.push(init.method!);
          },
        });
        const resolver = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(dependencies),
        );
        const prepared = yield* resolver.prepare(catalogEntry(target, Option.some(profile)));
        expect(prepared.rpcTicket).toBe(rpcTicket);
        expect(prepared.rpcWebTransport?.url).toBe(advertisement.url);
        expect(methods).toEqual(["GET"]);
      }),
  );
});
