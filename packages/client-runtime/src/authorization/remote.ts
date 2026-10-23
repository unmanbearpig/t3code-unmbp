import * as DateTime from "effect/DateTime";
import * as Redacted from "effect/Redacted";
import { FetchHttpClient } from "effect/http";
import type { PreparedRpcTicket } from "../connection/model.ts";
import {
  type AuthWebSocketTicketResult,
  AuthAccessTokenType,
  type AuthClientPresentationMetadata,
  AuthEnvironmentBootstrapTokenType,
  AuthTokenExchangeGrantType,
  type ClientConnectionMethod,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { encodeOAuthScope } from "@t3tools/shared/oauthScope";
import { hashTelemetryId } from "@t3tools/shared/telemetryId";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { environmentEndpointUrl } from "../environment/endpoint.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  executeEnvironmentHttpRequest,
  makeEnvironmentHttpApiGroupClient,
  type RemoteEnvironmentRequestError,
} from "../rpc/http.ts";

export { RemoteEnvironmentAuthUndeclaredStatusError } from "../rpc/http.ts";
export type RemoteEnvironmentAuthError = RemoteEnvironmentRequestError;

const DEFAULT_REMOTE_REQUEST_TIMEOUT_MS = 10_000;

const clientMetadataTokenExchangeFields = (
  clientMetadata: AuthClientPresentationMetadata | undefined,
) => {
  const displayOs = clientMetadata?.os;
  return {
    ...(clientMetadata?.label ? { client_label: clientMetadata.label } : {}),
    ...(clientMetadata?.deviceType ? { client_device_type: clientMetadata.deviceType } : {}),
    ...(displayOs && displayOs !== "unknown" && displayOs !== "other"
      ? { client_os: displayOs }
      : {}),
  };
};

/** Hashes of who is connecting. Only hashes leave the client. */
export interface ClientOwnerParams {
  readonly connect?: string;
  readonly device?: string;
}

/**
 * Tells the environment, for analytics, which T3 Connect account and device
 * are connecting, so it can count the environment as theirs.
 */
export const readClientOwnerParams = Effect.gen(function* () {
  const session = yield* Effect.serviceOption(ClientCapabilities.CloudSession);
  const account = Option.isSome(session) ? yield* session.value.identity : Option.none();
  const device = yield* (yield* ClientCapabilities.AnalyticsDevice).id;
  return {
    ...(Option.isSome(account) ? { connect: hashTelemetryId(account.value.accountId) } : {}),
    ...(Option.isSome(device) ? { device: hashTelemetryId(device.value) } : {}),
  } satisfies ClientOwnerParams;
});

export const appendClientConnectionParams = (
  url: URL,
  clientMetadata: AuthClientPresentationMetadata | undefined,
  connectionMethod?: ClientConnectionMethod,
  owners?: ClientOwnerParams,
): void => {
  if (clientMetadata?.surface) {
    url.searchParams.set("clientSurface", clientMetadata.surface);
  }
  if (clientMetadata?.appVersion) {
    url.searchParams.set("clientAppVersion", clientMetadata.appVersion);
  }
  if (clientMetadata?.deviceType) {
    const deviceType =
      clientMetadata.deviceType === "mobile"
        ? "phone"
        : clientMetadata.deviceType === "desktop" || clientMetadata.deviceType === "tablet"
          ? clientMetadata.deviceType
          : "unknown";
    url.searchParams.set("clientDeviceType", deviceType);
  }
  if (clientMetadata?.os) {
    url.searchParams.set("clientOs", clientMetadata.os);
  }
  if (clientMetadata?.surface === "web") {
    if (clientMetadata.webDeployment) {
      url.searchParams.set("clientWebDeployment", clientMetadata.webDeployment);
    }
    if (clientMetadata.browser) {
      url.searchParams.set("clientBrowser", clientMetadata.browser);
    }
  }
  if (clientMetadata?.surface === "mobile") {
    if (clientMetadata.osMajorVersion !== undefined) {
      url.searchParams.set("clientOsMajorVersion", String(clientMetadata.osMajorVersion));
    }
    if (clientMetadata.deviceModel) {
      url.searchParams.set("clientDeviceModel", clientMetadata.deviceModel);
    }
  }
  if (connectionMethod) {
    url.searchParams.set("connectionMethod", connectionMethod);
  }
  if (owners?.connect) {
    url.searchParams.set("clientOwner", owners.connect);
  }
  if (owners?.device) {
    url.searchParams.set("clientDevice", owners.device);
  }
};

export const exchangeRemoteDpopAccessToken = Effect.fn(
  "clientRuntime.authorization.exchangeRemoteDpopAccessToken",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly credential: string;
  readonly scopes?: ReadonlyArray<AuthEnvironmentScope>;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly dpopProof: string;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
  const response = yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/oauth/token"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.token({
      headers: { dpop: input.dpopProof },
      payload: {
        grant_type: AuthTokenExchangeGrantType,
        subject_token: input.credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        ...(input.scopes ? { scope: encodeOAuthScope(input.scopes) } : {}),
        ...clientMetadataTokenExchangeFields(input.clientMetadata),
      },
    }),
  );
  return response;
});

export const bootstrapRemoteBearerSession = Effect.fn(
  "clientRuntime.authorization.bootstrapRemoteBearerSession",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly credential: string;
  readonly scopes?: ReadonlyArray<AuthEnvironmentScope>;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/oauth/token"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.token({
      headers: {},
      payload: {
        grant_type: AuthTokenExchangeGrantType,
        subject_token: input.credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        ...(input.scopes ? { scope: encodeOAuthScope(input.scopes) } : {}),
        ...clientMetadataTokenExchangeFields(input.clientMetadata),
      },
    }),
  );
});

export const fetchRemoteSessionState = Effect.fn(
  "clientRuntime.authorization.fetchRemoteSessionState",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/session"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.session({
      headers: {
        authorization: `Bearer ${input.bearerToken}`,
      },
    }),
  );
});

export const issueRemoteWebSocketTicket = Effect.fn(
  "clientRuntime.authorization.issueRemoteWebSocketTicket",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.webSocketTicket({
      headers: {
        authorization: `Bearer ${input.bearerToken}`,
      },
    }),
  );
});

export const issueRemoteDpopWebSocketTicket = Effect.fn(
  "clientRuntime.authorization.issueRemoteDpopWebSocketTicket",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly accessToken: string;
  readonly dpopProof: string;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.webSocketTicket({
      headers: {
        authorization: `DPoP ${input.accessToken}`,
        dpop: input.dpopProof,
      },
    }),
  );
});

export const remoteWebSocketUrl = (
  input: {
    readonly wsBaseUrl: string;
    readonly clientMetadata?: AuthClientPresentationMetadata;
    readonly connectionMethod?: ClientConnectionMethod;
  },
  ticket?: string,
  owner?: ClientOwnerParams,
): string => {
  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  // A saved URL must not let a stale ticket override upgrade header auth.
  url.searchParams.delete("wsTicket");
  if (ticket !== undefined) {
    url.searchParams.set("wsTicket", ticket);
  }
  appendClientConnectionParams(url, input.clientMetadata, input.connectionMethod, owner);
  return url.toString();
};

export const resolveRemoteWebSocketConnection = Effect.fn(
  "clientRuntime.authorization.resolveRemoteWebSocketConnection",
)(function* (input: {
  readonly wsBaseUrl: string;
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly connectionMethod?: ClientConnectionMethod;
  readonly timeoutMs?: number;
}) {
  const issued = yield* issueRemoteWebSocketTicket({
    httpBaseUrl: input.httpBaseUrl,
    bearerToken: input.bearerToken,
    ...(input.timeoutMs ? { timeoutMs: input.timeoutMs } : {}),
  });

  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  url.searchParams.set("wsTicket", issued.ticket);
  appendClientConnectionParams(
    url,
    input.clientMetadata,
    input.connectionMethod,
    yield* readClientOwnerParams,
  );
  return { socketUrl: url.toString(), rpcTicket: prepareRpcTicket(issued) };
});

export const resolveRemoteDpopWebSocketConnection = Effect.fn(
  "clientRuntime.authorization.resolveRemoteDpopWebSocketConnection",
)(function* (input: {
  readonly wsBaseUrl: string;
  readonly httpBaseUrl: string;
  readonly accessToken: string;
  readonly dpopProof: string;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly connectionMethod?: ClientConnectionMethod;
  readonly timeoutMs?: number;
}) {
  const issued = yield* issueRemoteDpopWebSocketTicket({
    httpBaseUrl: input.httpBaseUrl,
    accessToken: input.accessToken,
    dpopProof: input.dpopProof,
    ...(input.timeoutMs ? { timeoutMs: input.timeoutMs } : {}),
  });
  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  url.searchParams.set("wsTicket", issued.ticket);
  appendClientConnectionParams(
    url,
    input.clientMetadata,
    input.connectionMethod,
    yield* readClientOwnerParams,
  );
  return { socketUrl: url.toString(), rpcTicket: prepareRpcTicket(issued) };
});

const prepareRpcTicket = (issued: AuthWebSocketTicketResult): PreparedRpcTicket => ({
  value: Redacted.make(issued.ticket),
  expiresAtEpochMs: DateTime.toEpochMillis(issued.expiresAt),
});

/** Uses the primary browser's existing cookie and the ticket route's origin policy. */
export const issuePrimaryRpcTicket = Effect.fn("clientRuntime.authorization.issuePrimaryRpcTicket")(
  function* (input: { readonly httpBaseUrl: string }) {
    const client = yield* makeEnvironmentHttpApiGroupClient(input.httpBaseUrl, "auth");
    const issued = yield* executeEnvironmentHttpRequest(
      environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
      DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
      client.webSocketTicket({ headers: {} }),
    ).pipe(Effect.provideService(FetchHttpClient.RequestInit, { credentials: "include" }));
    return prepareRpcTicket(issued);
  },
);

// Browser/device streaming sockets keep their existing string-returning API.
export const resolveRemoteWebSocketConnectionUrl = (
  input: Parameters<typeof resolveRemoteWebSocketConnection>[0],
) => resolveRemoteWebSocketConnection(input).pipe(Effect.map((connection) => connection.socketUrl));

export const resolveRemoteDpopWebSocketConnectionUrl = (
  input: Parameters<typeof resolveRemoteDpopWebSocketConnection>[0],
) =>
  resolveRemoteDpopWebSocketConnection(input).pipe(
    Effect.map((connection) => connection.socketUrl),
  );
