// @effect-diagnostics nodeBuiltinImport:off -- TLS validation uses Node's certificate implementation.
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import { ORCHESTRATION_PROTOCOL_VERSION, type OrchestrationClientOrigin } from "@t3tools/contracts";
import {
  RPC_WEBTRANSPORT_LIMITS,
  RPC_WEBTRANSPORT_WIRE_VERSION,
  createTransportByteBudget,
  wrapStandardWebTransportSession,
} from "@t3tools/shared/rpcWebTransport";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as HttpServerRequest from "effect/http/HttpServerRequest";

import * as AuthenticatedRpcSession from "../AuthenticatedRpcSession.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as WebTransportAvailability from "./WebTransportAvailability.ts";
import * as WebTransportBackend from "./WebTransportBackend.ts";
import { parseWebTransportConfig, WebTransportUnavailableError } from "./WebTransportConfig.ts";
import { prepareRpcWebTransportServer } from "./WebTransportProtocol.ts";

const INITIAL_TIMEOUT = "2 seconds";
const MAX_SESSIONS = 64;
const MAX_ADMISSIONS = 16;

/** The callback always resolves a status, including failed authentication and cancellation. */
function installRequestCallback(
  server: InstanceType<typeof import("@fails-components/webtransport").Http3Server>,
  admit: (header: unknown) => Effect.Effect<object>,
  context: Context.Context<never>,
  scope: Scope.Scope,
  path: string,
  isClosing: () => boolean,
) {
  const run = Effect.runPromiseWith(context);
  const pending = new Set<Promise<object>>();
  server.setRequestCallback(({ header }: { header: unknown }) => {
    const result = isClosing()
      ? Promise.resolve({ status: 403, path })
      : run(admit(header).pipe(Effect.forkIn(scope), Effect.flatMap(Fiber.join))).then(
          (result) => result,
          () => ({ status: 403, path }),
        );
    pending.add(result);
    void result.then(() => pending.delete(result));
    return result;
  });
  // Local callback settlement lets upstream finish/reject each admission before
  // native stop. It never waits for a peer or a native acknowledgement.
  return Effect.promise(() => Promise.allSettled(pending)).pipe(Effect.asVoid);
}

export class WebTransportListener extends Context.Service<
  WebTransportListener,
  { readonly listen: Effect.Effect<void, never, Scope.Scope> }
>()("t3/transport/WebTransportListener") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const availability = yield* WebTransportAvailability.WebTransportAvailability;
  const backend = yield* WebTransportBackend.WebTransportBackend;
  const fs = yield* FileSystem.FileSystem;
  const identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const rpc = yield* AuthenticatedRpcSession.AuthenticatedRpcSession;
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;

  const listen = Effect.gen(function* () {
    const rawConfig = config.rpcWebTransport;
    if (rawConfig === undefined) return;
    const listenerConfig = yield* Effect.try({
      try: () => parseWebTransportConfig(rawConfig),
      catch: (cause) => new WebTransportUnavailableError({ stage: "config", cause }),
    });
    const endpoint = new URL(listenerConfig.url);
    // The pinned backend requires path even when rejecting CONNECT.
    const rejected = { status: 403, path: endpoint.pathname } as const;
    const environmentId = yield* identity.getEnvironmentId;
    const now = yield* DateTime.now;
    const [cert, privKey] = yield* Effect.all(
      [fs.readFileString(listenerConfig.certPath), fs.readFileString(listenerConfig.keyPath)],
      { concurrency: 2 },
    ).pipe(
      Effect.mapError((cause) => new WebTransportUnavailableError({ stage: "certificate", cause })),
    );
    yield* Effect.try({
      try: () => {
        const certificate = new NodeCrypto.X509Certificate(cert);
        const hostname = endpoint.hostname.replace(/^\[|\]$/gu, "");
        const matches = NodeNet.isIP(hostname)
          ? certificate.checkIP(hostname)
          : certificate.checkHost(hostname);
        if (
          !matches ||
          !certificate.checkPrivateKey(NodeCrypto.createPrivateKey(privKey)) ||
          Date.parse(certificate.validFrom) > now.epochMilliseconds ||
          Date.parse(certificate.validTo) <= now.epochMilliseconds
        ) {
          throw new WebTransportUnavailableError({ stage: "certificate" });
        }
      },
      catch: (cause) => new WebTransportUnavailableError({ stage: "certificate", cause }),
    });
    const { Http3Server } = yield* backend.load;
    yield* startup.awaitCommandReady.pipe(
      Effect.mapError((cause) => new WebTransportUnavailableError({ stage: "listen", cause })),
    );
    const secret = yield* Effect.try({
      try: () => NodeCrypto.randomBytes(32).toString("hex"),
      catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
    });
    const server = yield* Effect.try({
      try: () =>
        new Http3Server({
          host: listenerConfig.host,
          port: listenerConfig.port,
          cert,
          privKey,
          secret,
          maxConnections: MAX_SESSIONS,
          initialBidirectionalStreams: 80,
          initialBidirectionalReceiveStreams: 80,
          initialBidirectionalSendStreams: 80,
        }),
      catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
    });
    // Native completion is advisory. Never await stop/closed while finalizing
    // handlers, background leases, or the server's own scope.
    let closing = false;
    let ready = false;
    let stopRequested = false;
    let stopped = false;
    let drainAdmissions = Effect.void;
    const stop = () => {
      if (!ready || !stopRequested || stopped) return;
      stopped = true;
      try {
        server.stopServer();
      } catch {
        // Native stop errors must not hold up logical cleanup.
      }
    };
    // startServer starts an async initialization without returning its promise.
    // Only fulfilled public readiness makes stop safe: stopping during DNS can
    // destroy the native server while initialization still owns its pointer.
    // Late readiness still belongs to us, without holding logical cleanup open.
    void server.ready.then(
      () => {
        ready = true;
        stop();
      },
      () => undefined,
    );
    yield* Effect.addFinalizer(() =>
      availability.set(Option.none()).pipe(
        Effect.andThen(
          Effect.sync(() => {
            closing = true;
          }),
        ),
        Effect.andThen(Effect.suspend(() => drainAdmissions)),
        Effect.andThen(
          Effect.sync(() => {
            stopRequested = true;
            stop();
          }),
        ),
      ),
    );
    void server.closed.catch(() => undefined);
    const reader = server.sessionStream(endpoint.pathname, { noAutoPaths: true }).getReader();
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        try {
          reader.releaseLock();
        } catch {
          /* An interrupted pending native read may still own it. */
        }
      }),
    );
    const budgets = {
      input: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerInputBytes),
      output: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerOutputBytes),
    };
    const scope = yield* Effect.scope;
    const context = yield* Effect.context<never>();
    let pending = 0;
    let reserved = 0;
    let nextAdmission = 0;
    const admissions = new Map<
      number,
      {
        ticket: string;
        origin: OrchestrationClientOrigin;
        props: Readonly<Record<string, unknown>>;
        release: () => void;
      }
    >();
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        for (const admission of admissions.values()) admission.release();
        admissions.clear();
      }),
    );
    const admit = (header: unknown) =>
      Effect.gen(function* () {
        if (pending >= MAX_ADMISSIONS || reserved >= MAX_SESSIONS) return rejected;
        pending++;
        reserved++;
        let handedOff = false;
        let released = false;
        const release = () => {
          if (!released) {
            released = true;
            reserved--;
          }
        };
        return yield* Effect.gen(function* () {
          const path =
            typeof header === "object" && header !== null && ":path" in header
              ? header[":path"]
              : undefined;
          if (typeof path !== "string" || path.length > 8192 || !path.startsWith("/"))
            return rejected;
          const url = yield* Effect.try({
            try: () => new URL(path, endpoint),
            catch: (cause) => new WebTransportUnavailableError({ stage: "admission", cause }),
          });
          const ticket = url.searchParams.get("wsTicket");
          if (
            url.origin !== endpoint.origin ||
            url.pathname !== endpoint.pathname ||
            !ticket ||
            ticket.length > 4096
          )
            return rejected;
          yield* auth.authenticateRpcTicket(ticket);
          const request = HttpServerRequest.fromWeb(new Request(url));
          const id = nextAdmission++;
          admissions.set(id, {
            ticket,
            origin: AuthenticatedRpcSession.readClientConnectionOrigin(request),
            props: AuthenticatedRpcSession.readClientAnalyticsProps(request),
            release,
          });
          // A successful CONNECT that never becomes an accepted native session
          // must not hold an application session slot indefinitely.
          yield* Effect.sleep(INITIAL_TIMEOUT).pipe(
            Effect.andThen(
              Effect.sync(() => {
                if (admissions.delete(id)) release();
              }),
            ),
            Effect.forkIn(scope),
          );
          handedOff = true;
          return { status: 200, path: endpoint.pathname, userData: { admissionId: id } };
        }).pipe(
          Effect.timeoutOrElse({
            duration: INITIAL_TIMEOUT,
            orElse: () => Effect.succeed(rejected),
          }),
          Effect.catch(() => Effect.succeed(rejected)),
          Effect.ensuring(
            Effect.sync(() => {
              pending--;
              if (!handedOff) release();
            }),
          ),
        );
      });
    drainAdmissions = installRequestCallback(
      server,
      admit,
      context,
      scope,
      endpoint.pathname,
      () => closing,
    );
    yield* Effect.try({
      try: () => server.startServer(),
      catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
    });
    yield* Effect.tryPromise({
      try: () => server.ready,
      catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
    }).pipe(
      Effect.timeoutOrElse({
        duration: "5 seconds",
        orElse: () => Effect.fail(new WebTransportUnavailableError({ stage: "listen" })),
      }),
    );
    yield* availability.set(
      Option.some({ url: endpoint.toString(), wireVersion: RPC_WEBTRANSPORT_WIRE_VERSION }),
    );
    const accept = Effect.gen(function* () {
      while (true) {
        const next = yield* Effect.tryPromise({
          try: (signal) =>
            reader.read().then((next) => {
              if (signal.aborted && !next.done) next.value.close({ closeCode: 1 });
              return next;
            }),
          catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
        });
        if (next.done) return;
        const raw = next.value;
        const data: unknown = (raw as typeof raw & { userData?: unknown }).userData;
        const id =
          typeof data === "object" && data !== null && "admissionId" in data
            ? data.admissionId
            : undefined;
        const admission = typeof id === "number" ? admissions.get(id) : undefined;
        if (!admission) {
          raw.close({ closeCode: 1 });
          continue;
        }
        admissions.delete(id as number);
        const session = wrapStandardWebTransportSession(raw);
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                session.close(0);
                admission.release();
              }),
            );
            const prepared = yield* prepareRpcWebTransportServer(session, {
              environmentId,
              orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
              budgets,
            }).pipe(
              Effect.timeoutOrElse({
                duration: INITIAL_TIMEOUT,
                orElse: () => Effect.fail(new WebTransportUnavailableError({ stage: "session" })),
              }),
            );
            const revalidated = yield* auth.authenticateRpcTicket(admission.ticket);
            yield* rpc
              .run({
                session: revalidated,
                protocol: prepared.protocol,
                lifetime: prepared.closed,
                closed: prepared.closed,
                close: prepared.close,
                origin: admission.origin,
                analyticsProps: admission.props,
              })
              .pipe(Effect.ensuring(prepared.close));
          }),
        ).pipe(
          // Session faults do not stop the optional listener or expose ticket data.
          Effect.catch(() => Effect.void),
          Effect.forkIn(scope),
        );
      }
    });
    yield* Effect.raceFirst(
      accept,
      Effect.tryPromise({
        try: () => server.closed,
        catch: (cause) => new WebTransportUnavailableError({ stage: "listen", cause }),
      }),
    );
  }).pipe(
    Effect.scoped,
    Effect.catch((error) =>
      availability
        .set(Option.none())
        .pipe(
          Effect.andThen(Effect.logWarning("RPC WebTransport disabled", { stage: error.stage })),
        ),
    ),
  );
  return WebTransportListener.of({ listen });
});

export const layer = Layer.effect(WebTransportListener, make);

export const layerListening = Layer.effectDiscard(
  Effect.gen(function* () {
    const listener = yield* WebTransportListener;
    yield* listener.listen.pipe(Effect.forkScoped);
  }),
).pipe(Layer.provide(layer), Layer.provide(WebTransportBackend.layer));
