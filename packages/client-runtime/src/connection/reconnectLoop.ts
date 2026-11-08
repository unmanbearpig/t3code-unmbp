import {
  ORCHESTRATION_V2_WS_METHODS,
  type EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
} from "@t3tools/contracts";
import { projectSnapshotRevision } from "@t3tools/shared/reconnectSnapshots";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Socket from "effect/socket/Socket";

import * as Persistence from "../platform/persistence.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import { AttemptSnapshotTrial, SnapshotExperiment } from "./snapshotExperiment.ts";
import { AttemptReconnectArm, ReconnectExperiment } from "./reconnectExperiment.ts";
import * as RpcSession from "../rpc/session.ts";
import { applyShellStreamEvents, mergeShellSnapshotProjects } from "../state/shellReducer.ts";
import { EnvironmentRegistry } from "./registry.ts";
import { ConnectionResolver } from "./resolver.ts";

class ReconnectLoopError extends Schema.TaggedError<ReconnectLoopError>()("ReconnectLoopError", {
  message: Schema.String,
}) {}

export type ReconnectLoopArm = "warmup" | "full" | "cached" | "restart";

export interface AppTraffic {
  readonly receivedBytes: number;
  readonly sentBytes: number;
  readonly receivedPackets: number;
  readonly sentPackets: number;
}

export interface ReconnectLoopRow {
  readonly at: string;
  readonly round: number;
  readonly arm: ReconnectLoopArm;
  readonly ok: boolean;
  readonly error?: string;
  readonly prepareMs: number;
  readonly configMs: number;
  readonly syncedMs: number;
  readonly jsonDown: number;
  readonly jsonUp: number;
  readonly sentRevisions: number;
  readonly reusedCatalogs: number;
  readonly projectFrames: number;
  /** Android per-app counters: every socket this app has open, including its own connection. */
  readonly appTraffic: AppTraffic | null;
}

const encoder = new TextEncoder();
const byteLength = (data: unknown) =>
  typeof data === "string"
    ? encoder.encode(data).length
    : data instanceof ArrayBuffer
      ? data.byteLength
      : 0;

const trafficDelta = (before: AppTraffic | null, after: AppTraffic | null): AppTraffic | null =>
  before === null || after === null
    ? null
    : {
        receivedBytes: after.receivedBytes - before.receivedBytes,
        sentBytes: after.sentBytes - before.sentBytes,
        receivedPackets: after.receivedPackets - before.receivedPackets,
        sentPackets: after.sentPackets - before.sentPackets,
      };

export const runReconnectLoop = Effect.fn("ReconnectLoop.run")(
  function* (input: {
    readonly environmentId: EnvironmentId;
    readonly rounds: number;
    readonly options: RpcSession.RpcSessionOptions;
    readonly readTraffic: () => AppTraffic | null;
    readonly onRow: (row: ReconnectLoopRow) => void;
  }) {
    const experiment = yield* ReconnectExperiment;
    const pipelineArm = experiment?.arm(input.environmentId) ?? "new";
    const registry = yield* EnvironmentRegistry;
    const resolver = yield* ConnectionResolver;
    const store = yield* Persistence.EnvironmentCacheStore;
    const webSocketConstructor = yield* Socket.WebSocketConstructor;

    let tally = { jsonDown: 0, jsonUp: 0, sentRevisions: 0, reusedCatalogs: 0 };
    const constructorWithHeaders = yield* Effect.serviceOption(
      ClientCapabilities.WebSocketConstructorWithHeaders,
    );
    const countSocket = (socket: Socket.WebSocketLike) => {
      const send = socket.send.bind(socket);
      socket.send = (data: Parameters<typeof send>[0]) => {
        tally.jsonUp += byteLength(data);
        if (typeof data === "string" && data.includes("providerCatalogRevisions")) {
          try {
            const message = JSON.parse(data) as {
              readonly payload?: { readonly providerCatalogRevisions?: object };
            };
            tally.sentRevisions += Object.keys(
              message.payload?.providerCatalogRevisions ?? {},
            ).length;
          } catch {}
        }
        return send(data);
      };
      socket.addEventListener("message", (event: Socket.WebSocketEvent) => {
        tally.jsonDown += byteLength(event.data);
        if (typeof event.data === "string" && event.data.includes("cachedProviderCatalogs")) {
          const match = /"cachedProviderCatalogs":\{([^}]*)\}/.exec(event.data);
          if (match?.[1]) tally.reusedCatalogs += match[1].split(",").length;
        }
      });
      return socket;
    };

    const makeFactory = (cache: "none" | "store") => {
      const factory = RpcSession.make(input.options).pipe(
        Effect.provideService(Socket.WebSocketConstructor, (url, options) =>
          countSocket(webSocketConstructor(url, options)),
        ),
        Effect.provideService(
          Persistence.EnvironmentCacheStore,
          cache === "store"
            ? store
            : Persistence.EnvironmentCacheStore.of({
                ...store,
                loadServerConfig: Effect.fn("ReconnectLoop.noStoredConfig")(function* () {
                  return Option.none();
                }),
              }),
        ),
      );
      return Option.isSome(constructorWithHeaders)
        ? factory.pipe(
            Effect.provideService(
              ClientCapabilities.WebSocketConstructorWithHeaders,
              (url, headers) => countSocket(constructorWithHeaders.value(url, headers)),
            ),
          )
        : factory;
    };
    const longLived = yield* makeFactory("store");

    let shell: OrchestrationV2ShellSnapshot | null = null;
    const applyItems = (items: ReadonlyArray<OrchestrationV2ShellStreamItem>) => {
      let deltas: Array<OrchestrationV2ShellStreamItem> = [];
      const flush = () => {
        if (deltas.length > 0 && shell !== null) {
          shell = applyShellStreamEvents(shell, deltas as never);
        }
        deltas = [];
      };
      for (const item of items) {
        if (item.kind !== "snapshot" && item.kind !== "synchronized") {
          deltas.push(item);
          continue;
        }
        flush();
        if (item.kind === "snapshot") {
          shell = mergeShellSnapshotProjects(
            shell,
            item.snapshot,
            item.resolvedRepositoryIdentityRoots === undefined
              ? undefined
              : { resolvedRepositoryIdentityRoots: item.resolvedRepositoryIdentityRoots },
          );
        }
      }
      flush();
    };

    const attempt = Effect.fn("ReconnectLoop.attempt")(function* (
      round: number,
      arm: ReconnectLoopArm,
    ) {
      const factory =
        arm === "full"
          ? yield* makeFactory("none")
          : arm === "restart"
            ? yield* makeFactory("store")
            : longLived;
      const entry = Option.fromUndefinedOr(
        (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId),
      );
      if (Option.isNone(entry)) {
        return yield* new ReconnectLoopError({ message: "Environment is not registered here." });
      }
      const trafficBefore = input.readTraffic();
      tally = { jsonDown: 0, jsonUp: 0, sentRevisions: 0, reusedCatalogs: 0 };
      const preparing = yield* Clock.currentTimeMillis;
      const prepared = yield* resolver.prepare(entry.value);
      const started = yield* Clock.currentTimeMillis;
      const result = yield* Effect.gen(function* () {
        const session = yield* factory.connect(prepared);
        const config = yield* session.initialConfig;
        const configMs = (yield* Clock.currentTimeMillis) - started;
        const omitIdleSubagents = config.shellThreadFamilies === true;
        const omitOlderSettled = config.shellOlderSettled === true && omitIdleSubagents;
        const current: OrchestrationV2ShellSnapshot | null = shell;
        const resume =
          current !== null && arm !== "warmup"
            ? {
                afterSequence: current.snapshotSequence,
                ...(arm === "full"
                  ? {}
                  : { projectsRevision: projectSnapshotRevision(current.projects) }),
              }
            : {};
        const items = yield* session.client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({
          ...resume,
          ...(omitIdleSubagents ? { omitIdleSubagents: true } : {}),
          ...(omitOlderSettled ? { omitOlderSettled: true } : {}),
          requestCompletionMarker: true,
          ...(pipelineArm === "old" ? { legacyCatchUpPacing: true } : {}),
        }).pipe(
          Stream.takeUntil((item) => item.kind === "synchronized"),
          Stream.runCollect,
        );
        const syncedMs = (yield* Clock.currentTimeMillis) - started;
        return { configMs, syncedMs, items: Array.from(items) };
      }).pipe(
        Effect.provideService(AttemptReconnectArm, pipelineArm),
        Effect.scoped,
        Effect.timeout("60 seconds"),
        Effect.exit,
      );
      const at = DateTime.formatIso(yield* DateTime.now);
      const base = {
        at,
        round,
        arm,
        prepareMs: started - preparing,
        ...tally,
        appTraffic: trafficDelta(trafficBefore, input.readTraffic()),
      };
      if (Exit.isFailure(result)) {
        input.onRow({
          ...base,
          ok: false,
          error: String(result.cause).slice(0, 300),
          configMs: 0,
          syncedMs: 0,
          projectFrames: 0,
        });
        return;
      }
      applyItems(result.value.items);
      input.onRow({
        ...base,
        ok: true,
        configMs: result.value.configMs,
        syncedMs: result.value.syncedMs,
        projectFrames: result.value.items.filter((item) => item.kind === "snapshot").length,
      });
    });

    yield* attempt(-1, "warmup");
    yield* Effect.sleep("5 seconds");
    yield* attempt(-1, "warmup");
    const arms: ReadonlyArray<ReconnectLoopArm> = ["full", "cached", "restart"];
    for (let round = 0; round < input.rounds; round++) {
      const order = yield* Random.shuffle(arms);
      for (const arm of order) yield* attempt(round, arm);
    }
  },
  Effect.provideService(SnapshotExperiment, null),
  Effect.provideService(AttemptSnapshotTrial, null),
);
