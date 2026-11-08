import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { ReconnectComparison } from "@t3tools/client-runtime/connection";
import * as Effect from "effect/Effect";
import { MobileReconnectComparison } from "../features/diagnostics/reconnect-comparison";
import { Connection } from "@t3tools/client-runtime/connection";
import { ShellSnapshotLoader } from "@t3tools/client-runtime/state/shell";
import { PullRequestDiffLoader } from "@t3tools/client-runtime/state/pull-requests";
import {
  BoundedThreadSnapshotLoader,
  ThreadHistoryController,
} from "@t3tools/client-runtime/state/threads";
import * as Layer from "effect/Layer";
import { Atom } from "effect/reactivity";

import type { FoundationHotModule } from "../lib/foundation-fast-refresh";
import { hotSwappableAtomRuntime } from "../lib/hot-swappable-atom-runtime";
import * as Runtime from "../lib/runtime";
import { appAtomRegistry } from "../state/atom-registry";
import * as BackgroundActivity from "./background-activity";
import * as ConnectionPlatform from "./platform";

declare const module: { readonly hot?: FoundationHotModule } | undefined;

const layerProvidedConnectionPlatform = ConnectionPlatform.layer.pipe(Layer.provide(Runtime.layer));

const layerSnapshotLoader = Layer.mergeAll(
  PullRequestDiffLoader.layer,
  BoundedThreadSnapshotLoader.layer,
  ShellSnapshotLoader.layer,
  ThreadHistoryController.layer,
);

type ConnectionLayerSource =
  | typeof Connection.layer
  | typeof layerSnapshotLoader
  | typeof Runtime.layer
  | typeof ConnectionPlatform.layer
  | typeof BackgroundActivity.layerObserver
  | typeof BackgroundActivity.layerReporter;

const layerComparison = Layer.effect(
  ReconnectComparison.ReconnectComparison,
  Effect.map(MobileReconnectComparison, (service) => service.control),
).pipe(Layer.provide(Runtime.layer));

const layerProvidedClientConnection = layerSnapshotLoader.pipe(
  Layer.provideMerge(
    Connection.layerWithOptions({
      usageLimitSources: true,
      usageLimitsCommand: true,
      providerCatalogsOnDemand: true,
      providerCatalogDigests: true,
      providerStatusDeltas: true,
    }),
  ),
  Layer.provideMerge(
    Layer.mergeAll(
      Runtime.layer,
      layerProvidedConnectionPlatform,
      layerComparison,
      BackgroundActivity.layerObserver,
    ),
  ),
);

const layerUploads = Layer.effectDiscard(
  Effect.gen(function* () {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const service = yield* MobileReconnectComparison;
    const connected = SubscriptionRef.changes(registry.entries).pipe(
      Stream.switchMap((entries) =>
        Stream.mergeAll({ concurrency: "unbounded" })(
          [...entries.keys()].map((environmentId) =>
            registry.stateChanges(environmentId).pipe(
              Stream.filter((state) => state.phase === "connected"),
              Stream.map(() => environmentId),
            ),
          ),
        ),
      ),
    );
    yield* Stream.merge(connected, Stream.fromQueue(service.recordsSaved)).pipe(
      Stream.runForEach(() =>
        service.isMeasuring()
          ? Effect.void
          : SubscriptionRef.get(registry.entries).pipe(
              Effect.flatMap((entries) =>
                Effect.forEach(
                  [...entries.keys()],
                  (environmentId) => service.upload(environmentId, registry).pipe(Effect.ignore),
                  { discard: true },
                ),
              ),
            ),
      ),
      Effect.forkScoped,
    );
  }),
);
const layerConnection = Layer.merge(BackgroundActivity.layerReporter, layerUploads).pipe(
  Layer.provideMerge(layerProvidedClientConnection),
);

export const connectionAtomRuntime: Atom.AtomRuntime<
  Layer.Success<ConnectionLayerSource>,
  Layer.Error<ConnectionLayerSource>
> = hotSwappableAtomRuntime({
  id: "t3.mobile.connection-runtime",
  hotModule: typeof module === "undefined" ? undefined : module.hot,
  registry: appAtomRegistry,
  layer: layerConnection,
});
