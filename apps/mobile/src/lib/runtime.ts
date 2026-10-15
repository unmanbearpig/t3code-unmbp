import { ConnectionDiagnostics } from "@t3tools/client-runtime/connection";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Socket from "effect/socket/Socket";

import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import { ClientCapabilities } from "@t3tools/client-runtime/platform";

import * as Dpop from "../features/cloud/dpop";
import * as ManagedRelayLayer from "../features/cloud/managedRelayLayer";
import { resolveCloudPublicConfig } from "../features/cloud/publicConfig";
import * as Tracing from "../features/observability/tracing";
import * as Persistence from "../persistence/layer";
import { disposeOnFoundationReplace, type FoundationHotModule } from "./foundation-fast-refresh";

declare const module: { readonly hot?: FoundationHotModule } | undefined;

function configuredRelayUrl(): string {
  return resolveCloudPublicConfig().relay.url ?? "http://relay.invalid";
}

const layerHttpClient = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(HttpClient.HttpClient, ConnectionDiagnostics.observePreparationHttp),
).pipe(Layer.provide(layerRemoteHttpClient(fetch)));
// Expo's DOM declaration omits React Native's third constructor argument.
const NativeWebSocket = WebSocket as new (
  url: string,
  protocols: undefined,
  options: { readonly headers: Readonly<Record<string, string>> },
) => WebSocket;
const layerWebSocketWithHeaders = Layer.succeed(
  ClientCapabilities.WebSocketConstructorWithHeaders,
  (url, headers) => new NativeWebSocket(url, undefined, { headers }),
);

type RuntimeLayerSource =
  | ReturnType<typeof ManagedRelayLayer.layer>
  | typeof Socket.layerWebSocketConstructorGlobal
  | typeof layerWebSocketWithHeaders
  | typeof Dpop.layer
  | typeof layerHttpClient
  | typeof Persistence.layer
  | typeof Tracing.layer;

const layerRuntime = Layer.mergeAll(
  ManagedRelayLayer.layer(configuredRelayUrl()),
  Socket.layerWebSocketConstructorGlobal,
  layerWebSocketWithHeaders,
).pipe(
  Layer.provideMerge(Dpop.layer),
  Layer.provideMerge(layerHttpClient),
  Layer.provideMerge(Tracing.layer.pipe(Layer.provide(layerHttpClient))),
  Layer.provideMerge(Persistence.layer),
);

export const runtime: ManagedRuntime.ManagedRuntime<
  Layer.Success<RuntimeLayerSource>,
  Layer.Error<RuntimeLayerSource>
> = ManagedRuntime.make(layerRuntime);

export const layer: Layer.Layer<
  Layer.Success<RuntimeLayerSource>,
  Layer.Error<RuntimeLayerSource>
> = Layer.effectContext(runtime.contextEffect);

disposeOnFoundationReplace(typeof module === "undefined" ? undefined : module.hot, () =>
  runtime.dispose(),
);
