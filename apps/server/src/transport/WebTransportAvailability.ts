import type { RpcWebTransportAdvertisement } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

/** Discovery reads this independent service, never the listener's dependencies. */
export class WebTransportAvailability extends Context.Service<
  WebTransportAvailability,
  {
    readonly get: Effect.Effect<Option.Option<RpcWebTransportAdvertisement>>;
    readonly set: (endpoint: Option.Option<RpcWebTransportAdvertisement>) => Effect.Effect<void>;
  }
>()("t3/transport/WebTransportAvailability") {}

const make = Effect.gen(function* () {
  const endpoint = yield* Ref.make(Option.none<RpcWebTransportAdvertisement>());
  return WebTransportAvailability.of({
    get: Ref.get(endpoint),
    set: (value) => Ref.set(endpoint, value),
  });
});

export const layer = Layer.effect(WebTransportAvailability, make);
