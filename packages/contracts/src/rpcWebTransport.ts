import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

/** A ready listener. Reachability still depends on the client's network route. */
export const RpcWebTransportAdvertisement = Schema.Struct({
  url: TrimmedNonEmptyString,
  wireVersion: Schema.Int,
});
export type RpcWebTransportAdvertisement = typeof RpcWebTransportAdvertisement.Type;
