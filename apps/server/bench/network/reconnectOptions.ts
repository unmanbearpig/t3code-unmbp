import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
export const VariantOptions = Schema.Struct({
  helloHandshake: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  helloSupportCached: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  websocketHeaders: Schema.Boolean,
  skipDescriptorOnHttpsReconnect: Schema.Boolean,
});
export type VariantOptions = typeof VariantOptions.Type;
export const defaultVariantOptions: VariantOptions = {
  helloHandshake: false,
  helloSupportCached: true,
  websocketHeaders: true,
  skipDescriptorOnHttpsReconnect: false,
};
