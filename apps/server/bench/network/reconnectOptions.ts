import * as Schema from "effect/Schema";
export const VariantOptions = Schema.Struct({
  websocketHeaders: Schema.Boolean,
  skipDescriptorOnHttpsReconnect: Schema.Boolean,
});
export type VariantOptions = typeof VariantOptions.Type;
export const defaultVariantOptions: VariantOptions = {
  websocketHeaders: true,
  skipDescriptorOnHttpsReconnect: false,
};
