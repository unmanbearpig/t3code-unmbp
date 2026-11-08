import { ServerProvider, type ServerConfigStreamEvent } from "@t3tools/contracts";
import { providerCatalogRevision } from "@t3tools/shared/reconnectSnapshots";
import * as Schema from "effect/Schema";

const encodeProvider = Schema.encodeSync(ServerProvider);
const wireRevisions = new WeakMap<ServerProvider, string>();

// Encoding normalizes fields (it trims model names), so hash the catalog the client receives.
function wireCatalogRevision(provider: ServerProvider): string {
  const cached = wireRevisions.get(provider);
  if (cached !== undefined) return cached;
  const value = providerCatalogRevision(encodeProvider(provider));
  wireRevisions.set(provider, value);
  return value;
}

export function omitCachedProviderCatalogs(
  event: ServerConfigStreamEvent,
  revisions: Readonly<Record<string, string>> | undefined,
): ServerConfigStreamEvent {
  if (event.type !== "snapshot" || revisions === undefined) return event;
  const cachedProviderCatalogs: Record<string, string> = {};
  const providers = event.config.providers.map((provider) => {
    const known = revisions[provider.instanceId];
    if (known === undefined || known !== wireCatalogRevision(provider)) return provider;
    cachedProviderCatalogs[provider.instanceId] = known;
    return { ...provider, models: [], skills: [], slashCommands: [] };
  });
  return Object.keys(cachedProviderCatalogs).length === 0
    ? event
    : { ...event, config: { ...event.config, providers }, cachedProviderCatalogs };
}
