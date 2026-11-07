import type { ServerConfigStreamEvent } from "@t3tools/contracts";
import { providerCatalogRevision } from "@t3tools/shared/reconnectSnapshots";

export function omitCachedProviderCatalogs(
  event: ServerConfigStreamEvent,
  revisions: Readonly<Record<string, string>> | undefined,
): ServerConfigStreamEvent {
  if (event.type !== "snapshot" || revisions === undefined) return event;
  const cachedProviderCatalogs: Record<string, string> = {};
  const providers = event.config.providers.map((provider) => {
    const known = revisions[provider.instanceId];
    if (known === undefined || known !== providerCatalogRevision(provider)) return provider;
    cachedProviderCatalogs[provider.instanceId] = known;
    return { ...provider, models: [], skills: [], slashCommands: [] };
  });
  return Object.keys(cachedProviderCatalogs).length === 0
    ? event
    : { ...event, config: { ...event.config, providers }, cachedProviderCatalogs };
}
