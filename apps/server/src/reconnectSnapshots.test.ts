import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
  type ServerConfig,
  type ServerConfigStreamEvent,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { providerCatalogRevision } from "@t3tools/shared/reconnectSnapshots";
import { omitCachedProviderCatalogs } from "./reconnectSnapshots.ts";

const encodeProvider = Schema.encodeSync(ServerProvider);
const decodeProvider = Schema.decodeUnknownSync(ServerProvider);

const provider: ServerProvider = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: "1",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-11-07T00:00:00Z",
  models: [{ slug: "luna", name: "Luna", isCustom: false, capabilities: null }],
  skills: [{ name: "test", path: "/skills/test", enabled: true }],
  slashCommands: [{ name: "help" }],
};

describe("cached provider catalogs", () => {
  it.each(["absent", "match", "untrimmed", "stale", "changed", "removed"] as const)(
    "returns a complete first response with %s client revisions",
    (mode) => {
      const current =
        mode === "changed"
          ? { ...provider, skills: [] }
          : mode === "untrimmed"
            ? // The client only ever sees the trimmed name the wire encoding sends.
              { ...provider, models: [{ ...provider.models[0]!, name: "Luna " }] }
            : { ...provider, checkedAt: "2026-11-07T01:00:00Z" };
      const config = { providers: mode === "removed" ? [] : [current] } as unknown as ServerConfig;
      const snapshot: ServerConfigStreamEvent = { version: 1, type: "snapshot", config };
      // The client hashes its decoded copy of what the server sent.
      const received = decodeProvider(encodeProvider(current));
      const revisions =
        mode === "absent"
          ? undefined
          : {
              codex:
                mode === "stale"
                  ? "stale"
                  : providerCatalogRevision(mode === "untrimmed" ? received : provider),
            };
      const next = omitCachedProviderCatalogs(snapshot, revisions);
      expect(next.type).toBe("snapshot");
      if (next.type !== "snapshot") throw new Error("Expected snapshot");
      if (mode === "match" || mode === "untrimmed") {
        expect(next.cachedProviderCatalogs).toEqual({ codex: providerCatalogRevision(provider) });
        expect(next.config.providers[0]).toEqual({
          ...current,
          models: [],
          skills: [],
          slashCommands: [],
        });
      } else {
        expect(next).toBe(snapshot);
      }
    },
  );
});
