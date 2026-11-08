import type { OrchestrationProjectShell, ServerProvider } from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";

const revision = (value: unknown) =>
  Hex.encode(
    sha256(
      new TextEncoder().encode(
        JSON.stringify(value, (_key, item: unknown) =>
          item !== null && typeof item === "object" && !Array.isArray(item)
            ? Object.fromEntries(
                Object.entries(item).sort(([left], [right]) =>
                  left < right ? -1 : left > right ? 1 : 0,
                ),
              )
            : item,
        ),
      ),
    ),
  ).slice(0, 32);

const catalogRevisions = new WeakMap<ServerProvider, string>();

export function providerCatalogRevision(provider: ServerProvider): string {
  const cached = catalogRevisions.get(provider);
  if (cached !== undefined) return cached;
  const value = revision({
    models: provider.models,
    skills: provider.skills,
    slashCommands: provider.slashCommands,
  });
  catalogRevisions.set(provider, value);
  return value;
}

export function projectSnapshotRevision(
  projects: ReadonlyArray<OrchestrationProjectShell>,
): string {
  return revision(
    [...projects].sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)),
  );
}
