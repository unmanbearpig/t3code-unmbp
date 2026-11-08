import type { OrchestrationProjectShell, ServerProvider } from "@t3tools/contracts";

type ServerProviderCatalog = Pick<
  typeof ServerProvider.Encoded,
  "models" | "skills" | "slashCommands"
>;
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

const catalogRevisions = new WeakMap<object, string>();

/** Accepts the encoded wire form too, which is what the server must hash. */
export function providerCatalogRevision(
  provider: Pick<ServerProvider, "models" | "skills" | "slashCommands"> | ServerProviderCatalog,
): string {
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
  projects: ReadonlyArray<OrchestrationProjectShell | typeof OrchestrationProjectShell.Encoded>,
): string {
  return revision(
    [...projects].sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)),
  );
}
