import { OrchestrationProjectShell, ServerProvider } from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";

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

// Both sides hash the encoded wire form: encoding normalizes fields (it trims model names), and
// hashing a decoded or in-memory copy would let client and server disagree without any error.
const encodeProvider = Schema.encodeSync(ServerProvider);
const encodeProjects = Schema.encodeSync(Schema.Array(OrchestrationProjectShell));

const catalogRevisions = new WeakMap<ServerProvider, string>();

export function providerCatalogRevision(provider: ServerProvider): string {
  const cached = catalogRevisions.get(provider);
  if (cached !== undefined) return cached;
  const { models, skills, slashCommands } = encodeProvider(provider);
  const value = revision({ models, skills, slashCommands });
  catalogRevisions.set(provider, value);
  return value;
}

export function projectSnapshotRevision(
  projects: ReadonlyArray<OrchestrationProjectShell>,
): string {
  return revision(
    [...encodeProjects(projects)].sort((left, right) =>
      left.id < right.id ? -1 : left.id > right.id ? 1 : 0,
    ),
  );
}
