import type { EnvironmentId, ServerConfig } from "@t3tools/contracts";
import { mergeWithDefaultKeybindings } from "@t3tools/shared/keybindings";
import { Atom } from "effect/unstable/reactivity";

type KeybindingsConfig = Pick<ServerConfig, "keybindings">;

export function createAppKeybindingsAtom(input: {
  readonly primaryEnvironmentIdAtom: Atom.Atom<EnvironmentId | null>;
  readonly primaryConfigAtom: Atom.Atom<KeybindingsConfig | null>;
  readonly environmentServerConfigsAtom: Atom.Atom<ReadonlyMap<EnvironmentId, KeybindingsConfig>>;
}) {
  return Atom.make((get) => {
    // A desktop with its local backend disabled has no primary environment.
    // Its global shortcuts follow the first available connected server instead.
    const config =
      get(input.primaryEnvironmentIdAtom) === null
        ? get(input.environmentServerConfigsAtom).values().next().value
        : get(input.primaryConfigAtom);
    return mergeWithDefaultKeybindings(config?.keybindings ?? []);
  }).pipe(Atom.withLabel("web-primary-server-keybindings"));
}
