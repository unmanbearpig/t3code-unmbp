import type { EnvironmentId, ServerConfig } from "@t3tools/contracts";
import { mergeWithDefaultKeybindings } from "@t3tools/shared/keybindings";
import { Atom } from "effect/reactivity";

type KeybindingsConfig = Pick<ServerConfig, "keybindings"> &
  Partial<Pick<ServerConfig, "environment">>;

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
    return mergeWithDefaultKeybindings(config?.keybindings ?? [], {
      leaderBindingsSupported: config?.environment?.capabilities.leaderKeybindings === true,
    });
  }).pipe(Atom.withLabel("web-primary-server-keybindings"));
}
