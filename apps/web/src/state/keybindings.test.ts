import { EnvironmentId, type ServerConfig } from "@t3tools/contracts";
import { compileResolvedKeybindingsConfig } from "@t3tools/shared/keybindings";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { resolveShortcutCommand, type ShortcutEventLike } from "../keybindings";
import { createAppKeybindingsAtom } from "./keybindings";

const LOCAL = EnvironmentId.make("local");
const REMOTE = EnvironmentId.make("remote");
const OTHER_REMOTE = EnvironmentId.make("other-remote");
type KeybindingsConfig = Pick<ServerConfig, "keybindings">;

function config(settleKey = "mod+d"): KeybindingsConfig {
  return {
    keybindings: compileResolvedKeybindingsConfig([
      { key: "mod+alt+d", command: "terminal.split", when: "terminalFocus" },
      { key: settleKey, command: "thread.settle", when: "!terminalFocus" },
    ]),
  };
}

const registries: AtomRegistry.AtomRegistry[] = [];
afterEach(() => {
  for (const registry of registries.splice(0)) registry.dispose();
});

function harness() {
  const primaryEnvironmentIdAtom = Atom.make<EnvironmentId | null>(null);
  const primaryConfigAtom = Atom.make<KeybindingsConfig | null>(null);
  const environmentServerConfigsAtom = Atom.make<ReadonlyMap<EnvironmentId, KeybindingsConfig>>(
    new Map(),
  );
  const keybindings = createAppKeybindingsAtom({
    primaryEnvironmentIdAtom,
    primaryConfigAtom,
    environmentServerConfigsAtom,
  });
  const registry = AtomRegistry.make();
  registries.push(registry);
  function command(overrides: Partial<ShortcutEventLike> = {}, terminalFocus = false) {
    return resolveShortcutCommand(
      { key: "d", ctrlKey: true, altKey: false, metaKey: false, shiftKey: false, ...overrides },
      registry.get(keybindings),
      { platform: "Linux", context: { terminalFocus } },
    );
  }
  return {
    registry,
    primaryEnvironmentIdAtom,
    primaryConfigAtom,
    environmentServerConfigsAtom,
    command,
  };
}

describe("app keybindings without a local backend", () => {
  it("uses a connected server's settle and split bindings", () => {
    const h = harness();
    h.registry.set(h.environmentServerConfigsAtom, new Map([[REMOTE, config()]]));
    expect(h.command()).toBe("thread.settle");
    expect(h.command({}, true)).toBeNull();
    expect(h.command({ altKey: true }, true)).toBe("terminal.split");
  });

  it("applies connected-server shortcut updates without remounting", () => {
    const h = harness();
    h.registry.set(h.environmentServerConfigsAtom, new Map([[REMOTE, config()]]));
    expect(h.command()).toBe("thread.settle");
    h.registry.set(h.environmentServerConfigsAtom, new Map([[REMOTE, config("mod+x")]]));
    expect(h.command()).toBe("diff.toggle");
    expect(h.command({ key: "x" })).toBe("thread.settle");
  });

  it("uses the first available server and falls back when it is removed", () => {
    const h = harness();
    h.registry.set(
      h.environmentServerConfigsAtom,
      new Map([
        [REMOTE, config()],
        [OTHER_REMOTE, config("mod+x")],
      ]),
    );
    expect(h.command()).toBe("thread.settle");
    h.registry.set(h.environmentServerConfigsAtom, new Map([[OTHER_REMOTE, config("mod+x")]]));
    expect(h.command({ key: "x" })).toBe("thread.settle");
  });

  it("keeps the primary environment's shortcuts when one exists", () => {
    const h = harness();
    h.registry.set(h.environmentServerConfigsAtom, new Map([[REMOTE, config()]]));
    expect(h.command()).toBe("thread.settle");
    h.registry.set(h.primaryEnvironmentIdAtom, LOCAL);
    expect(h.command()).toBe("diff.toggle");
    h.registry.set(h.primaryConfigAtom, config("mod+x"));
    expect(h.command()).toBe("diff.toggle");
    expect(h.command({ key: "x" })).toBe("thread.settle");
  });

  it("uses defaults until a server config is available", () => {
    const h = harness();
    expect(h.command()).toBe("diff.toggle");
    expect(h.command({}, true)).toBe("terminal.split");
    expect(h.command({ key: "s", shiftKey: true })).toBe("thread.settle");
  });
});
