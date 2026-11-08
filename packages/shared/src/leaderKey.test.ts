import { describe, expect, it } from "vite-plus/test";
import { createLeaderKeyController, LEADER_TIMEOUT_MS, matchesLeaderStroke } from "./leaderKey.ts";
import {
  compileResolvedKeybindingsConfig,
  mergeWithDefaultKeybindings,
  keybindingShortcutInput,
  parseAppKeybindingShortcut,
  parseKeybindingShortcut,
} from "./keybindings.ts";

const key = (key: string, type: "keydown" | "keyup" = "keydown", repeat = false) => ({
  key,
  code: key,
  type,
  repeat,
});

describe("leader sequences", () => {
  it("consumes the prefix, successor, releases and repeats exactly once", () => {
    const controller = createLeaderKeyController();
    expect(controller.handle(key("."), true, 10)).toBe("prefix");
    expect(controller.handle(key(".", "keydown", true), true, 20)).toBe("consume");
    expect(controller.handle(key(".", "keyup"), false, 30)).toBe("consume");
    expect(controller.handle(key("Control", "keyup"), false, 40)).toBe("pass");
    expect(controller.handle(key("Shift"), false, 50)).toBe("pass");
    expect(controller.handle(key("N"), false, 60)).toBe("stroke");
    expect(controller.active(60)).toBe(false);
    expect(controller.handle(key("N", "keydown", true), false, 70)).toBe("consume");
    expect(controller.handle(key("N", "keyup"), false, 80)).toBe("consume");
    expect(controller.handle(key("n"), false, 90)).toBe("pass");
  });

  it("Escape and blur cancellation retain consumed releases", () => {
    const controller = createLeaderKeyController();
    controller.handle(key("."), true, 10);
    expect(controller.handle(key("Escape"), false, 20)).toBe("consume");
    expect(controller.handle(key("n"), false, 30)).toBe("pass");
    controller.cancel();
    expect(controller.handle(key(".", "keyup"), false, 40)).toBe("consume");
    expect(controller.handle(key("Escape", "keyup"), false, 50)).toBe("consume");
  });

  it("expires and passes unmatched successors without swallowing their releases", () => {
    const controller = createLeaderKeyController();
    controller.handle(key("."), true, 10);
    expect(controller.handle(key("n"), false, 10 + LEADER_TIMEOUT_MS)).toBe("pass");
    controller.handle(key("."), true, 3000);
    expect(controller.handle(key("x"), false, 3010)).toBe("stroke");
    controller.release(key("x"));
    expect(controller.handle(key("x", "keyup"), false, 3020)).toBe("pass");
    expect(controller.handle(key("n"), false, 3030)).toBe("pass");
  });

  it("cancels for composition and reset", () => {
    const controller = createLeaderKeyController();
    controller.handle(key("."), true, 10);
    expect(controller.handle({ ...key("n"), isComposing: true }, false, 20)).toBe("pass");
    expect(controller.active(20)).toBe(false);
    controller.reset();
    expect(controller.handle(key(".", "keyup"), false, 30)).toBe("pass");
  });

  it("lets a fresh press replace a suppression whose release was lost on blur", () => {
    const controller = createLeaderKeyController();
    controller.handle(key("."), true, 10);
    controller.cancel();
    expect(controller.handle(key("."), false, 20)).toBe("pass");
    expect(controller.handle(key(".", "keyup"), false, 30)).toBe("pass");
  });

  it("supports shifted numbers and punctuation without physical QWERTY positions", () => {
    const input = {
      key: ">",
      code: "KeyE",
      ctrlKey: false,
      metaKey: false,
      shiftKey: true,
      altKey: false,
    };
    expect(matchesLeaderStroke(input, parseKeybindingShortcut("shift+.")!, false)).toBe(true);
    expect(
      matchesLeaderStroke(
        { ...input, key: "!", code: "Digit1" },
        parseKeybindingShortcut("shift+1")!,
        false,
      ),
    ).toBe(true);
    expect(matchesLeaderStroke(input, parseKeybindingShortcut("shift+e")!, false)).toBe(false);
  });

  it("matches logical Dvorak keys and exact modifiers", () => {
    const shortcut = parseKeybindingShortcut("ctrl+.")!;
    const input = {
      key: ".",
      code: "KeyE",
      ctrlKey: true,
      metaKey: false,
      shiftKey: false,
      altKey: false,
    };
    expect(matchesLeaderStroke(input, shortcut, false)).toBe(true);
    expect(matchesLeaderStroke({ ...input, key: "v", code: "Period" }, shortcut, false)).toBe(
      false,
    );
    expect(matchesLeaderStroke({ ...input, altKey: true }, shortcut, false)).toBe(false);
  });
});

describe("leader syntax", () => {
  it.each(["leader+n", "leader+shift+n", "ctrl+leader+1", "leader++", "leader+space"])(
    "round trips %s",
    (input) => {
      const shortcut = parseAppKeybindingShortcut(input)!;
      expect(shortcut).not.toBeNull();
      expect(parseAppKeybindingShortcut(keybindingShortcutInput(shortcut))).toEqual(shortcut);
      expect(parseKeybindingShortcut(input)).toBeNull();
    },
  );
  it.each(["", "leader", "leader+", "leader+ctrl", "leader+leader+n", "leader+n+p"])(
    "rejects %s",
    (input) => {
      expect(parseAppKeybindingShortcut(input)).toBeNull();
    },
  );
});

describe("older environment defaults", () => {
  it("moves the untouched project-thread default to leader+c on older servers", () => {
    const stock = compileResolvedKeybindingsConfig([
      { key: "mod+shift+n", command: "chat.newLocal", when: "!terminalFocus" },
    ]);
    const merged = mergeWithDefaultKeybindings(stock, { leaderBindingsSupported: false });
    expect(
      merged
        .filter((rule) => rule.command === "chat.newLocal")
        .map((rule) => keybindingShortcutInput(rule.shortcut)),
    ).toEqual(["leader+c"]);
    expect(
      mergeWithDefaultKeybindings(stock, { leaderBindingsSupported: true }).filter(
        (rule) => rule.command === "chat.newLocal",
      ),
    ).toEqual(stock);
  });

  it("preserves customized project-thread bindings and occupied leader+c on older servers", () => {
    for (const input of [
      [{ key: "alt+n", command: "chat.newLocal", when: "!terminalFocus" }],
      [{ key: "mod+shift+n", command: "chat.newLocal", when: "!editableFocus" }],
      [
        { key: "mod+shift+n", command: "chat.newLocal", when: "!terminalFocus" },
        { key: "leader+c", command: "thread.next" },
      ],
    ] as const) {
      const custom = compileResolvedKeybindingsConfig(input);
      expect(
        mergeWithDefaultKeybindings(custom, { leaderBindingsSupported: false }).filter(
          (rule) => rule.command === "chat.newLocal",
        ),
      ).toEqual(custom.filter((rule) => rule.command === "chat.newLocal"));
    }
  });

  it("adds leader defaults beside stock shortcuts, honors customization and removals on new servers", () => {
    const stock = compileResolvedKeybindingsConfig([
      { key: "mod+shift+]", command: "thread.next" },
    ]);
    expect(
      mergeWithDefaultKeybindings(stock, { leaderBindingsSupported: false }).some(
        (rule) => rule.command === "thread.next" && "leader" in rule.shortcut,
      ),
    ).toBe(true);
    expect(
      mergeWithDefaultKeybindings(stock, { leaderBindingsSupported: true }).some(
        (rule) => rule.command === "thread.next" && "leader" in rule.shortcut,
      ),
    ).toBe(false);
    const custom = compileResolvedKeybindingsConfig([{ key: "alt+n", command: "thread.next" }]);
    expect(
      mergeWithDefaultKeybindings(custom, { leaderBindingsSupported: false }).filter(
        (rule) => rule.command === "thread.next",
      ),
    ).toEqual(custom);
  });
});
