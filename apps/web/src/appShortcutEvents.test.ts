// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vite-plus/test";
import { compileResolvedKeybindingsConfig } from "@t3tools/shared/keybindings";
import {
  addAppShortcutListener,
  dispatchLeaderShortcut,
  isLeaderShortcutEvent,
  registerLeaderShortcutHandler,
} from "./appShortcutEvents";
import { resolveShortcutCommand } from "./keybindings";

const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
});
const stroke = () => new KeyboardEvent("keydown", { key: "1", code: "Digit1", cancelable: true });

describe("app command routing", () => {
  it("gives a contextual picker priority and stops before thread navigation", () => {
    const bindings = compileResolvedKeybindingsConfig([
      { key: "leader+1", command: "thread.jump.1" },
    ]);
    const actions: string[] = [];
    cleanups.push(
      registerLeaderShortcutHandler(
        (event) => {
          if (resolveShortcutCommand(event, bindings, { platform: "Linux" })) {
            actions.push("fallback");
            event.preventDefault();
          }
        },
        false,
        true,
      ),
    );
    cleanups.push(
      addAppShortcutListener(window, (event) => {
        if (resolveShortcutCommand(event, bindings, { platform: "Linux" })) {
          actions.push("thread");
          event.preventDefault();
        }
      }),
    );
    cleanups.push(
      registerLeaderShortcutHandler((event) => {
        if (resolveShortcutCommand(event, bindings, { platform: "Linux" })) {
          actions.push("picker");
          event.preventDefault();
        }
      }, true),
    );
    const event = stroke();
    expect(dispatchLeaderShortcut(event)).toBe(true);
    expect(actions).toEqual(["picker"]);
    expect(isLeaderShortcutEvent(event)).toBe(false);
  });
  it("lets Settings record a shortcut before global commands claim it", () => {
    const input = document.createElement("input");
    input.dataset.keybindingCapture = "";
    document.body.append(input);
    const actions: string[] = [];
    cleanups.push(() => input.remove());
    cleanups.push(
      addAppShortcutListener(
        window,
        (event) => {
          actions.push("global");
          event.preventDefault();
        },
        true,
      ),
    );
    input.addEventListener("keydown", () => actions.push("recorder"));
    input.dispatchEvent(
      new KeyboardEvent("keydown", { key: "b", ctrlKey: true, bubbles: true, cancelable: true }),
    );
    expect(actions).toEqual(["recorder"]);
  });
  it("does not turn ordinary typing into a leader or physical shortcut", () => {
    const bindings = compileResolvedKeybindingsConfig([
      { key: "leader+1", command: "thread.jump.1" },
      { key: "ctrl+1", command: "thread.jump.2" },
    ]);
    const commands: string[] = [];
    cleanups.push(
      addAppShortcutListener(window, (event) => {
        const command = resolveShortcutCommand(event, bindings, { platform: "Linux" });
        if (command) {
          commands.push(command);
          event.preventDefault();
        }
      }),
    );
    window.dispatchEvent(stroke());
    expect(commands).toEqual([]);
    const event = stroke();
    expect(dispatchLeaderShortcut(event)).toBe(true);
    expect(commands).toEqual(["thread.jump.1"]);
    expect(resolveShortcutCommand(stroke(), bindings, { platform: "Linux" })).toBeNull();
  });
});
