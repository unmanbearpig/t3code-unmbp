// @vitest-environment jsdom
import { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  compileResolvedKeybindingsConfig,
  DEFAULT_RESOLVED_KEYBINDINGS,
} from "@t3tools/shared/keybindings";

import { resolveShortcutCommand } from "../keybindings";
import { installTextareaNewlineShortcut } from "./textboxNewline";

const cleanups: (() => void)[] = [];
afterEach(() => {
  cleanups
    .splice(0)
    .toReversed()
    .forEach((cleanup) => cleanup());
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  document.body.replaceChildren();
});

function press(target: HTMLElement, modifiers: KeyboardEventInit = {}) {
  const event = new KeyboardEvent("keydown", {
    key: "j",
    ctrlKey: true,
    bubbles: true,
    cancelable: true,
    ...modifiers,
  });
  target.dispatchEvent(event);
  return event;
}

function textbox(value = "hello") {
  const textarea = document.body.appendChild(document.createElement("textarea"));
  textarea.value = value;
  textarea.focus();
  textarea.setSelectionRange(2, 2);
  cleanups.push(installTextareaNewlineShortcut(window));
  return textarea;
}

describe("Ctrl-J in multiline text boxes", () => {
  it("inserts at the caret and replaces selected text", () => {
    const textarea = textbox();
    const changes: string[] = [];
    textarea.addEventListener("input", () => changes.push(textarea.value));
    expect(press(textarea).defaultPrevented).toBe(true);
    expect(textarea.value).toBe("he\nllo");
    expect(textarea.selectionStart).toBe(3);
    textarea.setSelectionRange(1, 5);
    press(textarea);
    expect(textarea.value).toBe("h\no");
    expect(textarea.selectionStart).toBe(2);
    expect(changes).toEqual(["he\nllo", "h\no"]);
  });

  it("updates controlled React state through onChange", async () => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const host = document.body.appendChild(document.createElement("div"));
    const root = createRoot(host);
    cleanups.push(() => act(() => root.unmount()));
    cleanups.push(installTextareaNewlineShortcut(window));
    function ControlledTextbox() {
      const [value, setValue] = useState("hello");
      return (
        <>
          <textarea value={value} onChange={(event) => setValue(event.target.value)} />
          <output>{value}</output>
        </>
      );
    }
    await act(async () => root.render(<ControlledTextbox />));
    const textarea = host.querySelector("textarea")!;
    textarea.focus();
    textarea.setSelectionRange(2, 4);
    await act(async () => {
      press(textarea);
    });
    expect(textarea.value).toBe("he\no");
    expect(host.querySelector("output")?.textContent).toBe("he\no");
    expect(textarea.selectionStart).toBe(3);
  });

  it("uses native text insertion when available", () => {
    const textarea = textbox();
    const nativeInsert = vi.fn(() => {
      textarea.setRangeText("\n", textarea.selectionStart, textarea.selectionEnd, "end");
      return true;
    });
    Object.defineProperty(document, "execCommand", { configurable: true, value: nativeInsert });
    cleanups.push(() => {
      Reflect.deleteProperty(document, "execCommand");
    });
    press(textarea);
    expect(nativeInsert).toHaveBeenCalledWith("insertText", false, "\n");
    expect(textarea.value).toBe("he\nllo");
  });

  it("yields to newline editing even for a custom conflicting app shortcut", () => {
    const textarea = textbox();
    const event = press(textarea);
    const keybindings = compileResolvedKeybindingsConfig([
      { key: "ctrl+j", command: "commandPalette.toggle" },
    ]);
    expect(resolveShortcutCommand(event, keybindings, { platform: "Linux" })).toBeNull();
    expect(
      resolveShortcutCommand(
        {
          ...event,
          key: "j",
          ctrlKey: true,
          altKey: false,
          metaKey: false,
          shiftKey: false,
          target: document.body,
        },
        keybindings,
        { platform: "Linux" },
      ),
    ).toBe("commandPalette.toggle");
  });

  it("leaves terminals, single-line fields, and read-only text boxes alone", () => {
    const textarea = textbox();
    textarea.className = "t3-ghostty-input";
    expect(press(textarea).defaultPrevented).toBe(false);
    expect(
      resolveShortcutCommand(press(textarea), DEFAULT_RESOLVED_KEYBINDINGS, {
        platform: "Linux",
        context: { terminalFocus: true },
      }),
    ).toBe("terminal.toggle");
    textarea.className = "";
    textarea.readOnly = true;
    expect(press(textarea).defaultPrevented).toBe(false);
    textarea.readOnly = false;
    textarea.disabled = true;
    expect(press(textarea).defaultPrevented).toBe(false);
    const input = document.body.appendChild(document.createElement("input"));
    expect(press(input).defaultPrevented).toBe(false);
    expect(textarea.value).toBe("hello");
  });

  it("leaves IME and other modifier combinations alone", () => {
    const textarea = textbox();
    for (const modifiers of [
      { ctrlKey: false },
      { altKey: true },
      { metaKey: true },
      { shiftKey: true },
      { isComposing: true },
      { keyCode: 229 },
    ]) {
      expect(press(textarea, modifiers).defaultPrevented).toBe(false);
    }
    expect(textarea.value).toBe("hello");
  });

  it("respects maximum length and removes the listener on cleanup", () => {
    const textarea = textbox();
    textarea.maxLength = 5;
    press(textarea);
    expect(textarea.value).toBe("hello");
    textarea.setSelectionRange(2, 3);
    press(textarea);
    expect(textarea.value).toBe("he\nlo");
    cleanups.splice(0).forEach((cleanup) => cleanup());
    expect(press(textarea).defaultPrevented).toBe(false);
  });
});
