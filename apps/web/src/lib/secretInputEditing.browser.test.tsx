import { compileResolvedKeybindingsConfig, isMacPlatform } from "@t3tools/shared/keybindings";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import { render } from "../../test/render";
import { Input } from "../components/ui/input";
import { resolveShortcutCommand } from "../keybindings";
import { installSecretInputEditing } from "./secretInputEditing";

const keybindings = compileResolvedKeybindingsConfig([
  { key: "ctrl+k", command: "commandPalette.toggle" },
]);

function press(target: HTMLElement, modifiers: KeyboardEventInit = {}) {
  const event = new KeyboardEvent("keydown", {
    key: "k",
    ctrlKey: true,
    bubbles: true,
    cancelable: true,
    ...modifiers,
  });
  target.dispatchEvent(event);
  return event;
}

function ControlledInput({ type }: { type: "password" | "masked" }) {
  const [value, setValue] = useState("before-after");
  return (
    <>
      <Input
        type={type === "password" ? "password" : "text"}
        data-secret-input={type === "masked" || undefined}
        value={value}
        onChange={(event) => setValue(event.target.value)}
      />
      <output>{value}</output>
    </>
  );
}

describe("Ctrl-K in secret inputs", () => {
  it.each(["password", "masked"] as const)(
    "reserves Ctrl-K for editing a %s input instead of opening the command palette",
    async (type) => {
      await using screen = await render(<ControlledInput type={type} />);
      const element = screen.container.querySelector("input")!;
      const event = {
        key: "k",
        ctrlKey: true,
        altKey: false,
        metaKey: false,
        shiftKey: false,
        target: element,
      };
      for (const platform of ["Linux", "Win32", "MacIntel"]) {
        expect(resolveShortcutCommand(event, keybindings, { platform })).toBeNull();
        expect(
          resolveShortcutCommand({ ...event, target: document.body }, keybindings, { platform }),
        ).toBe("commandPalette.toggle");
      }
    },
  );

  it.each([
    { type: "password", native: true },
    { type: "masked", native: true },
    { type: "password", native: false },
    { type: "masked", native: false },
  ] as const)(
    "deletes from the cursor to the end of a controlled $type input with native editing $native",
    async ({ type, native }) => {
      await using screen = await render(<ControlledInput type={type} />);
      using listeners = new DisposableStack();
      listeners.defer(installSecretInputEditing(window));
      using _execCommand = native ? null : vi.spyOn(document, "execCommand").mockReturnValue(false);
      const element = screen.container.querySelector("input")!;
      const changes: InputEvent[] = [];
      element.addEventListener("input", (event) => changes.push(event));
      await userEvent.click(element);
      element.setSelectionRange(7, 9);

      await userEvent.keyboard("{Control>}k{/Control}");

      await expect.element(element).toHaveValue("before-");
      await expect.element(screen.getByRole("status")).toHaveTextContent("before-");
      expect(element.selectionStart).toBe(7);
      expect(element.selectionEnd).toBe(7);
      expect(changes).toHaveLength(1);
      expect(changes[0]?.data).toBeNull();
      expect(press(element).defaultPrevented).toBe(true);
      expect(changes).toHaveLength(1);

      if (native) {
        await userEvent.keyboard(
          isMacPlatform(navigator.platform) ? "{Meta>}z{/Meta}" : "{Control>}z{/Control}",
        );
        await expect.element(element).toHaveValue("before-after");
        await expect.element(screen.getByRole("status")).toHaveTextContent("before-after");
      }

      element.setSelectionRange(0, 0);
      await userEvent.keyboard("{Control>}k{/Control}");
      await expect.element(element).toHaveValue("");
      await expect.element(screen.getByRole("status")).toBeEmptyDOMElement();
      expect(press(element).defaultPrevented).toBe(true);
    },
  );

  it("leaves other modifiers, IME, read-only fields, and shortcut capture alone", async () => {
    await using screen = await render(<Input type="password" defaultValue="before-after" />);
    using listeners = new DisposableStack();
    listeners.defer(installSecretInputEditing(window));
    const element = screen.container.querySelector("input")!;
    for (const modifiers of [
      { ctrlKey: false },
      { altKey: true },
      { metaKey: true },
      { shiftKey: true },
      { isComposing: true },
      { keyCode: 229 },
    ]) {
      expect(press(element, modifiers).defaultPrevented).toBe(false);
    }
    element.readOnly = true;
    expect(press(element).defaultPrevented).toBe(false);
    element.readOnly = false;
    element.disabled = true;
    expect(press(element).defaultPrevented).toBe(false);
    element.disabled = false;
    element.setAttribute("data-keybinding-capture", "");
    expect(press(element).defaultPrevented).toBe(false);
    element.removeAttribute("data-keybinding-capture");
    element.type = "text";
    expect(press(element).defaultPrevented).toBe(false);
    await expect.element(element).toHaveValue("before-after");
  });
});
