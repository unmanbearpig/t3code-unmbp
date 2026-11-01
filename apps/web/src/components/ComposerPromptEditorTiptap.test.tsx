// @vitest-environment jsdom

import { DEFAULT_CLIENT_SETTINGS, type ClientSettings } from "@t3tools/contracts/settings";
import { act, createRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  ComposerPromptEditorTiptap,
  type ComposerPromptEditorHandle,
} from "./ComposerPromptEditorTiptap";

const settings = vi.hoisted(() => ({ emacs: false }));
vi.mock("~/hooks/useSettings", () => ({
  useClientSettings: <T,>(select: (value: ClientSettings) => T) =>
    select({ ...DEFAULT_CLIENT_SETTINGS, composerEmacsEditingEnabled: settings.emacs }),
}));
vi.mock("./chat/ComposerCodeBlockNodeView", async () => {
  const { NodeViewContent, NodeViewWrapper } = await import("@tiptap/react");
  return {
    ComposerCodeBlockNodeView: () => (
      <NodeViewWrapper>
        <NodeViewContent />
      </NodeViewWrapper>
    ),
  };
});

const roots: Root[] = [];
const rangeGeometry = ["getClientRects", "getBoundingClientRect"] as const;
const originalRangeGeometry = rangeGeometry.map((name) =>
  Object.getOwnPropertyDescriptor(Range.prototype, name),
);
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  Object.defineProperties(Range.prototype, {
    getClientRects: { configurable: true, value: () => [] },
    getBoundingClientRect: { configurable: true, value: () => new DOMRect() },
  });
});
afterEach(async () => {
  for (const root of roots.splice(0)) await act(() => root.unmount());
  document.body.replaceChildren();
  rangeGeometry.forEach((name, index) => {
    const descriptor = originalRangeGeometry[index];
    if (descriptor) Object.defineProperty(Range.prototype, name, descriptor);
    else Reflect.deleteProperty(Range.prototype, name);
  });
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function composer(value: string, richTextEnabled = true) {
  const host = document.body.appendChild(document.createElement("div"));
  const root = createRoot(host);
  roots.push(root);
  const editorRef = createRef<ComposerPromptEditorHandle>();
  await act(() =>
    root.render(
      <ComposerPromptEditorTiptap
        value={value}
        cursor={value.length}
        richTextEnabled={richTextEnabled}
        contextRecords={new Map()}
        skills={[]}
        disabled={false}
        placeholder="Message"
        onChange={() => {}}
        onPaste={() => {}}
        editorRef={editorRef}
      />,
    ),
  );
  await act(() => editorRef.current!.focusAtEnd());
  const element = host.querySelector<HTMLElement>('[data-testid="composer-editor"]')!;
  return {
    read: () => editorRef.current!.readSnapshot(),
    async press(key: string, modifiers: KeyboardEventInit) {
      const event = new KeyboardEvent("keydown", {
        key,
        bubbles: true,
        cancelable: true,
        ...modifiers,
      });
      await act(() => element.dispatchEvent(event));
      return event;
    },
  };
}

describe.each([false, true])("Ctrl+J with Emacs editing %s", (emacs) => {
  beforeEach(() => {
    settings.emacs = emacs;
  });

  it.each([
    ["- item", "- item\n- "],
    ["* item", "* item\n* "],
    ["3) item", "3) item\n4) "],
    ["- parent\n  - item", "- parent\n  - item\n  - "],
    ["- [x] item", "- [x] item\n- [ ] "],
    ["- [ ] parent\n  - [x] item", "- [ ] parent\n  - [x] item\n  - [ ] "],
    ["- **item**", "- **item**\n- "],
  ])("continues %j like Shift+Enter", async (value, expected) => {
    const ctrlJ = await composer(value);
    const shiftEnter = await composer(value);
    expect((await ctrlJ.press("j", { ctrlKey: true })).defaultPrevented).toBe(true);
    await shiftEnter.press("Enter", { shiftKey: true });
    expect(ctrlJ.read().value).toBe(expected);
    expect(ctrlJ.read()).toEqual(shiftEnter.read());
  });

  it("leaves an empty nested item and then exits the list", async () => {
    const ctrlJ = await composer("- parent\n  - item");
    const shiftEnter = await composer("- parent\n  - item");
    for (let count = 0; count < 3; count++) {
      await ctrlJ.press("j", { ctrlKey: true });
      await shiftEnter.press("Enter", { shiftKey: true });
      expect(ctrlJ.read()).toEqual(shiftEnter.read());
    }
    expect(ctrlJ.read().value).toBe("- parent\n  - item\n");
  });

  it("undoes list continuation in one step", async () => {
    const editor = await composer("- item");
    await editor.press("j", { ctrlKey: true });
    await editor.press("z", { ctrlKey: true });
    expect(editor.read().value).toBe("- item");
  });

  it.each([true, false])("inserts plain newlines with rich text %s", async (richText) => {
    const editor = await composer("hello", richText);
    await editor.press("j", { ctrlKey: true });
    expect(editor.read().value).toBe("hello\n");
  });

  it.each(["> quote", "> ", "```ts\n  code()\n```", "```ts\ncode()\n\n\n```"])(
    "handles quotes and fences like Shift+Enter in %j",
    async (value) => {
      const ctrlJ = await composer(value);
      const shiftEnter = await composer(value);
      await ctrlJ.press("j", { ctrlKey: true });
      await shiftEnter.press("Enter", { shiftKey: true });
      expect(ctrlJ.read()).toEqual(shiftEnter.read());
      expect(ctrlJ.read().value).not.toBe(value);
    },
  );

  it("leaves Ctrl+J during IME composition alone", async () => {
    const editor = await composer("- item");
    expect((await editor.press("j", { ctrlKey: true, isComposing: true })).defaultPrevented).toBe(
      false,
    );
    expect(editor.read().value).toBe("- item");
  });
});
