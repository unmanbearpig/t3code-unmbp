import { __setClientSettingsForTests } from "~/hooks/useSettings";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { createRef } from "react";
import { act, render } from "../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  ComposerPromptEditorTiptap,
  type ComposerPromptEditorHandle,
} from "./ComposerPromptEditorTiptap";

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

function setup(emacs: boolean) {
  __setClientSettingsForTests({ ...DEFAULT_CLIENT_SETTINGS, composerEmacsEditingEnabled: emacs });
}

async function composer(value: string, richTextEnabled = true) {
  await using stack = new AsyncDisposableStack();
  const editorRef = createRef<ComposerPromptEditorHandle>();
  const screen = stack.use(
    await render(
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
  const element = screen.container.querySelector<HTMLElement>('[data-testid="composer-editor"]')!;
  const resources = stack.move();
  return {
    [Symbol.asyncDispose]: () => resources[Symbol.asyncDispose](),
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
  it.each([
    ["- item", "- item\n- "],
    ["* item", "* item\n* "],
    ["3) item", "3) item\n4) "],
    ["- parent\n  - item", "- parent\n  - item\n  - "],
    ["- [x] item", "- [x] item\n- [ ] "],
    ["- [ ] parent\n  - [x] item", "- [ ] parent\n  - [x] item\n  - [ ] "],
    ["- **item**", "- **item**\n- "],
  ])("continues %j like Shift+Enter", async (value, expected) => {
    setup(emacs);

    await using ctrlJ = await composer(value);
    await using shiftEnter = await composer(value);
    expect((await ctrlJ.press("j", { ctrlKey: true })).defaultPrevented).toBe(true);
    await shiftEnter.press("Enter", { shiftKey: true });
    expect(ctrlJ.read().value).toBe(expected);
    expect(ctrlJ.read()).toEqual(shiftEnter.read());
  });

  it("leaves an empty nested item and then exits the list", async () => {
    setup(emacs);

    await using ctrlJ = await composer("- parent\n  - item");
    await using shiftEnter = await composer("- parent\n  - item");
    for (let count = 0; count < 3; count++) {
      await ctrlJ.press("j", { ctrlKey: true });
      await shiftEnter.press("Enter", { shiftKey: true });
      expect(ctrlJ.read()).toEqual(shiftEnter.read());
    }
    expect(ctrlJ.read().value).toBe("- parent\n  - item\n");
  });

  it("undoes list continuation in one step", async () => {
    setup(emacs);

    await using editor = await composer("- item");
    await editor.press("j", { ctrlKey: true });
    await editor.press("z", { ctrlKey: true });
    expect(editor.read().value).toBe("- item");
  });

  it.each([true, false])("inserts plain newlines with rich text %s", async (richText) => {
    setup(emacs);

    await using editor = await composer("hello", richText);
    await editor.press("j", { ctrlKey: true });
    expect(editor.read().value).toBe("hello\n");
  });

  it.each(["> quote", "> ", "```ts\n  code()\n```", "```ts\ncode()\n\n\n```"])(
    "handles quotes and fences like Shift+Enter in %j",
    async (value) => {
      setup(emacs);

      await using ctrlJ = await composer(value);
      await using shiftEnter = await composer(value);
      await ctrlJ.press("j", { ctrlKey: true });
      await shiftEnter.press("Enter", { shiftKey: true });
      expect(ctrlJ.read()).toEqual(shiftEnter.read());
      expect(ctrlJ.read().value).not.toBe(value);
    },
  );

  it("leaves Ctrl+J during IME composition alone", async () => {
    setup(emacs);

    await using editor = await composer("- item");
    expect((await editor.press("j", { ctrlKey: true, isComposing: true })).defaultPrevented).toBe(
      false,
    );
    expect(editor.read().value).toBe("- item");
  });
});
