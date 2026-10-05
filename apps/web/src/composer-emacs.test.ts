// @vitest-environment jsdom
import { getSchema, Node } from "@tiptap/core";
import StarterKit from "@tiptap/starter-kit";
import { history, undo } from "@tiptap/pm/history";
import { EditorState, TextSelection, type Transaction } from "@tiptap/pm/state";
import { EditorView } from "@tiptap/pm/view";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_RESOLVED_KEYBINDINGS,
  compileResolvedKeybindingsConfig,
  mergeWithDefaultKeybindings,
} from "@t3tools/shared/keybindings";

import { createComposerEmacsHandler } from "./composer-emacs";
import { isCtrlNewlineShortcut } from "./lib/textboxNewline";
import { resolveShortcutCommand } from "./keybindings";
import { groupUndoByChangeKind, type ComposerChangeKind } from "./composer-undo-grouping";

const schema = getSchema([
  StarterKit,
  Node.create({
    name: "composer-mention",
    inline: true,
    group: "inline",
    atom: true,
    addAttributes: () => ({ path: { default: "file.ts" } }),
    renderHTML: ({ HTMLAttributes }) => ["span", HTMLAttributes, "@file.ts"],
  }),
]);
const views: EditorView[] = [];
const cleanups: (() => void)[] = [];

afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  for (const view of views.splice(0)) view.destroy();
  document.body.replaceChildren();
});

function composer(lines: string[] = ["hello world"], enabled = true) {
  const host = document.body.appendChild(document.createElement("div"));
  const handle = createComposerEmacsHandler();
  let previous: ComposerChangeKind | null = null;
  const view = new EditorView(host, {
    state: EditorState.create({
      schema,
      doc: schema.node(
        "doc",
        null,
        lines.map((text) => schema.node("paragraph", null, text ? schema.text(text) : [])),
      ),
      plugins: [history()],
    }),
    attributes: { "data-composer-emacs": String(enabled), "aria-multiline": "true" },
    handleKeyDown: (view, event) =>
      (enabled || isCtrlNewlineShortcut(event)) && handle(view, event),
    handleScrollToSelection: () => true,
    dispatchTransaction: (tr: Transaction) => {
      previous = groupUndoByChangeKind(tr, previous);
      view.updateState(view.state.apply(tr));
    },
  });
  views.push(view);
  const at = (position: number, anchor = position) => {
    view.dispatch(
      view.state.tr.setSelection(TextSelection.create(view.state.doc, anchor, position)),
    );
    view.focus();
  };
  const press = (key: string, modifiers: KeyboardEventInit = { ctrlKey: true }) => {
    const event = new KeyboardEvent("keydown", {
      key,
      bubbles: true,
      cancelable: true,
      ...modifiers,
    });
    view.dom.dispatchEvent(event);
    return event;
  };
  at(1);
  return {
    view,
    at,
    press,
    text: () => view.state.doc.textBetween(0, view.state.doc.content.size, "\n", "@file.ts"),
  };
}

describe("Emacs composer editing", () => {
  it("moves Ctrl-A and Ctrl-E to hard-line boundaries instead of selecting all", () => {
    const editor = composer(["first", "second"]);
    editor.at(10);
    editor.press("a");
    expect(editor.view.state.selection.from).toBe(8);
    expect(editor.view.state.selection.empty).toBe(true);
    editor.press("e");
    expect(editor.view.state.selection.head).toBe(14);
  });

  it("moves and deletes whole Unicode graphemes", () => {
    const editor = composer(["a👩‍💻éz"]);
    editor.at(2);
    editor.press("f");
    expect(editor.view.state.selection.head).toBe(7);
    editor.press("b");
    expect(editor.view.state.selection.head).toBe(2);
    editor.press("d");
    expect(editor.text()).toBe("aéz");
    editor.at(4);
    editor.press("h");
    expect(editor.text()).toBe("az");
  });

  it.each(["b", "ArrowLeft"])("moves backward by word with Alt-%s", (key) => {
    const editor = composer();
    editor.at(12);
    editor.press(key, { altKey: true });
    expect(editor.view.state.selection.head).toBe(7);
    editor.press(key, { altKey: true });
    expect(editor.view.state.selection.head).toBe(1);
  });

  it.each(["f", "ArrowRight"])("moves forward by word with Alt-%s", (key) => {
    const editor = composer();
    editor.press(key, { altKey: true });
    expect(editor.view.state.selection.head).toBe(6);
    editor.press(key, { altKey: true });
    expect(editor.view.state.selection.head).toBe(12);
  });

  it("extends selections with shifted Control and Option movement", () => {
    const editor = composer();
    editor.press("f", { ctrlKey: true, shiftKey: true });
    editor.press("f", { ctrlKey: true, shiftKey: true });
    expect(editor.view.state.selection.to - editor.view.state.selection.from).toBe(2);
    editor.press("ArrowRight", { altKey: true, shiftKey: true });
    expect(
      editor.view.state.doc.textBetween(
        editor.view.state.selection.from,
        editor.view.state.selection.to,
      ),
    ).toBe("hello");
    editor.press("e", { ctrlKey: true, shiftKey: true });
    expect(editor.view.state.selection.anchor).toBe(1);
    expect(editor.view.state.selection.head).toBe(12);
  });

  it("moves between lines retaining the column and consumes boundary no-ops", () => {
    const editor = composer(["first", "second"]);
    editor.at(3);
    editor.press("n");
    expect(editor.view.state.selection.head).toBe(10);
    expect(editor.press("n").defaultPrevented).toBe(true);
    editor.press("p");
    expect(editor.view.state.selection.head).toBe(3);
  });

  it("moves to document boundaries with Alt-< and Alt->", () => {
    const editor = composer(["first", "second"]);
    editor.press(">", { altKey: true, shiftKey: true });
    expect(editor.view.state.selection.head).toBe(14);
    expect(editor.view.state.selection.empty).toBe(true);
    editor.press("<", { altKey: true, shiftKey: true });
    expect(editor.view.state.selection.head).toBe(1);
  });

  it("kills text and the newline on repeated Ctrl-K, then yanks both back", () => {
    const editor = composer(["hello world", "second"]);
    editor.at(7);
    editor.press("k");
    expect(editor.text()).toBe("hello \nsecond");
    editor.press("k");
    expect(editor.text()).toBe("hello second");
    editor.press("y");
    expect(editor.text()).toBe("hello world\nsecond");
  });

  it("kills to line start and restores it", () => {
    const editor = composer();
    editor.at(7);
    editor.press("u");
    expect(editor.text()).toBe("world");
    editor.press("y");
    expect(editor.text()).toBe("hello world");
  });

  it("combines consecutive backward word kills in their original order", () => {
    const editor = composer(["one two three"]);
    editor.at(14);
    editor.press("w");
    editor.press("w");
    expect(editor.text()).toBe("one ");
    editor.press("y");
    expect(editor.text()).toBe("one two three");
  });

  it("handles Option-produced symbols and vertical Option arrows", () => {
    const editor = composer();
    editor.press("ƒ", { altKey: true, code: "KeyF" });
    expect(editor.view.state.selection.head).toBe(6);
    editor.press("ArrowDown", { altKey: true });
    expect(editor.view.state.selection.head).toBe(12);
    editor.press("ArrowUp", { altKey: true });
    expect(editor.view.state.selection.head).toBe(1);
  });

  it("joins hard lines with Control-D and Control-H", () => {
    const editor = composer(["first", "second"]);
    editor.at(6);
    editor.press("d");
    expect(editor.text()).toBe("firstsecond");
    editor.press("j");
    editor.press("h");
    expect(editor.text()).toBe("firstsecond");
  });

  it.each([
    ["w", { ctrlKey: true }],
    ["Backspace", { altKey: true }],
    ["Backspace", { ctrlKey: true }],
  ])("kills the preceding word with %s", (key, modifiers) => {
    const editor = composer();
    editor.at(12);
    editor.press(key, modifiers);
    expect(editor.text()).toBe("hello ");
    editor.press("y");
    expect(editor.text()).toBe("hello world");
  });

  it.each(["d", "Delete"])("kills the following word with Alt-%s", (key) => {
    const editor = composer();
    editor.press(key, { altKey: true });
    expect(editor.text()).toBe(" world");
    editor.press("y");
    expect(editor.text()).toBe("hello world");
  });

  it("preserves formatting, chip identity and independent undo steps through kill/yank", () => {
    const editor = composer();
    const paragraph = schema.node("paragraph", null, [
      schema.text("bold", [schema.mark("bold")]),
      schema.nodes["composer-mention"]!.create({ path: "original.ts" }),
      schema.text("tail"),
    ]);
    editor.view.updateState(
      EditorState.create({
        schema,
        doc: schema.node("doc", null, [paragraph]),
        plugins: [history()],
      }),
    );
    editor.at(1);
    const original = editor.view.state.doc.toJSON();
    editor.press("k");
    expect(editor.text()).toBe("");
    editor.press("y");
    expect(editor.view.state.doc.toJSON()).toEqual(original);
    undo(editor.view.state, editor.view.dispatch);
    expect(editor.text()).toBe("");
    undo(editor.view.state, editor.view.dispatch);
    expect(editor.view.state.doc.toJSON()).toEqual(original);
  });

  it("does not append a later kill to stale content after typing", () => {
    const editor = composer(["hello world"]);
    editor.at(7);
    editor.press("k");
    editor.view.dispatch(editor.view.state.tr.insertText("new"));
    editor.at(7);
    editor.press("k");
    editor.press("y");
    expect(editor.text()).toBe("hello new");
  });

  it("transposes characters at the cursor and at line end", () => {
    const editor = composer(["ab👩‍💻"]);
    editor.at(2);
    editor.press("t");
    expect(editor.text()).toBe("ba👩‍💻");
    editor.at(editor.view.state.doc.content.size - 1);
    editor.press("t");
    expect(editor.text()).toBe("b👩‍💻a");
  });

  it("inserts a newline with Ctrl-J and keeps the cursor before a Ctrl-O open line", () => {
    const editor = composer(["hello"]);
    editor.at(3);
    editor.press("o");
    expect(editor.text()).toBe("he\nllo");
    expect(editor.view.state.selection.head).toBe(3);
    editor.press("j");
    expect(editor.text()).toBe("he\n\nllo");
    expect(editor.view.state.selection.head).toBe(5);
  });

  it("leaves ordinary, Meta and IME keys to their existing handlers", () => {
    const editor = composer();
    expect(editor.press("a", {}).defaultPrevented).toBe(false);
    expect(editor.press("a", { metaKey: true }).defaultPrevented).toBe(false);
    expect(editor.press("a", { ctrlKey: true, isComposing: true }).defaultPrevented).toBe(false);
    expect(editor.text()).toBe("hello world");
  });

  it("keeps the default editing behavior when the setting is off", () => {
    const editor = composer(["hello"], false);
    editor.at(4);
    expect(editor.press("a").defaultPrevented).toBe(false);
    expect(editor.view.state.selection.head).toBe(4);
  });

  it("inserts and undoes Ctrl-J with Emacs editing off without toggling the terminal", () => {
    const editor = composer(["hello"], false);
    editor.at(3);
    const commands: string[] = [];
    const listener = (event: KeyboardEvent) => {
      const command = resolveShortcutCommand(event, DEFAULT_RESOLVED_KEYBINDINGS, {
        platform: "Linux",
      });
      if (command) commands.push(command);
    };
    window.addEventListener("keydown", listener, true);
    cleanups.push(() => window.removeEventListener("keydown", listener, true));
    expect(editor.press("j").defaultPrevented).toBe(true);
    expect(editor.text()).toBe("he\nllo");
    expect(commands).toEqual([]);
    expect(undo(editor.view.state, editor.view.dispatch)).toBe(true);
    expect(editor.text()).toBe("hello");
  });

  it("lets editing win over capture-phase app shortcuts only in an enabled composer", () => {
    const editor = composer();
    const commands: string[] = [];
    const listener = (event: KeyboardEvent) => {
      const command = resolveShortcutCommand(event, DEFAULT_RESOLVED_KEYBINDINGS, {
        platform: "Linux",
      });
      if (command) {
        commands.push(command);
        event.preventDefault();
      }
    };
    window.addEventListener("keydown", listener, true);
    cleanups.push(() => window.removeEventListener("keydown", listener, true));
    for (const key of ["k", "b", "d", "j", "n", "p", "u", "o", "w", "y"]) editor.press(key);
    expect(commands).toEqual([]);
    editor.view.dom.setAttribute("data-composer-emacs", "false");
    editor.press("k");
    expect(commands).toEqual(["commandPalette.toggle"]);
  });

  it("evaluates editableFocus from the event target when a handler omits it", () => {
    const editor = composer(["hello"], false);
    const scoped = mergeWithDefaultKeybindings(
      compileResolvedKeybindingsConfig([
        {
          key: "mod+k",
          command: "commandPalette.toggle",
          when: "!terminalFocus && !editableFocus",
        },
      ]),
    );
    const commands: (string | null)[] = [];
    const listener = (event: KeyboardEvent) =>
      commands.push(resolveShortcutCommand(event, scoped, { platform: "Linux" }));
    window.addEventListener("keydown", listener, true);
    cleanups.push(() => window.removeEventListener("keydown", listener, true));
    editor.press("k");
    expect(commands).toEqual([null]);
    expect(
      resolveShortcutCommand(
        {
          key: "k",
          ctrlKey: true,
          metaKey: false,
          altKey: false,
          shiftKey: false,
          target: document.body,
        },
        scoped,
        { platform: "Linux" },
      ),
    ).toBe("commandPalette.toggle");
  });
});
