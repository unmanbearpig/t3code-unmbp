import {
  chainCommands,
  deleteSelection,
  joinBackward,
  joinForward,
  selectNodeBackward,
  selectNodeForward,
  splitBlockKeepMarks,
} from "@tiptap/pm/commands";
import { Slice, type Node as ProseMirrorNode } from "@tiptap/pm/model";
import { TextSelection } from "@tiptap/pm/state";
import type { EditorView } from "@tiptap/pm/view";
import { markAsClipboardEdit } from "./composer-undo-grouping";
import { composerEmacsAction } from "./lib/composerEmacsShortcuts";

const graphemes = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const backspace = chainCommands(deleteSelection, joinBackward, selectNodeBackward);
const forwardDelete = chainCommands(deleteSelection, joinForward, selectNodeForward);

function characterPosition(view: EditorView, direction: -1 | 1, extend = false) {
  const { selection, doc } = view.state;
  if (!selection.empty && !extend) return direction === -1 ? selection.from : selection.to;
  const { $head } = selection;
  const adjacent = direction === -1 ? $head.nodeBefore : $head.nodeAfter;
  if (adjacent?.isText && adjacent.text) {
    const segments = [...graphemes.segment(adjacent.text)];
    const segment = direction === -1 ? segments.at(-1) : segments[0];
    return $head.pos + direction * (segment?.segment.length ?? 1);
  }
  const position = $head.pos + direction * (adjacent?.isInline ? adjacent.nodeSize : 1);
  return TextSelection.near(
    doc.resolve(Math.max(0, Math.min(doc.content.size, position))),
    direction,
  ).head;
}

function wordPosition(view: EditorView, direction: -1 | 1) {
  const { $head } = view.state.selection;
  const text = $head.parent.textBetween(0, $head.parent.content.size, "\n", "\ufffc");
  const segments = [...graphemes.segment(text)];
  const candidates =
    direction === -1
      ? segments.filter((segment) => segment.index < $head.parentOffset).toReversed()
      : segments.filter((segment) => segment.index >= $head.parentOffset);
  let position = $head.parentOffset;
  let inWord = false;
  for (const segment of candidates) {
    const isWord = /[\p{L}\p{N}_\ufffc]/u.test(segment.segment);
    if (inWord && !isWord) break;
    inWord ||= isWord;
    position = direction === -1 ? segment.index : segment.index + segment.segment.length;
  }
  return position === $head.parentOffset
    ? characterPosition(view, direction)
    : $head.start() + position;
}

function move(view: EditorView, position: number, extend = false) {
  const selection = TextSelection.near(view.state.doc.resolve(position));
  view.dispatch(
    view.state.tr
      .setSelection(
        extend
          ? TextSelection.create(view.state.doc, view.state.selection.anchor, selection.head)
          : selection,
      )
      .scrollIntoView(),
  );
}

function moveLine(view: EditorView, direction: -1 | 1, extend: boolean) {
  // Chromium can follow a visual line across soft wraps. Sync its selection
  // back into ProseMirror rather than leaving the DOM and editor state apart.
  const selection = view.dom.ownerDocument.getSelection();
  if (selection?.anchorNode && view.dom.contains(selection.anchorNode) && selection.modify) {
    selection.modify(extend ? "extend" : "move", direction === -1 ? "backward" : "forward", "line");
    if (selection.focusNode && view.dom.contains(selection.focusNode)) {
      move(view, view.posAtDOM(selection.focusNode, selection.focusOffset), extend);
      return;
    }
  }
  const { $head } = view.state.selection;
  let target: number | null = null;
  view.state.doc.descendants((node, position) => {
    if (!node.isTextblock) return;
    const start = position + 1;
    if (direction === -1 && start < $head.start()) {
      target = start + Math.min($head.parentOffset, node.content.size);
    } else if (direction === 1 && target === null && start > $head.start()) {
      target = start + Math.min($head.parentOffset, node.content.size);
    }
    return false;
  });
  if (target !== null) move(view, target, extend);
}

function transpose(view: EditorView) {
  const { state } = view;
  const { $head, empty } = state.selection;
  if (!empty) return;
  const text = $head.parent.textBetween(0, $head.parent.content.size, "\n", "\ufffc");
  const segments = [...graphemes.segment(text)];
  const rightIndex =
    $head.parentOffset === text.length
      ? segments.length - 1
      : segments.findIndex((segment) => segment.index === $head.parentOffset);
  const left = segments[rightIndex - 1];
  const right = segments[rightIndex];
  if (!left || !right || left.segment === "\ufffc" || right.segment === "\ufffc") return;
  const from = $head.start() + left.index;
  const middle = $head.start() + right.index;
  const to = middle + right.segment.length;
  const replacement = state.doc
    .slice(middle, to)
    .content.append(state.doc.slice(from, middle).content);
  const tr = state.tr.replaceWith(from, to, replacement);
  view.dispatch(tr.setSelection(TextSelection.create(tr.doc, to)).scrollIntoView());
}

/** The kill buffer belongs to this mounted composer and retains marks and chips. */
export function createComposerEmacsHandler() {
  let killed = Slice.empty;
  let killSource: ProseMirrorNode | null = null;
  let killFrom = 0;
  let killTo = 0;
  let killPosition = 0;
  let afterKill: ProseMirrorNode | null = null;
  return (view: EditorView, event: KeyboardEvent): boolean => {
    const action = composerEmacsAction(event);
    if (!action || !view.editable) {
      afterKill = null;
      return false;
    }
    const { state } = view;
    const { selection } = state;
    const dispatch = view.dispatch.bind(view);
    const extend = event.shiftKey && event.key !== "<" && event.key !== ">";
    if (!action.startsWith("kill")) afterKill = null;
    switch (action) {
      case "lineStart":
        move(view, selection.$head.start(), extend);
        break;
      case "lineEnd":
        move(view, selection.$head.end(), extend);
        break;
      case "backward":
      case "forward":
        move(view, characterPosition(view, action === "backward" ? -1 : 1, extend), extend);
        break;
      case "wordBackward":
      case "wordForward":
        move(view, wordPosition(view, action === "wordBackward" ? -1 : 1), extend);
        break;
      case "previousLine":
      case "nextLine":
        moveLine(view, action === "previousLine" ? -1 : 1, extend);
        break;
      case "documentStart":
      case "documentEnd":
        move(view, action === "documentStart" ? 0 : state.doc.content.size, extend);
        break;
      case "transpose":
        transpose(view);
        break;
      case "backspace":
      case "delete": {
        if (!selection.empty) {
          deleteSelection(state, dispatch);
        } else {
          const direction = action === "backspace" ? -1 : 1;
          const adjacent =
            direction === -1 ? selection.$head.nodeBefore : selection.$head.nodeAfter;
          if (adjacent?.isInline) {
            const position = characterPosition(view, direction);
            dispatch(
              state.tr.delete(
                Math.min(position, selection.head),
                Math.max(position, selection.head),
              ),
            );
          } else {
            (direction === -1 ? backspace : forwardDelete)(state, dispatch);
          }
        }
        break;
      }
      case "killEnd":
      case "killStart":
      case "killWordBackward":
      case "killWordForward": {
        const { $head } = selection;
        let target =
          action === "killStart"
            ? $head.start()
            : action === "killEnd"
              ? $head.end()
              : wordPosition(view, action === "killWordBackward" ? -1 : 1);
        // At the end of a hard line, Ctrl-K kills the newline and joins lines.
        if (action === "killEnd" && target === $head.pos && target < state.doc.content.size - 1) {
          target = characterPosition(view, 1);
        }
        const from = selection.empty ? Math.min($head.pos, target) : selection.from;
        const to = selection.empty ? Math.max($head.pos, target) : selection.to;
        if (from !== to) {
          if (
            state.doc === afterKill &&
            killSource &&
            (from === killPosition || to === killPosition)
          ) {
            if (from === killPosition) killTo += to - from;
            else killFrom -= to - from;
          } else {
            killSource = state.doc;
            killFrom = from;
            killTo = to;
          }
          killed = killSource.slice(killFrom, killTo);
          const tr = markAsClipboardEdit(state.tr.deleteRange(from, to), "cut").scrollIntoView();
          afterKill = tr.doc;
          killPosition = from;
          dispatch(tr);
        }
        break;
      }
      case "yank":
        if (killed.size > 0) {
          dispatch(
            markAsClipboardEdit(state.tr.replaceSelection(killed), "paste").scrollIntoView(),
          );
        }
        break;
      case "newline":
      case "openLine":
        splitBlockKeepMarks(state, (tr) => {
          if (action === "openLine") {
            tr.setSelection(TextSelection.near(tr.doc.resolve(selection.from), -1));
          }
          dispatch(tr.scrollIntoView());
        });
        break;
    }
    // Claim even a no-op at the document boundary so an app shortcut cannot run.
    event.preventDefault();
    event.stopPropagation();
    return true;
  };
}
