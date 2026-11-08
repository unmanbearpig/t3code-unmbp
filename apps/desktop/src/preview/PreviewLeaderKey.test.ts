import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  compileResolvedKeybindingsConfig,
  parseKeybindingShortcut,
} from "@t3tools/shared/keybindings";
import { LEADER_TIMEOUT_MS } from "@t3tools/shared/leaderKey";
import { createPreviewLeaderKeyHandler } from "./PreviewLeaderKey.ts";

const input = (key: string, type = "keyDown", control = false) => ({
  key,
  code: key,
  type,
  control,
  meta: false,
  alt: false,
  shift: false,
  isAutoRepeat: false,
});
const setup = () => {
  vi.useFakeTimers();
  const notify = vi.fn();
  const handler = createPreviewLeaderKeyHandler({
    isMac: false,
    notify,
    getConfig: () => ({
      trigger: parseKeybindingShortcut("ctrl+.")!,
      bindings: compileResolvedKeybindingsConfig([
        { key: "leader+n", command: "thread.next" },
        { key: "leader+r", command: "preview.refresh", when: "previewFocus" },
        { key: "leader+1", command: "modelPicker.jump.1", when: "modelPickerOpen" },
      ]),
    }),
  });
  return { ...handler, notify };
};
afterEach(() => vi.useRealTimers());

describe("embedded browser leader input", () => {
  it("prevents matching strokes and releases before the guest page receives them", () => {
    const handler = setup();
    expect(handler.handle(input(".", "keyDown", true))).toBe(true);
    expect(handler.handle(input(".", "keyUp", true))).toBe(true);
    expect(handler.handle(input("n"))).toBe(true);
    handler.reset();
    expect(handler.handle(input("n", "keyUp"))).toBe(true);
    expect(handler.notify).toHaveBeenCalledWith(
      expect.objectContaining({ type: "stroke", key: "n" }),
    );
  });
  it("lets unmatched or out-of-context input continue and cancels the prefix", () => {
    const handler = setup();
    handler.handle(input(".", "keyDown", true));
    expect(handler.handle(input("1"))).toBe(false);
    expect(handler.handle(input("1", "keyUp"))).toBe(false);
    expect(handler.handle(input("n"))).toBe(false);
    expect(handler.handle(input(".", "keyUp", true))).toBe(true);
  });
  it("cancels on timeout, Escape and blur without releasing the swallowed prefix", () => {
    const handler = setup();
    handler.handle(input(".", "keyDown", true));
    vi.advanceTimersByTime(LEADER_TIMEOUT_MS);
    expect(handler.handle(input("n"))).toBe(false);
    expect(handler.handle(input(".", "keyUp", true))).toBe(true);
    handler.handle(input(".", "keyDown", true));
    expect(handler.handle(input("Escape"))).toBe(true);
    handler.reset();
    expect(handler.handle(input("n"))).toBe(false);
  });
});
