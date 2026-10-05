// @effect-diagnostics globalDate:off globalTimers:off -- Electron's synchronous before-input callback owns the key deadline.
import type { DesktopLeaderConfig, DesktopLeaderInput } from "@t3tools/contracts";
import {
  createLeaderKeyController,
  LEADER_TIMEOUT_MS,
  matchesLeaderStroke,
  matchesLeaderWhen,
} from "@t3tools/shared/leaderKey";

interface NativeInput {
  readonly type: string;
  readonly key: string;
  readonly code: string;
  readonly control: boolean;
  readonly meta: boolean;
  readonly alt: boolean;
  readonly shift: boolean;
  readonly isAutoRepeat: boolean;
  readonly isComposing?: boolean;
}

export function createPreviewLeaderKeyHandler(options: {
  readonly getConfig: () => DesktopLeaderConfig;
  readonly notify: (input: DesktopLeaderInput) => void;
  readonly isMac: boolean;
}) {
  const controller = createLeaderKeyController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const reset = () => {
    controller.cancel();
    clearTimeout(timeout);
    options.notify({
      type: "cancel",
      key: "",
      code: "",
      ctrlKey: false,
      metaKey: false,
      altKey: false,
      shiftKey: false,
    });
  };
  const handle = (input: NativeInput) => {
    if (input.type !== "keyDown" && input.type !== "keyUp") return false;
    const stroke = {
      key: input.key,
      code: input.code,
      ctrlKey: input.control,
      metaKey: input.meta,
      altKey: input.alt,
      shiftKey: input.shift,
    };
    const config = options.getConfig();
    const event = {
      key: input.key,
      code: input.code,
      type: input.type === "keyUp" ? ("keyup" as const) : ("keydown" as const),
      repeat: input.isAutoRepeat,
      isComposing: input.isComposing ?? false,
    };
    const action = controller.handle(
      event,
      config.trigger !== null && matchesLeaderStroke(stroke, config.trigger, options.isMac),
      Date.now(),
    );
    if (action === "pass") return false;
    if (action === "prefix") {
      clearTimeout(timeout);
      timeout = setTimeout(reset, LEADER_TIMEOUT_MS);
      options.notify({ ...stroke, type: "prefix" });
    } else if (action === "stroke") {
      const matches = config.bindings.some(
        (rule) =>
          "leader" in rule.shortcut &&
          matchesLeaderStroke(stroke, rule.shortcut.leader, options.isMac) &&
          matchesLeaderWhen(rule.whenAst, {
            ...config.context,
            previewFocus: true,
            previewOpen: true,
            isDesktop: true,
            isWeb: false,
          }),
      );
      if (!matches) {
        controller.release(event);
        reset();
        return false;
      }
      clearTimeout(timeout);
      options.notify({ ...stroke, type: "stroke" });
    } else if (input.type === "keyDown") {
      options.notify({ ...stroke, type: "cancel" });
    }
    return true;
  };
  return { handle, reset };
}
