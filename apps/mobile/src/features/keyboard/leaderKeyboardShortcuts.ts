import { useAtomValue } from "@effect/atom-react";
import type { KeybindingShortcut } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { mergeWithDefaultKeybindings, parseKeybindingShortcut } from "@t3tools/shared/keybindings";
import { matchesLeaderWhen } from "@t3tools/shared/leaderKey";
import { AsyncResult } from "effect/unstable/reactivity";
import { useMemo } from "react";
import { Platform } from "react-native";
import { mobilePreferencesAtom } from "../../state/preferences";
import { environmentServerConfigsAtom } from "../../state/server";
import {
  mobileLeaderCommand,
  parseActiveThreadPath,
  type HardwareKeyboardCommand,
} from "./hardwareKeyboardCommands";

export interface NativeLeaderShortcut {
  readonly key: string;
  readonly ctrlKey: boolean;
  readonly metaKey: boolean;
  readonly altKey: boolean;
  readonly shiftKey: boolean;
}
export interface NativeLeaderConfig {
  readonly trigger: NativeLeaderShortcut | null;
  readonly bindings: ReadonlyArray<
    NativeLeaderShortcut & { readonly command: HardwareKeyboardCommand }
  >;
}

function nativeShortcut(shortcut: KeybindingShortcut): NativeLeaderShortcut {
  const isMac = Platform.OS === "ios";
  return {
    key: shortcut.key,
    ctrlKey: shortcut.ctrlKey || (shortcut.modKey && !isMac),
    metaKey: shortcut.metaKey || (shortcut.modKey && isMac),
    shiftKey: shortcut.shiftKey,
    altKey: shortcut.altKey,
  };
}

export function useNativeLeaderConfig(
  pathname: string,
  enabledCommands: ReadonlyArray<HardwareKeyboardCommand>,
): NativeLeaderConfig {
  const preferences = useAtomValue(mobilePreferencesAtom);
  const configs = useAtomValue(environmentServerConfigsAtom);
  const triggerInput = AsyncResult.isSuccess(preferences)
    ? preferences.value.leaderShortcut
    : undefined;
  return useMemo(() => {
    const trigger =
      triggerInput === null
        ? null
        : triggerInput === undefined
          ? DEFAULT_CLIENT_SETTINGS.leaderShortcut
          : parseKeybindingShortcut(triggerInput);
    const thread = parseActiveThreadPath(pathname);
    const config = thread ? configs.get(thread.environmentId) : configs.values().next().value;
    const bindings = mergeWithDefaultKeybindings(config?.keybindings ?? [], {
      leaderBindingsSupported: config?.environment.capabilities.leaderKeybindings === true,
    });
    const context = {
      isWeb: false,
      isDesktop: false,
      terminalFocus: /\/terminal(?:\/|$)/.test(pathname),
      terminalOpen: /\/terminal(?:\/|$)/.test(pathname),
    };
    const claimed = new Set<string>();
    const nativeBindings: NativeLeaderConfig["bindings"][number][] = [];
    for (let index = bindings.length - 1; index >= 0; index -= 1) {
      const rule = bindings[index];
      if (!rule) continue;
      if (!("leader" in rule.shortcut) || !matchesLeaderWhen(rule.whenAst, context)) continue;
      const stroke = nativeShortcut(rule.shortcut.leader);
      const id = [stroke.key, stroke.ctrlKey, stroke.metaKey, stroke.shiftKey, stroke.altKey].join(
        "|",
      );
      if (claimed.has(id)) continue;
      claimed.add(id);
      const command = mobileLeaderCommand(rule.command);
      if (command && enabledCommands.includes(command)) nativeBindings.push({ ...stroke, command });
    }
    return { trigger: trigger ? nativeShortcut(trigger) : null, bindings: nativeBindings };
  }, [triggerInput, pathname, configs, enabledCommands]);
}
