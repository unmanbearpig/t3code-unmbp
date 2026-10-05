import type { NativeLeaderConfig } from "../features/keyboard/leaderKeyboardShortcuts";
import type { PropsWithChildren } from "react";
import { View } from "react-native";

import type { HardwareKeyboardCommand } from "../features/keyboard/hardwareKeyboardCommands";

export function T3KeyboardCommands(
  props: PropsWithChildren<{
    readonly leaderConfig?: NativeLeaderConfig;
    readonly enabledCommands: ReadonlyArray<HardwareKeyboardCommand>;
    readonly onCommand: (command: HardwareKeyboardCommand) => void;
  }>,
) {
  return <View className="flex-1">{props.children}</View>;
}
