import { useState } from "react";
import { Schema } from "effect";
import { LeaderShortcut } from "@t3tools/contracts/settings";
import { parseKeybindingShortcut } from "@t3tools/shared/keybindings";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { useNavigation } from "@react-navigation/native";
import { AsyncResult } from "effect/reactivity";
import { Platform, ScrollView, TextInput, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AndroidScreenHeader } from "../../components/AndroidScreenHeader";
import { AppText as Text } from "../../components/AppText";
import { NativeStackScreenOptions } from "../../native/StackHeader";
import {
  DEFAULT_COMPOSER_ENTER_BEHAVIOR,
  type ComposerEnterBehavior,
} from "../../lib/composerEnterBehavior";
import { mobilePreferencesAtom, updateMobilePreferencesAtom } from "../../state/preferences";
import { SettingsChoiceRow } from "./components/SettingsChoiceRow";
import { SettingsSection } from "./components/SettingsSection";

const isLeaderShortcut = Schema.is(LeaderShortcut);

const ENTER_BEHAVIOR_OPTIONS: ReadonlyArray<{
  readonly behavior: ComposerEnterBehavior;
  readonly label: string;
  readonly description: string;
}> = [
  {
    behavior: "send",
    label: "Send message",
    description: "Return sends the message. Shift-Return inserts a new line.",
  },
  {
    behavior: "newline",
    label: "Insert new line",
    description: "Return inserts a new line. Command-Return sends the message.",
  },
];

export function SettingsKeyboardRouteScreen() {
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  const preferencesResult = useAtomValue(mobilePreferencesAtom);
  const savePreferences = useAtomSet(updateMobilePreferencesAtom);
  const preferencesReady = AsyncResult.isSuccess(preferencesResult) && !preferencesResult.waiting;
  const selectedBehavior = AsyncResult.isSuccess(preferencesResult)
    ? (preferencesResult.value.composerEnterBehavior ?? DEFAULT_COMPOSER_ENTER_BEHAVIOR)
    : null;

  const storedLeader = AsyncResult.isSuccess(preferencesResult)
    ? preferencesResult.value.leaderShortcut
    : undefined;
  const [leaderDraft, setLeaderDraft] = useState(storedLeader ?? "ctrl+.");
  const [previousStoredLeader, setPreviousStoredLeader] = useState(storedLeader);
  if (storedLeader !== previousStoredLeader) {
    setPreviousStoredLeader(storedLeader);
    setLeaderDraft(storedLeader ?? "ctrl+.");
  }
  const leaderValid = isLeaderShortcut(parseKeybindingShortcut(leaderDraft));

  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      {Platform.OS === "android" ? (
        <>
          <NativeStackScreenOptions options={{ headerShown: false }} />
          <AndroidScreenHeader title="Keyboard" onBack={() => navigation.goBack()} />
        </>
      ) : null}
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        showsVerticalScrollIndicator={false}
        className="flex-1"
        contentContainerClassName="gap-3 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
      >
        <SettingsSection title="Leader key">
          <View className="gap-2 px-4 py-3">
            <Text className="text-sm text-foreground-muted">
              Press and release the trigger, then a shortcut. Use n/p for next/previous thread, 1–9
              to jump, k for commands, or Shift+n for a new thread. Escape or two seconds cancels.
            </Text>
            <TextInput
              accessibilityLabel="Leader trigger shortcut"
              value={leaderDraft}
              onChangeText={setLeaderDraft}
              onEndEditing={() => {
                if (leaderValid) savePreferences({ leaderShortcut: leaderDraft });
              }}
              autoCapitalize="none"
              autoCorrect={false}
              editable={preferencesReady}
              className="rounded-lg border border-border px-3 py-2 font-mono text-foreground"
            />
            {!leaderValid ? (
              <Text className="text-sm text-foreground-muted">
                Use a modified shortcut such as ctrl+. or ctrl+space.
              </Text>
            ) : null}
          </View>
          <SettingsChoiceRow
            description="Use leader shortcuts on this device."
            separated={false}
            label="Enabled"
            selected={storedLeader !== null}
            disabled={!preferencesReady || !leaderValid}
            onPress={() => savePreferences({ leaderShortcut: leaderDraft })}
          />
          <SettingsChoiceRow
            description="Pass the trigger through normally."
            label="Disabled"
            selected={storedLeader === null}
            separated
            disabled={!preferencesReady}
            onPress={() => savePreferences({ leaderShortcut: null })}
          />
        </SettingsSection>
        <SettingsSection title="Return key">
          {ENTER_BEHAVIOR_OPTIONS.map((option, index) => (
            <SettingsChoiceRow
              key={option.behavior}
              label={option.label}
              description={option.description}
              selected={selectedBehavior === option.behavior}
              separated={index > 0}
              disabled={!preferencesReady}
              onPress={() => savePreferences({ composerEnterBehavior: option.behavior })}
            />
          ))}
        </SettingsSection>
        <Text className="px-2 text-sm text-foreground-muted">
          Applies to the composer when a hardware keyboard is connected.
        </Text>
      </ScrollView>
    </View>
  );
}
