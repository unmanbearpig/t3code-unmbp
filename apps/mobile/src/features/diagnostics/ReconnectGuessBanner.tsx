import { useEffect, useState, useSyncExternalStore } from "react";
import { Pressable, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { AppText } from "../../components/ui/AppText";
import * as Runtime from "../../lib/runtime";
import * as ConnectionLog from "./connection-log";

export function ReconnectGuessBanner() {
  const [log, setLog] = useState<ConnectionLog.MobileConnectionLog["Service"] | null>(null);
  useEffect(() => {
    let active = true;
    void Runtime.runtime
      .runPromise(ConnectionLog.MobileConnectionLog)
      .then((service) => {
        if (active) setLog(service);
      })
      .catch(() => undefined);
    return () => {
      active = false;
    };
  }, []);
  return log ? <LoadedBanner log={log} /> : null;
}

function LoadedBanner({ log }: { readonly log: ConnectionLog.MobileConnectionLog["Service"] }) {
  const prompt = useSyncExternalStore(log.subscribe, log.prompt, log.prompt);
  const insets = useSafeAreaInsets();
  useEffect(() => {
    if (!prompt) return;
    const timer = setTimeout(
      () => log.dismissPrompt(prompt.id),
      Math.max(0, prompt.expiresAt - Date.now()),
    );
    return () => clearTimeout(timer);
  }, [log, prompt]);
  if (!prompt) return null;
  return (
    <View
      pointerEvents="box-none"
      style={{ position: "absolute", top: insets.top + 8, left: 16, right: 16 }}
    >
      <View className="flex-row items-center gap-2 rounded-xl border border-border-subtle bg-surface p-3">
        <AppText className="flex-1 text-sm text-foreground">Reconnect</AppText>
        {(["old", "new", "skip"] as const).map((guess) => (
          <Pressable
            key={guess}
            accessibilityRole="button"
            accessibilityLabel={`Guess ${guess}`}
            className="rounded-lg bg-subtle px-3 py-3 active:opacity-70"
            onPress={() => log.guess(prompt.id, guess)}
          >
            <AppText className="text-sm text-foreground">
              {guess === "old" ? "Old" : guess === "new" ? "New" : "Skip"}
            </AppText>
          </Pressable>
        ))}
      </View>
    </View>
  );
}
