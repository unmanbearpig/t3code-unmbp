import { File, Paths } from "expo-file-system";
import * as Sharing from "expo-sharing";
import { useEffect, useState, useSyncExternalStore } from "react";
import { Alert, Platform, Pressable, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { tryCopyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import * as Runtime from "../../lib/runtime";
import { SettingsActionRow } from "../settings/components/SettingsActionRow";
import { SettingsSection } from "../settings/components/SettingsSection";
import * as ConnectionLog from "./connection-log";
import { formatConnectionLog } from "./connection-log-model";

export function ConnectionLogSection(props: {
  readonly app: { readonly version: string; readonly build: string };
}) {
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
  if (log === null) return null;
  return <LoadedConnectionLog log={log} app={props.app} />;
}

function LoadedConnectionLog(props: {
  readonly log: ConnectionLog.MobileConnectionLog["Service"];
  readonly app: { readonly version: string; readonly build: string };
}) {
  const { log } = props;
  const records = useSyncExternalStore(log.subscribe, log.read, log.read);
  const [selected, setSelected] = useState<string | null>(null);
  const [sharing, setSharing] = useState(false);
  const recent = records.slice(-20);
  // Hermes lacks toReversed; slice already made a copy.
  recent.reverse();
  const report = () => formatConnectionLog(log.read(), { ...props.app, platform: Platform.OS });
  const share = async () => {
    setSharing(true);
    const file = new File(Paths.cache, "t3-mobile-connections.json");
    try {
      file.write(report());
      await Sharing.shareAsync(file.uri, {
        mimeType: "application/json",
        UTI: "public.json",
        dialogTitle: "Connection log",
      });
    } catch {
      Alert.alert("Couldn't share connection log");
    } finally {
      // Keep one fixed cache file: Android receivers can read the URI after the sheet closes.
      setSharing(false);
    }
  };
  return (
    <SettingsSection title={`Connections (${records.length})`}>
      <SettingsActionRow
        icon="doc.on.doc"
        label="Copy connection log"
        disabled={records.length === 0}
        onPress={() => {
          void tryCopyTextWithHaptic(report(), { target: "connection log" });
        }}
      />
      <SettingsActionRow
        icon="tray.and.arrow.up"
        label="Share connection log"
        disabled={records.length === 0 || sharing}
        onPress={() => {
          void share();
        }}
      />
      <SettingsActionRow
        icon="trash"
        label="Clear connection log"
        tone="danger"
        disabled={records.length === 0}
        onPress={() => {
          log.clear();
          const file = new File(Paths.cache, "t3-mobile-connections.json");
          if (file.exists) file.delete();
        }}
      />
      {!log.storageAvailable() ? (
        <Text className="p-4 text-sm text-danger-foreground">
          Storage unavailable. This session's log is in memory.
        </Text>
      ) : null}
      {records.length === 0 ? (
        <Text className="p-4 text-sm text-foreground-muted">No connections recorded</Text>
      ) : null}
      {recent.map((record) => (
        <View key={record.id} className="border-t border-border-subtle">
          <Pressable
            accessibilityRole="button"
            accessibilityState={{ expanded: selected === record.id }}
            onPress={() => setSelected(selected === record.id ? null : record.id)}
            className="gap-1 p-4"
          >
            <Text className="text-sm text-foreground">
              {new Date(record.timestamp).toLocaleString()}
            </Text>
            <Text className="text-xs text-foreground-muted">
              {record.launch} · {record.wakeup} · {record.outcome} · {Math.round(record.durationMs)}{" "}
              ms
            </Text>
          </Pressable>
          {selected === record.id ? (
            <Text selectable className="px-4 pb-4 font-mono text-xs text-foreground">
              {JSON.stringify(record, null, 2)}
            </Text>
          ) : null}
        </View>
      ))}
    </SettingsSection>
  );
}
