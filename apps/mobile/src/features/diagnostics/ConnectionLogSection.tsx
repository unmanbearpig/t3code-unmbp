import { File, Paths } from "expo-file-system";
import { useEffect, useState, useSyncExternalStore } from "react";
import { Alert, Platform, Pressable, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { tryCopyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import * as Runtime from "../../lib/runtime";
import { ListActionRow as SettingsActionRow } from "../../components/ui/ListActionRow";
import { SettingsSwitchRow } from "../settings/components/SettingsSwitchRow";
import { SettingsSection } from "../settings/components/SettingsSection";
import * as ConnectionLog from "./connection-log";
import { formatConnectionLog } from "./connection-log-model";
import { shareConnectionLog } from "./connection-log-sharing";
import { readPerformanceReport } from "./performance-telemetry";

export function ConnectionLogSection(props: {
  readonly app: { readonly version: string; readonly build: string };
}) {
  const [log, setLog] = useState<ConnectionLog.MobileConnectionLog["Service"] | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- loads the connection log service for the external store subscription
  useEffect(() => {
    let active = true;
    void Runtime.runtime
      .runPromise(ConnectionLog.MobileConnectionLog)
      .then((service) => {
        if (active) setLog(service);
      })
      .catch(() => {
        if (active) setLoadFailed(true);
      });
    return () => {
      active = false;
    };
  }, []);
  if (log === null) {
    return (
      <SettingsSection title="Connections">
        <Text className="p-4 text-sm text-foreground-muted">
          {loadFailed ? "Connection log unavailable" : "Loading connection log…"}
        </Text>
      </SettingsSection>
    );
  }
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
  const enabled = useSyncExternalStore(log.subscribe, log.experimentEnabled, log.experimentEnabled);
  const [saving, setSaving] = useState(false);
  const recent = records.slice(-50);
  // Hermes lacks toReversed; slice already made a copy.
  recent.reverse();
  const report = () =>
    formatConnectionLog(
      log.read(),
      { ...props.app, platform: Platform.OS },
      readPerformanceReport(),
    );
  const share = async () => {
    setSharing(true);
    try {
      await shareConnectionLog(report());
    } catch {
      Alert.alert("Couldn't share connection log");
    } finally {
      setSharing(false);
    }
  };
  return (
    <SettingsSection title={`Connections (${records.length})`}>
      <SettingsSwitchRow
        label="Randomized reconnect A/B"
        value={enabled}
        disabled={saving}
        onValueChange={(value) => {
          setSaving(true);
          void Runtime.runtime
            .runPromise(log.setExperimentEnabled(value))
            .catch(() => Alert.alert("Couldn't save reconnect experiment"))
            .finally(() => setSaving(false));
        }}
      />
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
