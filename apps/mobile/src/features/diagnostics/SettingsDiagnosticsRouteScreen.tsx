import { Platform } from "react-native";
import { ConnectionLogSection } from "./ConnectionLogSection";
import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { ScreenScrollView as ScrollView } from "../../components/ui/ScreenScrollView";
import * as Application from "expo-application";
import Constants from "expo-constants";
import * as Updates from "expo-updates";
import { useEffect, useState } from "react";
import { View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { tryCopyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import { SettingsActionRow } from "../settings/components/SettingsActionRow";
import { SettingsScreen } from "../settings/components/SettingsScreen";
import { SettingsSection } from "../settings/components/SettingsSection";
import {
  formatStartupCrashReport,
  parseStartupCrashRecords,
  type StartupCrashRecord,
} from "./crash-log-model";
import {
  clearDiagnosticLog,
  formatDiagnosticLog,
  readDiagnosticLog,
  type DiagnosticLogEntry,
} from "./diagnostic-log";
import { performanceTelemetry, readPerformanceReport } from "./performance-telemetry";
import { PerformanceLogSection } from "./PerformanceLogSection";

// Request the full iOS window. Android's Expo log is limited to one day.
const LOG_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

type CrashLogState =
  | { readonly status: "loading" }
  | { readonly status: "unavailable" }
  | { readonly status: "ready"; readonly records: ReadonlyArray<StartupCrashRecord> };

function appIdentity() {
  return {
    version: Constants.expoConfig?.version ?? "0.0.0",
    // EAS assigns build numbers remotely, so expo-constants never sees them;
    // only the installed binary knows its own.
    build: Application.nativeBuildVersion ?? "dev",
  };
}

function loadActivityLog(): { entries: ReadonlyArray<DiagnosticLogEntry>; unavailable: boolean } {
  try {
    return { entries: readDiagnosticLog(), unavailable: false };
  } catch {
    return { entries: [], unavailable: true };
  }
}

/**
 * Saved activity and errors in every build, supplemented by expo-updates'
 * recovery log when available.
 */
export function SettingsDiagnosticsRouteScreen() {
  const insets = useSafeAreaInsets();
  const [state, setState] = useState<CrashLogState>(() =>
    Updates.isEnabled ? { status: "loading" } : { status: "unavailable" },
  );
  const [copied, setCopied] = useState(false);
  const [activityLog, setActivityLog] = useState(loadActivityLog);
  const [performanceReport, setPerformanceReport] = useState(readPerformanceReport);
  const log = activityLog.entries;

  useEffect(() => {
    if (!Updates.isEnabled) return;
    let cancelled = false;
    Updates.readLogEntriesAsync(LOG_WINDOW_MS)
      .then((entries) => {
        if (cancelled) return;
        setState({ status: "ready", records: parseStartupCrashRecords(entries) });
      })
      .catch((error: unknown) => {
        console.warn("[diagnostics] could not read the expo-updates log", error);
        if (!cancelled) setState({ status: "unavailable" });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const records = state.status === "ready" ? state.records : [];
  const copyReport = async () => {
    let entries = log;
    try {
      entries = readDiagnosticLog();
    } catch {
      setActivityLog((current) => ({ ...current, unavailable: true }));
    }
    const app = appIdentity();
    const report = [
      state.status === "ready"
        ? formatStartupCrashReport(records, app)
        : `T3 Code ${app.version} (${app.build})\nUpdate crash log ${state.status === "loading" ? "loading" : "unavailable"}.`,
      `Platform: ${Platform.OS}`,
      "\nActivity log",
      formatDiagnosticLog(entries),
      "\nPerformance telemetry",
      JSON.stringify(readPerformanceReport(), null, 2),
    ]
      .filter(Boolean)
      .join("\n");
    const ok = await tryCopyTextWithHaptic(report, { target: "diagnostic report" });
    if (ok) setCopied(true);
  };
  const clearLog = () => {
    try {
      clearDiagnosticLog();
      performanceTelemetry.clear();
      setPerformanceReport(readPerformanceReport());
      setActivityLog({ entries: [], unavailable: false });
      setCopied(false);
    } catch {
      setActivityLog((current) => ({ ...current, unavailable: true }));
    }
  };

  return (
    <SettingsScreen title="Diagnostics">
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        contentInset={{ bottom: Math.max(insets.bottom, 18) }}
        showsVerticalScrollIndicator={false}
        className="flex-1"
        contentContainerClassName="gap-6 px-5 pt-4 pb-[18px]"
      >
        <SettingsSection title="Activity log">
          {activityLog.unavailable ? (
            <Text className="p-4 text-sm text-foreground-muted">
              Could not read the activity log.
            </Text>
          ) : log.length === 0 ? (
            <Text className="p-4 text-sm text-foreground-muted">No activity recorded.</Text>
          ) : (
            <Text selectable className="p-4 font-mono text-xs leading-snug text-foreground-muted">
              {formatDiagnosticLog(
                log.slice(0, 30).map((entry) => ({
                  ...entry,
                  message:
                    entry.message.length > 500 ? `${entry.message.slice(0, 500)}…` : entry.message,
                })),
              )}
            </Text>
          )}
        </SettingsSection>
        <ConnectionLogSection app={appIdentity()} />
        <PerformanceLogSection summary={performanceReport.summary} />
        <SettingsSection title="Startup crashes">
          {state.status === "loading" ? (
            <View className="items-center gap-3 px-6 py-8">
              <AppActivityIndicator />
              <Text className="text-center text-sm text-foreground-muted">Reading crash log…</Text>
            </View>
          ) : state.status === "unavailable" ? (
            <EmptyState icon="exclamationmark.triangle" title="Update crash log unavailable" />
          ) : records.length === 0 ? (
            <EmptyState icon="checkmark.circle" title="No startup crashes" />
          ) : (
            records.map((record, index) => (
              <CrashRow key={record.timestamp} record={record} first={index === 0} />
            ))
          )}
        </SettingsSection>

        <View className="gap-3">
          <SettingsSection title="Actions">
            <SettingsActionRow
              icon={copied ? "checkmark" : "doc.on.doc"}
              label={copied ? "Copied" : "Copy diagnostic report"}
              onPress={() => void copyReport()}
            />
            <SettingsActionRow
              icon="arrow.clockwise"
              label="Refresh"
              onPress={() => {
                setCopied(false);
                setActivityLog(loadActivityLog());
                setPerformanceReport(readPerformanceReport());
              }}
            />
            <SettingsActionRow
              icon="trash"
              label="Clear activity log"
              tone="danger"
              onPress={clearLog}
            />
          </SettingsSection>
        </View>
      </ScrollView>
    </SettingsScreen>
  );
}

function EmptyState(props: {
  readonly icon: "exclamationmark.triangle" | "checkmark.circle";
  readonly title: string;
}) {
  return (
    <View className="items-center gap-2 px-6 py-8">
      <SymbolView
        name={props.icon}
        size={28}
        tintColorClassName="accent-icon"
        type="monochrome"
        weight="regular"
      />
      <Text className="text-center text-base text-foreground">{props.title}</Text>
    </View>
  );
}

function CrashRow(props: { readonly record: StartupCrashRecord; readonly first: boolean }) {
  const { record } = props;
  return (
    <View className={props.first ? "gap-1.5 p-4" : "gap-1.5 border-t border-border-subtle p-4"}>
      <Text className="text-xs text-foreground-muted">
        {new Date(record.timestamp).toLocaleString()}
      </Text>
      <Text selectable className="text-base leading-snug text-danger-foreground">
        {record.description}
      </Text>
      {record.frames.length > 0 ? (
        <Text selectable className="font-mono text-xs leading-snug text-foreground-muted">
          {record.frames.slice(0, 4).join("\n")}
        </Text>
      ) : null}
    </View>
  );
}
