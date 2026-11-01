import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { SettingsSection } from "../settings/components/SettingsSection";
import type { PerformanceSummary } from "./performance-model";

const LABELS: Record<PerformanceSummary["operation"], string> = {
  "startup.navigation-ready": "Navigation ready",
  "startup.splash-hide-requested": "Splash hide request",
  "lifecycle.resume-js": "Resume JS scheduling",
  "runtime.js-delay": "JS scheduling delay",
  "runtime.memory-warning": "Memory warnings",
  "cache.read": "Cache read",
  "cache.decode": "Cache decode",
  "cache.encode": "Cache encode",
  "cache.write": "Cache write",
  "shell.apply-batch": "Shell updates",
  "thread.apply-batch": "Thread updates",
  "rpc.request": "RPC requests",
};

export function PerformanceLogSection(props: {
  readonly summary: ReadonlyArray<PerformanceSummary>;
}) {
  return (
    <SettingsSection title="Performance">
      {props.summary.length === 0 ? (
        <Text className="p-4 text-sm text-foreground-muted">No measurements recorded</Text>
      ) : (
        props.summary.map((metric) => (
          <View key={`${metric.operation}:${metric.category ?? ""}`} className="gap-1 p-4">
            <Text className="text-sm text-foreground">
              {LABELS[metric.operation]}
              {metric.category ? ` · ${metric.category}` : ""}
            </Text>
            <Text className="text-xs text-foreground-muted">
              {metric.operation === "runtime.memory-warning"
                ? metric.count
                : `${metric.count} samples · recent p95 ${Math.round(metric.recentP95Ms)} ms · max ${Math.round(metric.maxMs)} ms`}
            </Text>
          </View>
        ))
      )}
    </SettingsSection>
  );
}
