// Bench-only: runs ReconnectLoop against a paired environment and shares the rows as NDJSON.
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { ReconnectLoop } from "@t3tools/client-runtime/connection";
import type { EnvironmentId } from "@t3tools/contracts";
import { requireOptionalNativeModule } from "expo";
import { AsyncResult, Atom } from "effect/reactivity";
import { useRef, useState } from "react";
import { Share, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { ListActionRow as SettingsActionRow } from "../../components/ui/ListActionRow";
import { environmentCatalog } from "../../connection/catalog";
import { connectionAtomRuntime } from "../../connection/runtime";
import { SettingsSection } from "../settings/components/SettingsSection";

const ROUNDS = 30;
const native = requireOptionalNativeModule<{ getAppNetworkTraffic?: () => unknown }>(
  "T3NativeControls",
);

function readTraffic(): ReconnectLoop.AppTraffic | null {
  try {
    const value = native?.getAppNetworkTraffic?.();
    return value !== null && typeof value === "object" ? (value as ReconnectLoop.AppTraffic) : null;
  } catch {
    return null;
  }
}

const runLoopAtom = connectionAtomRuntime
  .fn(
    (input: {
      readonly environmentId: EnvironmentId;
      readonly onRow: (row: ReconnectLoop.ReconnectLoopRow) => void;
    }) =>
      ReconnectLoop.runReconnectLoop({
        environmentId: input.environmentId,
        rounds: ROUNDS,
        options: {
          usageLimitSources: true,
          usageLimitsCommand: true,
        },
        readTraffic,
        onRow: input.onRow,
      }),
  )
  .pipe(Atom.withLabel("mobile:diagnostics:reconnect-loop"));

const median = (values: ReadonlyArray<number>) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2;
};

function summary(rows: ReadonlyArray<ReconnectLoop.ReconnectLoopRow>) {
  return (["full", "cached", "restart"] as const).map((arm) => {
    const ok = rows.filter((row) => row.arm === arm && row.ok);
    const failed = rows.filter((row) => row.arm === arm && !row.ok).length;
    const appDown = ok.flatMap((row) => (row.appTraffic ? [row.appTraffic.receivedBytes] : []));
    return `${arm}: n=${ok.length}${failed > 0 ? ` (${failed} failed)` : ""}, sync ${Math.round(
      median(ok.map((row) => row.syncedMs)),
    )} ms, config ${Math.round(median(ok.map((row) => row.configMs)))} ms, JSON ${(
      median(ok.map((row) => row.jsonDown)) / 1000
    ).toFixed(1)} kB${appDown.length > 0 ? `, app ${(median(appDown) / 1000).toFixed(1)} kB` : ""}`;
  });
}

export function ReconnectLoopSection() {
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const result = useAtomValue(runLoopAtom);
  const run = useAtomSet(runLoopAtom);
  const rowsRef = useRef<Array<ReconnectLoop.ReconnectLoopRow>>([]);
  const [rows, setRows] = useState<ReadonlyArray<ReconnectLoop.ReconnectLoopRow>>([]);
  const [target, setTarget] = useState<string | null>(null);
  const running = result.waiting;

  const start = (environmentId: EnvironmentId, label: string) => {
    rowsRef.current = [];
    setRows([]);
    setTarget(label);
    run({
      environmentId,
      onRow: (row) => {
        rowsRef.current.push(row);
        setRows([...rowsRef.current]);
      },
    });
  };
  const share = () =>
    void Share.share({
      title: "reconnect-loop.ndjson",
      message: rowsRef.current.map((row) => JSON.stringify({ target, ...row })).join("\n"),
    });
  const measured = rows.filter((row) => row.arm !== "warmup");
  const failure = AsyncResult.isFailure(result) ? String(result.cause).slice(0, 200) : null;

  return (
    <View className="gap-3">
      <SettingsSection title="Reconnect loop">
        {[...catalog.entries].map(([environmentId, entry]) => (
          <SettingsActionRow
            key={environmentId}
            icon="arrow.clockwise"
            label={`Run ${ROUNDS} rounds on ${entry.target.label}`}
            disabled={running}
            onPress={() => start(environmentId, entry.target.label)}
          />
        ))}
        {running ? (
          <SettingsActionRow icon="stop.fill" label="Stop" onPress={() => run(Atom.Interrupt)} />
        ) : null}
        <SettingsActionRow
          icon="square.and.arrow.up"
          label={`Share ${rows.length} rows`}
          disabled={rows.length === 0}
          onPress={share}
        />
      </SettingsSection>
      <Text selectable className="px-2 font-mono text-xs leading-snug text-foreground-muted">
        {[
          target === null
            ? ""
            : `${target}: ${running ? "running" : "stopped"}, ${measured.length}/${ROUNDS * 3} attempts`,
          ...(measured.length > 0 ? summary(measured) : []),
          ...(failure ? [`Error: ${failure}`] : []),
        ].join("\n")}
      </Text>
    </View>
  );
}
