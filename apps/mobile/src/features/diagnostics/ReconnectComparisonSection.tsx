import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ReconnectComparisonMode } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Atom, AsyncResult } from "effect/reactivity";
import { useState, useSyncExternalStore } from "react";
import { Share, View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { SelectionRow } from "../../components/ui/SelectionRow";
import { ListActionRow } from "../../components/ui/ListActionRow";
import { environmentCatalog } from "../../connection/catalog";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentShell } from "../../state/shell";
import { SettingsSection } from "../settings/components/SettingsSection";
import { MobileReconnectComparison, ROUNDS, BUILD_ID } from "./reconnect-comparison";
import * as MobileDatabase from "../../persistence/mobile-database";

const serviceAtom = connectionAtomRuntime.atom(MobileReconnectComparison);
const runAtom = connectionAtomRuntime.fn((environmentId: EnvironmentId) =>
  Effect.gen(function* () {
    const service = yield* MobileReconnectComparison;
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    yield* service.run(environmentId, registry);
  }),
);
const chooseAtom = connectionAtomRuntime.fn(
  (input: { environmentId: EnvironmentId; mode: ReconnectComparisonMode }) =>
    Effect.gen(function* () {
      const service = yield* MobileReconnectComparison;
      const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
      yield* service.choose(input, registry);
    }),
);
const uploadAtom = connectionAtomRuntime.fn((environmentId: EnvironmentId) =>
  Effect.gen(function* () {
    const service = yield* MobileReconnectComparison;
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    yield* service.upload(environmentId, registry);
  }),
);
const shareAtom = connectionAtomRuntime.fn((_input: void) =>
  Effect.gen(function* () {
    const database = yield* MobileDatabase.MobileDatabase;
    const rows = yield* database.listCache("reconnect-comparison");
    yield* Effect.promise(() =>
      Share.share({
        title: "reconnect-comparison-v2.ndjson",
        message: rows.filter((row) => row.startsWith("{")).join("\n"),
      }),
    );
  }),
);

export function ReconnectComparisonSection() {
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const running = useAtomValue(runAtom).waiting;
  const result = useAtomValue(serviceAtom);
  const [selected, setSelected] = useState<EnvironmentId | null>(null);
  const target = selected ?? catalog.entries.keys().next().value ?? null;
  if (!AsyncResult.isSuccess(result)) return null;
  return (
    <View className="gap-3">
      <SettingsSection title="Test environment">
        {[...catalog.entries].map(([id, entry]) => (
          <SelectionRow
            key={id}
            title={entry.target.label}
            selected={id === target}
            onPress={() => setSelected(id)}
            disabled={running}
          />
        ))}
      </SettingsSection>
      {target === null ? null : <LoadedComparison service={result.value} environmentId={target} />}
    </View>
  );
}

function LoadedComparison(props: {
  readonly service: MobileReconnectComparison["Service"];
  readonly environmentId: EnvironmentId;
}) {
  useAtomValue(environmentShell.stateValueAtom(props.environmentId));
  const status = useSyncExternalStore(
    props.service.subscribe,
    props.service.read,
    props.service.read,
  );
  const run = useAtomSet(runAtom);
  const result = useAtomValue(runAtom);
  const choose = useAtomSet(chooseAtom);
  const choosing = useAtomValue(chooseAtom);
  const upload = useAtomSet(uploadAtom);
  const share = useAtomSet(shareAtom);
  const busy = status.running || choosing.waiting;
  return (
    <View className="gap-3">
      <SettingsSection title="Connection mode">
        {(["baseline", "proposed"] as const).map((mode) => (
          <SelectionRow
            key={mode}
            title={mode === "baseline" ? "Baseline" : "Proposed caching"}
            selected={status.mode === mode}
            disabled={busy}
            onPress={() => choose({ environmentId: props.environmentId, mode })}
          />
        ))}
      </SettingsSection>
      <SettingsSection title="Paired reconnect test">
        <ListActionRow
          icon="arrow.clockwise"
          label={`Run ${ROUNDS} pairs`}
          disabled={busy}
          onPress={() => run(props.environmentId)}
        />
        {status.running ? (
          <ListActionRow icon="stop.fill" label="Stop" onPress={() => run(Atom.Interrupt)} />
        ) : null}
        <ListActionRow
          icon="tray.and.arrow.up"
          label="Retry upload"
          disabled={busy}
          onPress={() => upload(props.environmentId)}
        />
        <ListActionRow
          icon="square.and.arrow.up"
          label="Share pending data"
          disabled={busy}
          onPress={() => share()}
        />
      </SettingsSection>
      <Text
        selectable
        className="px-2 font-mono text-xs text-foreground-muted"
      >{`${BUILD_ID}\n${status.completed}/${status.total} attempts · ${status.pending} pending`}</Text>
      {AsyncResult.isFailure(result) ? (
        <Text selectable className="px-2 text-sm text-danger-foreground">
          {String(result.cause).slice(0, 240)}
        </Text>
      ) : null}
      {AsyncResult.isFailure(choosing) ? (
        <Text className="px-2 text-sm text-danger-foreground">
          Could not switch connection mode
        </Text>
      ) : null}
    </View>
  );
}
