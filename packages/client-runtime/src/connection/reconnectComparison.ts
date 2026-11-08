import type {
  EnvironmentId,
  OrchestrationV2ShellSnapshot,
  ReconnectComparisonAssignment,
  ReconnectComparisonMode,
  ServerConfig,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface SessionObservation {
  readonly socket: () => void;
  readonly catalogRequest: (revisions: number) => void;
  readonly shellRequest: (hasRevision: boolean) => void;
  readonly config: (config: ServerConfig, reusedCatalogs: number) => void;
  readonly synchronized: (snapshot: OrchestrationV2ShellSnapshot) => Effect.Effect<void>;
}

export class ReconnectComparison extends Context.Reference<{
  readonly mode: (environmentId: EnvironmentId) => ReconnectComparisonMode;
  readonly observe: (
    environmentId: EnvironmentId,
    origin: string,
  ) => SessionObservation | undefined;
} | null>("@t3tools/client-runtime/connection/ReconnectComparison", {
  defaultValue: () => null,
}) {}

export function makeComparisonAssignments(rounds: number, proposedFirst: () => boolean) {
  const result: Array<ReconnectComparisonAssignment> = [];
  for (let round = -1; round < rounds; round++) {
    const order: ReadonlyArray<ReconnectComparisonMode> = proposedFirst()
      ? ["proposed", "baseline"]
      : ["baseline", "proposed"];
    for (const [position, mode] of order.entries()) {
      result.push({
        index: result.length,
        round,
        position: position as 0 | 1,
        mode,
        warmup: round === -1,
      });
    }
  }
  return result;
}
