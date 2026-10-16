import * as Context from "effect/Context";

export type ReconnectArm = "old" | "new";

/** Phone-only field trial. The platform owns randomization and durable settings. */
export class ReconnectExperiment extends Context.Reference<{
  readonly environments?: (ids: ReadonlyArray<string>) => void;
  readonly begin: (environmentId: string, reset: boolean) => void;
  readonly arm: (environmentId: string) => ReconnectArm;
} | null>("@t3tools/client-runtime/connection/ReconnectExperiment", {
  defaultValue: () => null,
}) {}

/** Immutable for a whole attempt, including its transport finalizers. */
export class AttemptReconnectArm extends Context.Reference<ReconnectArm>(
  "@t3tools/client-runtime/connection/AttemptReconnectArm",
  { defaultValue: () => "new" },
) {}
