import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

export type PerformanceOperation =
  | "startup.navigation-ready"
  | "startup.splash-hide-requested"
  | "lifecycle.resume-js"
  | "runtime.js-delay"
  | "runtime.memory-warning"
  | "cache.read"
  | "cache.decode"
  | "cache.encode"
  | "cache.write"
  | "shell.apply-batch"
  | "thread.apply-batch"
  | "rpc.request";

export interface PerformanceMeasurement {
  readonly operation: PerformanceOperation;
  readonly durationMs: number;
  readonly outcome: "success" | "failure" | "interrupted";
  readonly category?: "shell" | "thread" | "server-config" | "vcs-refs";
  readonly itemCount?: number;
  /** String length, deliberately not described as UTF-8 bytes. */
  readonly payloadChars?: number;
  /** Static RPC tag only. Inputs, outputs, identifiers, and error text are excluded. */
  readonly rpcMethod?: string;
}

export interface PerformanceObserver {
  readonly record: (measurement: PerformanceMeasurement) => void;
}

/** Platform-owned local telemetry. Uninstrumented clients take the original path. */
export class PerformanceTelemetry extends Context.Reference<PerformanceObserver | null>(
  "@t3tools/client-runtime/PerformanceTelemetry",
  { defaultValue: () => null },
) {}

/** Measure elapsed work while preserving its result, failure, and interruption. */
export function measure<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  observer: PerformanceObserver | null,
  measurement: Omit<PerformanceMeasurement, "durationMs" | "outcome">,
  resultDetails?: (value: A) => Pick<PerformanceMeasurement, "payloadChars" | "itemCount">,
): Effect.Effect<A, E, R> {
  if (observer === null) return effect;
  return Effect.gen(function* () {
    const started = yield* Clock.currentTimeNanos;
    return yield* effect.pipe(
      Effect.onExit((exit) =>
        Clock.currentTimeNanos.pipe(
          Effect.map((ended) => {
            try {
              observer.record({
                ...measurement,
                ...(Exit.isSuccess(exit) ? resultDetails?.(exit.value) : undefined),
                durationMs: Math.max(0, Number(ended - started) / 1_000_000),
                outcome: Exit.isSuccess(exit)
                  ? "success"
                  : Cause.hasInterruptsOnly(exit.cause)
                    ? "interrupted"
                    : "failure",
              });
            } catch {
              // Optional telemetry must never turn a successful operation into a failure.
            }
          }),
        ),
      ),
    );
  });
}
