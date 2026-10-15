// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Owns only benchmark children captured at spawn.
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as Schema from "effect/Schema";
import { controllerPlan } from "./reconnectNetwork.ts";
import type { ReconnectCase } from "./reconnectManifest.ts";

export function benchmarkLockArguments(
  waitForLock: boolean,
  lockPath: string,
  command: readonly string[],
) {
  return [
    "--no-fork",
    ...(waitForLock ? ["--exclusive"] : ["-n", "--conflict-exit-code", "75"]),
    lockPath,
    ...command,
  ];
}

/** The tracked PID stays the locker/namespace guard; fixture work needs an explicit start grant. */
export function controlBenchmarkCommand(child: NodeChildProcess.ChildProcess) {
  let phase: "waiting" | "active" | "finished" = "waiting";
  let cancellation: "SIGINT" | "SIGTERM" | undefined;
  const send = (message: string) => {
    if (!child.connected) {
      child.kill("SIGTERM");
      return;
    }
    child.send(message, (error) => {
      if (error && phase !== "finished") child.kill("SIGTERM");
    });
  };
  const onMessage = (message: unknown) => {
    if (message !== "benchmark-lock-ready" || phase !== "waiting") return;
    if (cancellation) child.kill(cancellation);
    else {
      phase = "active";
      send("benchmark-start");
    }
  };
  child.on("message", onMessage);
  const done = new Promise<number>((resolve, reject) => {
    child.once("error", (error) => {
      phase = "finished";
      child.off("message", onMessage);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      phase = "finished";
      child.off("message", onMessage);
      resolve(code ?? (signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : 1));
    });
  });
  return {
    done,
    interrupt(signal: "SIGINT" | "SIGTERM") {
      if (phase === "finished" || cancellation) return;
      cancellation = signal;
      if (phase === "waiting") child.kill(signal);
      else send("interrupt");
    },
  };
}

/** Called after interruption handlers exist and before namespace setup, calibration or listeners. */
export function awaitBenchmarkStart() {
  if (!process.send) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const onMessage = (message: unknown) => {
      if (message !== "benchmark-start") return;
      process.off("message", onMessage);
      resolve();
    };
    process.on("message", onMessage);
    process.send!("benchmark-lock-ready", (error) => {
      if (error) {
        process.off("message", onMessage);
        reject(error);
      }
    });
  });
}

export async function stopChild(child: NodeChildProcess.ChildProcess | undefined) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => child.kill("SIGKILL"), 3000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
    child.kill("SIGTERM");
  });
}
export function receive(child: NodeChildProcess.ChildProcess, timeoutMs = 15000): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => done(new Error(`Child ${child.pid} message timeout`)),
      timeoutMs,
    );
    const onError = (error: Error) => done(error);
    const onMessage = (message: unknown) => done(undefined, message);
    const onExit = (code: number | null) => done(new Error(`Child ${child.pid} exited ${code}`));
    const done = (error?: Error, value?: unknown) => {
      clearTimeout(timer);
      child.off("message", onMessage);
      child.off("exit", onExit);
      child.off("error", onError);
      if (error) reject(error);
      else resolve(value);
    };
    child.once("message", onMessage);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}
const ControllerMessage = Schema.Struct({
  type: Schema.Literals(["ready", "applied", "done"]),
  action: Schema.optionalKey(Schema.String),
  plannedMs: Schema.optionalKey(Schema.Number),
  submittedMs: Schema.optionalKey(Schema.Number),
  appliedMs: Schema.optionalKey(Schema.Number),
});
const decodeController = Schema.decodeUnknownSync(Schema.fromJsonString(ControllerMessage));
export async function startController(entry: ReconnectCase, queuePackets: number) {
  const plan = controllerPlan(entry.profile, entry.seed, queuePackets, entry.phaseMs);
  const child = NodeChildProcess.spawn(
    "python3",
    [new URL("./reconnectController.py", import.meta.url).pathname],
    { stdio: ["pipe", "pipe", "inherit"] },
  );
  const records: (typeof ControllerMessage.Type)[] = [];
  let readyResolve!: () => void;
  let readyReject!: (error: Error) => void;
  let failure: Error | undefined;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const fail = (error: Error) => {
    failure = error;
    readyReject(error);
  };
  const timer = setTimeout(() => fail(new Error("Controller readiness timed out")), 5000);
  const done = new Promise<void>((resolve, reject) => {
    child.once("exit", (code) => {
      if (code !== 0 || failure || !records.some((r) => r.type === "done")) {
        const error =
          failure ?? new Error(`Controller exited ${code} before successful completion`);
        readyReject(error);
        reject(error);
      } else resolve();
    });
  });
  // It may fail while the client is still reconnecting. Preserve that failure until awaited.
  void done.catch(() => {});
  const lines = NodeReadline.createInterface({ input: child.stdout! });
  lines.on("line", (line) => {
    try {
      const record = decodeController(line);
      records.push(record);
      if (record.type === "ready") readyResolve();
    } catch (error) {
      fail(new Error(`Invalid controller output: ${String(error)}`));
      child.kill("SIGTERM");
    }
  });
  child.on("error", fail);
  child.stdin!.on("error", fail);
  child.stdin!.write(JSON.stringify(plan) + "\n");
  try {
    await ready;
  } catch (error) {
    await stopChild(child);
    throw error;
  } finally {
    clearTimeout(timer);
  }
  let signalNs: string | undefined;
  return {
    child,
    records,
    done,
    start: () => {
      signalNs = process.hrtime.bigint().toString();
      child.stdin!.end(JSON.stringify({ startNs: signalNs }) + "\n");
    },
    signalNs: () => signalNs,
  };
}
