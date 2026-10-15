// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Owns only benchmark children captured at spawn.
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as Schema from "effect/Schema";
import { controllerPlan } from "./reconnectNetwork.ts";
import type { ReconnectCase } from "./reconnectManifest.ts";

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
const ControllerMessage = Schema.Struct({
  type: Schema.Literals(["ready", "applied", "done"]),
  action: Schema.optionalKey(Schema.String),
  plannedMs: Schema.optionalKey(Schema.Number),
  submittedMs: Schema.optionalKey(Schema.Number),
  appliedMs: Schema.optionalKey(Schema.Number),
});
const decodeController = Schema.decodeUnknownSync(Schema.fromJsonString(ControllerMessage));
export async function startController(
  entry: ReconnectCase,
  queuePackets: number,
  spawn: () => NodeChildProcess.ChildProcess = () =>
    NodeChildProcess.spawn(
      "python3",
      [new URL("./reconnectController.py", import.meta.url).pathname],
      { stdio: ["pipe", "pipe", "inherit"] },
    ),
) {
  const plan = controllerPlan(entry.profile, entry.seed, queuePackets, entry.phaseMs);
  if (!plan.length) {
    let signalNs: string | undefined;
    return {
      child: undefined,
      records: [{ type: "ready" as const }, { type: "done" as const }],
      done: Promise.resolve(),
      start: () => {
        signalNs = process.hrtime.bigint().toString();
      },
      signalNs: () => signalNs,
    };
  }

  const child = spawn();
  const records: (typeof ControllerMessage.Type)[] = [];
  const initialChanges = plan.filter((event) => event.atMs === 0).length;
  let initialApplied = 0;
  let initialResolve!: () => void;
  let initialReject!: (error: Error) => void;
  const initial = new Promise<void>((resolve, reject) => {
    initialResolve = resolve;
    initialReject = reject;
  });
  void initial.catch(() => {});
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
    initialReject(error);
  };
  const timer = setTimeout(() => fail(new Error("Controller readiness timed out")), 5000);
  const done = new Promise<void>((resolve, reject) => {
    child.once("exit", (code) => {
      if (
        code !== 0 ||
        failure ||
        initialApplied < initialChanges ||
        !records.some((r) => r.type === "done")
      ) {
        const error =
          failure ?? new Error(`Controller exited ${code} before successful completion`);
        readyReject(error);
        initialReject(error);
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
      if (record.type === "applied" && record.plannedMs === 0) {
        initialApplied++;
        if (initialApplied === initialChanges) initialResolve();
      }
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
      // Prevent a SYN or probe escaping a nominally immediate outage/spike.
      if (initialChanges) return initial;
    },
    signalNs: () => signalNs,
  };
}

/** Install immediately after spawning so fast child messages cannot race the next receive. */
export function mailbox(
  source: Pick<NodeChildProcess.ChildProcess, "on" | "off"> | NodeJS.Process,
) {
  const queue: unknown[] = [];
  let waiter: { resolve: (value: unknown) => void; reject: (error: Error) => void } | undefined;
  let failure: Error | undefined;
  const onMessage = (message: unknown) => {
    if (waiter) {
      const next = waiter;
      waiter = undefined;
      next.resolve(message);
    } else queue.push(message);
  };
  const onExit = () => {
    failure = new Error("Message source exited");
    waiter?.reject(failure);
    waiter = undefined;
  };
  source.on("message", onMessage);
  source.on("exit", onExit);
  source.on("error", onExit);
  return {
    next: (timeoutMs = 120000) =>
      new Promise<unknown>((resolve, reject) => {
        if (queue.length) {
          resolve(queue.shift());
          return;
        }
        if (failure) {
          reject(failure);
          return;
        }
        if (waiter) {
          reject(new Error("Concurrent mailbox receives"));
          return;
        }
        const timer = setTimeout(() => {
          waiter = undefined;
          reject(new Error("Mailbox timeout"));
        }, timeoutMs);
        waiter = {
          resolve: (value) => {
            clearTimeout(timer);
            resolve(value);
          },
          reject: (error) => {
            clearTimeout(timer);
            reject(error);
          },
        };
      }),
    close: () => {
      source.off("message", onMessage);
      source.off("exit", onExit);
      source.off("error", onExit);
    },
  };
}
