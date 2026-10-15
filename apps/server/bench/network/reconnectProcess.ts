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
export function receive(child: NodeChildProcess.ChildProcess, timeoutMs = 15000): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => done(new Error(`Child ${child.pid} message timeout`)),
      timeoutMs,
    );
    const onMessage = (message: unknown) => done(undefined, message);
    const onExit = (code: number | null) => done(new Error(`Child ${child.pid} exited ${code}`));
    const done = (error?: Error, value?: unknown) => {
      clearTimeout(timer);
      child.off("message", onMessage);
      child.off("exit", onExit);
      if (error) reject(error);
      else resolve(value);
    };
    child.once("message", onMessage);
    child.once("exit", onExit);
    child.once("error", reject);
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
  const child = NodeChildProcess.spawn(
    "python3",
    [new URL("./reconnectController.py", import.meta.url).pathname],
    { stdio: ["pipe", "pipe", "inherit"] },
  );
  const records: (typeof ControllerMessage.Type)[] = [];
  let readyResolve!: () => void;
  let doneResolve!: () => void;
  let fail!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    fail = reject;
  });
  const done = new Promise<void>((resolve) => {
    doneResolve = resolve;
  });
  const lines = NodeReadline.createInterface({ input: child.stdout! });
  lines.on("line", (line) => {
    const record = decodeController(line);
    records.push(record);
    if (record.type === "ready") readyResolve();
    if (record.type === "done") doneResolve();
  });
  child.on("error", fail);
  child.once("exit", (code) => {
    if (code !== 0) fail(new Error(`Controller exited ${code}`));
  });
  child.stdin!.write(
    JSON.stringify(controllerPlan(entry.profile, entry.seed, queuePackets, entry.phaseMs)) + "\n",
  );
  await ready;
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
