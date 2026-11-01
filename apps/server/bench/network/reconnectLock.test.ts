// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Disposable OS-lock tests own and reap only their captured children; deadlines fail stalled fixtures, never select passing order.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import { expect, it } from "@effect/vitest";
import { benchmarkLockArguments, controlBenchmarkCommand, stopChild } from "./reconnectProcess.ts";

const hasFlock =
  NodeChildProcess.spawnSync("flock", ["--version"], { stdio: "ignore" }).status === 0;
const namespaceArgs = ["--user", "--map-root-user", "--pid", "--fork", "--kill-child=SIGTERM"];
const hasPidNamespace =
  hasFlock &&
  NodeChildProcess.spawnSync("unshare", [...namespaceArgs, "true"], { stdio: "ignore" }).status ===
    0;
const processModule = new URL("./reconnectProcess.ts", import.meta.url).href;
const controlledChild = String.raw`
import * as fs from 'node:fs';
import * as readline from 'node:readline';
const [journal, label, role, helper, disconnectCleanup] = process.argv.slice(1);
const mark = event => {
  fs.appendFileSync(journal, JSON.stringify({label, event, pid:process.pid})+'\n');
  process.stdout.write(event+'\n');
};
let cleaning = false;
const interrupted = () => {
  if (cleaning) return;
  cleaning = true;
  mark('cleanup-start');
};
const command = message => {
  if (message === 'finish') process.exit(cleaning ? 143 : 0);
  if (message === 'interrupt') interrupted();
  if (message === 'release-cleanup') {
    mark('cleanup-done');
    process.exit(143);
  }
};
process.on('message', command);
readline.createInterface({input:process.stdin}).on('line', command);
process.on('disconnect', () => {
  mark('ipc-disconnected');
  if (disconnectCleanup !== 'ignore') interrupted();
});
process.on('SIGINT', () => process.exit(130));
process.on('SIGTERM', () => process.exit(143));
if (role === 'holder') mark('holding');
else {
  const {awaitBenchmarkStart} = await import(helper);
  await awaitBenchmarkStart();
  mark('started');
  if (role !== 'active') process.exit(0);
}
`;

function bounded<T>(pending: Promise<T>, label: string) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Controlled lock fixture stalled: ${label}`)),
      5000,
    );
    pending.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function harness(name: string) {
  const scratch = NodeFS.mkdtempSync(
    NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-lock-test-"),
  );
  const lock = NodePath.join(scratch, "disposable.lock");
  const journal = NodePath.join(scratch, "events.jsonl");
  const children: NodeChildProcess.ChildProcess[] = [];
  const exits: {
    label: string;
    pid: number | undefined;
    code: number | null;
    signal: NodeJS.Signals | null;
  }[] = [];
  const launch = (
    label: string,
    role: "holder" | "queued" | "active",
    options: {
      wait?: boolean;
      controlled?: boolean;
      namespace?: boolean;
      ignoreDisconnect?: boolean;
    } = {},
  ) => {
    const node = [
      process.execPath,
      "--input-type=module",
      "-e",
      controlledChild,
      journal,
      label,
      role,
      processModule,
      options.ignoreDisconnect ? "ignore" : "cleanup",
    ];
    const command = options.namespace ? ["unshare", ...namespaceArgs, ...node] : node;
    const child = NodeChildProcess.spawn(
      "flock",
      benchmarkLockArguments(options.wait !== false, lock, command),
      { stdio: ["pipe", "pipe", "pipe", "ipc"] },
    );
    children.push(child);
    const controller = options.controlled ? controlBenchmarkCommand(child) : undefined;
    const lines = new Set<string>();
    const notices = new Map<string, () => void>();
    NodeReadline.createInterface({ input: child.stdout! }).on("line", (line) => {
      lines.add(line);
      notices.get(line)?.();
    });
    const exit = new Promise<number>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        exits.push({ label, pid: child.pid, code, signal });
        resolve(code ?? (signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : 1));
      });
    });
    const line = (value: string) =>
      bounded(
        Promise.race([
          lines.has(value)
            ? Promise.resolve()
            : new Promise<void>((resolve) => notices.set(value, resolve)),
          exit.then(() => {
            throw new Error(`${label} exited before ${value}`);
          }),
        ]),
        `${label}/${value}`,
      );
    return {
      child,
      controller,
      line,
      done: () => bounded(controller?.done ?? exit, `${label}/exit`),
    };
  };
  const events = () =>
    NodeFS.existsSync(journal)
      ? NodeFS.readFileSync(journal, "utf8")
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line) as { label: string; event: string; pid: number })
      : [];
  return {
    launch,
    events,
    async close() {
      for (const child of children) if (child.connected) child.send("finish", () => {});
      await Promise.all(children.map(stopChild));
      const evidenceDirectory = process.env.RPC_RECONNECT_LOCK_EVIDENCE_DIR;
      if (evidenceDirectory) {
        NodeFS.mkdirSync(evidenceDirectory, { recursive: true });
        NodeFS.writeFileSync(
          NodePath.join(evidenceDirectory, `${name}.json`),
          JSON.stringify(
            {
              name,
              realBenchmarkLockTouched: false,
              fixtureListenersStarted: 0,
              events: events(),
              startGrantsByLabel: Object.fromEntries(
                exits.map(({ label }) => [
                  label,
                  events().filter((event) => event.label === label && event.event === "started")
                    .length,
                ]),
              ),
              exits,
              allCapturedChildrenReaped: children.every(
                (child) => child.exitCode !== null || child.signalCode !== null,
              ),
            },
            null,
            2,
          ) + "\n",
        );
      }
      NodeFS.rmSync(scratch, { recursive: true, force: true });
    },
  };
}

it.skipIf(!hasFlock).each(["SIGINT", "SIGTERM"] as const)(
  "cancels a waiting %s before its protected command can launch",
  async (signal) => {
    const h = harness(`waiting-${signal}`);
    try {
      const holder = h.launch("holder", "holder");
      await holder.line("holding");
      const queued = h.launch("queued", "queued", { controlled: true });
      queued.controller!.interrupt(signal);
      expect(await queued.done()).toBe(signal === "SIGINT" ? 130 : 143);
      expect(holder.child.exitCode).toBeNull();
      holder.child.send("finish");
      expect(await holder.done()).toBe(0);
      expect(h.events().some((event) => event.label === "queued")).toBe(false);
      const after = h.launch("after-cancellation", "queued", { controlled: true });
      expect(await after.done()).toBe(0);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "holder/holding",
        "after-cancellation/started",
      ]);
    } finally {
      await h.close();
    }
  },
  20000,
);

it.skipIf(!hasPidNamespace).each(["SIGINT", "SIGTERM"] as const)(
  "cancels acquired %s before a start grant and retains the namespace lock through cleanup",
  async (signal) => {
    const h = harness(`acquired-${signal}`);
    try {
      const acquired = h.launch("acquired", "active", { controlled: true, namespace: true });
      acquired.child.prependListener("message", (message) => {
        if (message === "benchmark-lock-ready") acquired.controller!.interrupt(signal);
      });
      await acquired.line("cleanup-start");
      expect(acquired.child.exitCode).toBeNull();
      const contender = h.launch("contender", "queued", { controlled: true });
      const denied = h.launch("denied-during-cleanup", "queued", { controlled: true, wait: false });
      expect(await denied.done()).toBe(75);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "acquired/cleanup-start",
      ]);
      acquired.child.send("release-cleanup");
      expect(await acquired.done()).toBe(143);
      expect(acquired.child.signalCode).toBeNull();
      expect(await contender.done()).toBe(0);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "acquired/cleanup-start",
        "acquired/cleanup-done",
        "contender/started",
      ]);
    } finally {
      await h.close();
    }
  },
  20000,
);

it.skipIf(!hasPidNamespace).each(["disconnected", "send-error", "send-throws"] as const)(
  "cleans up an acquired namespace after %s IPC without granting start",
  async (failure) => {
    const h = harness(`failed-ipc-${failure}`);
    try {
      const acquired = h.launch("acquired", "active", { controlled: true, namespace: true });
      if (failure !== "disconnected") {
        acquired.child.send = (_message: NodeChildProcess.Serializable, ...args: unknown[]) => {
          const error = new Error("Controlled IPC failure");
          if (failure === "send-throws") throw error;
          const callback = args.find(
            (arg): arg is (error: Error | null) => void => typeof arg === "function",
          );
          callback?.(error);
          return false;
        };
      }
      acquired.child.prependListener("message", (message) => {
        if (message !== "benchmark-lock-ready") return;
        if (failure === "disconnected") acquired.child.disconnect();
        acquired.controller!.interrupt("SIGTERM");
      });
      await acquired.line("cleanup-start");
      expect(acquired.child.exitCode).toBeNull();
      const contender = h.launch("contender", "queued", { controlled: true });
      const denied = h.launch("denied-during-cleanup", "queued", { controlled: true, wait: false });
      expect(await denied.done()).toBe(75);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "acquired/ipc-disconnected",
        "acquired/cleanup-start",
      ]);
      acquired.child.stdin!.write("release-cleanup\n");
      expect(await acquired.done()).toBe(143);
      expect(acquired.child.signalCode).toBeNull();
      expect(await contender.done()).toBe(0);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "acquired/ipc-disconnected",
        "acquired/cleanup-start",
        "acquired/cleanup-done",
        "contender/started",
      ]);
    } finally {
      await h.close();
    }
  },
  20000,
);

it.skipIf(!hasPidNamespace)(
  "reaps an unresponsive namespace guard after start-grant IPC fails",
  async () => {
    const h = harness("failed-start-ipc-escalation");
    try {
      const acquired = h.launch("acquired", "active", {
        controlled: true,
        namespace: true,
        ignoreDisconnect: true,
      });
      acquired.child.send = (_message: NodeChildProcess.Serializable, ...args: unknown[]) => {
        const callback = args.find(
          (arg): arg is (error: Error | null) => void => typeof arg === "function",
        );
        callback?.(new Error("Controlled start-grant IPC failure"));
        return false;
      };
      await acquired.line("ipc-disconnected");
      const denied = h.launch("denied-before-escalation", "queued", {
        controlled: true,
        wait: false,
      });
      expect(await denied.done()).toBe(75);
      expect(await acquired.done()).toBe(1);
      expect(acquired.child.signalCode).toBe("SIGKILL");
      const after = h.launch("after-escalation", "queued", { controlled: true });
      expect(await after.done()).toBe(0);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "acquired/ipc-disconnected",
        "after-escalation/started",
      ]);
    } finally {
      await h.close();
    }
  },
  20000,
);

it.skipIf(!hasPidNamespace).each(["SIGINT", "SIGTERM"] as const)(
  "keeps the namespace lock through active %s cleanup and reaps its guard",
  async (signal) => {
    const h = harness(`active-${signal}`);
    try {
      const active = h.launch("active", "active", { controlled: true, namespace: true });
      await active.line("started");
      active.controller!.interrupt(signal);
      await active.line("cleanup-start");
      expect(active.child.exitCode).toBeNull();
      const contender = h.launch("contender", "queued", { controlled: true });
      const denied = h.launch("denied-during-cleanup", "queued", { controlled: true, wait: false });
      expect(await denied.done()).toBe(75);
      expect(h.events().some((event) => event.label === "contender")).toBe(false);
      active.child.send("release-cleanup");
      expect(await active.done()).toBe(143);
      expect(await contender.done()).toBe(0);
      expect(h.events().map((event) => `${event.label}/${event.event}`)).toEqual([
        "active/started",
        "active/cleanup-start",
        "active/cleanup-done",
        "contender/started",
      ]);
    } finally {
      await h.close();
    }
  },
  20000,
);
