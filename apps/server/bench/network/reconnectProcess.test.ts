// @effect-diagnostics nodeBuiltinImport:off - Exercises controller acknowledgements without a namespace or tc.
import * as NodeChildProcess from "node:child_process";
import { expect, it } from "@effect/vitest";
import { makeManifest } from "./reconnectManifest.ts";
import { startController, stopChild } from "./reconnectProcess.ts";

const entry = makeManifest(true).cases.find((c) => c.profile.name === "slow-start-500")!;
const mock = `
const readline = require('node:readline');
const lines = readline.createInterface({input: process.stdin});
let plan;
const emit = value => process.stdout.write(JSON.stringify(value)+'\\n');
lines.on('line', line => {
  if (!plan) { plan=JSON.parse(line); emit({type:'ready'}); }
  else process.send('signalled');
});
process.on('message', value => {
  if (value === 'fail') process.exit(1);
  else {
    for (const event of plan) emit({type:'applied',action:event.action,plannedMs:event.atMs,appliedMs:event.atMs});
    emit({type:'done'});
    process.disconnect();
  }
});
`;

it.each([false, true])("waits for initial apply acknowledgement, failure=%s", async (fails) => {
  let owned: NodeChildProcess.ChildProcess | undefined;
  const controller = await startController(entry, 100000, () => {
    owned = NodeChildProcess.spawn(process.execPath, ["-e", mock], {
      stdio: ["pipe", "pipe", "inherit", "ipc"],
    });
    return owned;
  });
  try {
    const signalled = new Promise<void>((resolve) => owned!.once("message", () => resolve()));
    const started = controller.start();
    expect(
      await Promise.race([
        Promise.resolve(started).then(() => "started"),
        signalled.then(() => "signalled"),
      ]),
    ).toBe("signalled");
    owned!.send(fails ? "fail" : "ack");
    if (fails) {
      await expect(started).rejects.toThrow("Controller exited");
      await expect(controller.done).rejects.toThrow("Controller exited");
    } else {
      await started;
      await controller.done;
      expect(controller.records.some((r) => r.type === "applied" && r.plannedMs === 0)).toBe(true);
    }
  } finally {
    await stopChild(owned);
  }
});
