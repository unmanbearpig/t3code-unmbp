import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";

const [nativeRoot, mode] = process.argv.slice(2);
const udp = new NodeEvents.EventEmitter();
const calls = [];
const sends = [];
let closeFramesSent = 0;
let closed = false;
let queuedPackets = 0;
let buffered = true;
let duringProcess;
const initialized = Promise.withResolvers();
Object.assign(udp, {
  bind: () => initialized.resolve(),
  address: () => ({ address: "127.0.0.1", port: 4433 }),
  getSendQueueCount: () => queuedPackets,
  send: (message, ...args) => {
    if (message.toString() === "close") closeFramesSent += 1;
    sends.push(args.at(-1));
  },
  close: () => {
    NodeAssert.equal(closed, true);
    calls.push("socket.close");
    udp.emit("close");
  },
});
globalThis.__quicSocketTest = {
  lookup: async () => ({ address: "127.0.0.1", family: 4 }),
  createSocket: () => udp,
};
const hook = NodeModule.registerHooks({
  resolve(specifier, context, nextResolve) {
    const exports =
      specifier === "node:dgram"
        ? "export const createSocket = (...args) => globalThis.__quicSocketTest.createSocket(...args)"
        : specifier === "node:dns/promises"
          ? "export const lookup = (...args) => globalThis.__quicSocketTest.lookup(...args)"
          : undefined;
    return exports
      ? { shortCircuit: true, url: `data:text/javascript,${encodeURIComponent(exports)}` }
      : nextResolve(specifier, context);
  },
});
const nativeCall = (name) => {
  NodeAssert.equal(closed, false, `native ${name} after close`);
  calls.push(name);
};
const native = {
  setHostname: () => nativeCall("setHostname"),
  onCanWrite: () => nativeCall("onCanWrite"),
  recvPaket: () => {
    nativeCall("recvPaket");
    return buffered;
  },
  processBufferedChlos: () => {
    nativeCall("processBufferedChlos");
    duringProcess?.();
  },
  destroy: () => {
    nativeCall("destroy");
    // Native shutdown must still be able to send its final close frames.
    socket.sendPacket({
      msg: Buffer.from("close"),
      offset: 0,
      length: 5,
      port: 4433,
      address: "127.0.0.1",
    });
    closed = true;
  },
  closeClientInt: () => native.destroy(),
};
const client = mode === "client-close";
const exported = client ? "Http3WebTransportClientSocket" : "Http3WebTransportServerSocket";
const moduleName = client ? "clientsocket.js" : "serversocket.js";
const { [exported]: Socket } = await import(
  NodeURL.pathToFileURL(NodePath.join(nativeRoot, "lib", moduleName)).href
);
const socket = new Socket({ host: "localhost", port: 4433 });
socket.cobj = native;
socket.jsobj = {
  onServerListening: () => {},
  onServerError: (error) => {
    throw error;
  },
  onClientError: (error) => {
    throw error;
  },
  onServerClose: () => {
    if (mode === "send-completion") socket.stopServer();
  },
};
socket.init();
await initialized.promise;
const packet = () =>
  udp.emit("message", Buffer.from("packet"), { address: "127.0.0.1", port: 4434 });
const count = (name) => calls.filter((value) => value === name).length;
try {
  if (mode === "server-stop") {
    packet();
    NodeAssert.equal(socket.chlosSched, true);
    socket.stopServer();
    await NodeTimersPromises.setImmediate();
    socket.stopServer();
    packet();
    NodeAssert.equal(count("processBufferedChlos"), 0);
    NodeAssert.equal(count("recvPaket"), 1);
    NodeAssert.equal(socket.chlosSched, false);
  } else if (mode === "buffered-processing") {
    packet();
    packet();
    await NodeTimersPromises.setImmediate();
    NodeAssert.equal(count("processBufferedChlos"), 1);
    NodeAssert.equal(socket.chlosSched, false);
    // A packet delivered by a native callback can queue the next buffered batch.
    duringProcess = () => {
      duringProcess = undefined;
      packet();
    };
    packet();
    await NodeTimersPromises.setImmediate();
    NodeAssert.equal(count("processBufferedChlos"), 2);
    NodeAssert.equal(socket.chlosSched, true);
    await NodeTimersPromises.setImmediate();
    NodeAssert.equal(count("processBufferedChlos"), 3);
    NodeAssert.equal(socket.chlosSched, false);
    buffered = false;
    packet();
    await NodeTimersPromises.setImmediate();
    NodeAssert.equal(count("processBufferedChlos"), 3);
    socket.stopServer();
  } else if (client) {
    socket.closeClient();
    await new Promise((resolve) => process.nextTick(resolve));
    await NodeTimersPromises.setImmediate();
    socket.closeClient();
    socket.closeClient();
    await new Promise((resolve) => process.nextTick(resolve));
    packet();
    NodeAssert.equal(count("onCanWrite"), 0);
    NodeAssert.equal(count("recvPaket"), 0);
  } else if (mode === "send-completion") {
    queuedPackets = 1;
    socket.sendPacket({
      msg: Buffer.from("data"),
      offset: 0,
      length: 4,
      port: 4433,
      address: "127.0.0.1",
    });
    queuedPackets = 0;
    sends.shift()();
    NodeAssert.equal(count("onCanWrite"), 2);
    queuedPackets = 1;
    socket.sendPacket({
      msg: Buffer.from("data"),
      offset: 0,
      length: 4,
      port: 4433,
      address: "127.0.0.1",
    });
    socket.stopServer();
    queuedPackets = 0;
    for (const callback of sends.splice(0)) callback();
    NodeAssert.equal(count("onCanWrite"), 2);
  } else {
    throw new Error(`Unknown fixture mode ${mode}`);
  }
  NodeAssert.equal(count("destroy"), 1);
  NodeAssert.equal(count("socket.close"), 1);
  NodeAssert.equal(closeFramesSent, 1);
  console.log(JSON.stringify({ mode, calls, closed: socket.closed, closeFramesSent }));
} finally {
  hook.deregister();
  delete globalThis.__quicSocketTest;
}
