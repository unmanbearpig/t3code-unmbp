import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_VERSION,
  type ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";
import {
  BearerConnectionTarget,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
} from "./model.ts";
import { eligibleRpcWebTransport } from "./rpcWebTransport.ts";

const environmentId = EnvironmentId.make("environment");
const target = new PrimaryConnectionTarget({
  environmentId,
  label: "Primary",
  httpBaseUrl: "https://host.example",
  wsBaseUrl: "wss://host.example",
});
const descriptor: ExecutionEnvironmentDescriptor = {
  environmentId,
  label: "Primary",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "test",
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  capabilities: { repositoryIdentity: true },
  rpcWebTransport: { url: "https://host.example:4443/rpc", wireVersion: 1 },
};
const input = { target, httpBaseUrl: target.httpBaseUrl, descriptor, connectorAvailable: true };

describe("RPC WebTransport route eligibility", () => {
  it.each([
    target,
    new BearerConnectionTarget({ environmentId, label: "Bearer", connectionId: "saved" }),
    new BearerConnectionTarget({
      environmentId,
      label: "Learned Connect route",
      connectionId: "learned:connect",
    }),
  ])("allows direct HTTPS target $_tag across ports", (target) => {
    expect(eligibleRpcWebTransport({ ...input, target })).toEqual({
      url: "https://host.example:4443/rpc",
      wireVersion: 1,
      orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    });
  });
  it.each([
    new RelayConnectionTarget({ environmentId, label: "Relay" }),
    new SshConnectionTarget({ environmentId, label: "SSH", connectionId: "ssh" }),
  ])("keeps $_tag on WSS", (target) => {
    expect(eligibleRpcWebTransport({ ...input, target })).toBeUndefined();
  });
  it.each([
    "http://host.example/rpc",
    "https://other.example/rpc",
    "https://user@host.example/rpc",
    "https://user:password@host.example/rpc",
    "https://host.example/rpc?ticket=secret",
    "https://host.example/rpc?",
    "https://host.example/rpc#fragment",
    "https://host.example/rpc#",
    "/rpc",
    "wss://host.example/rpc",
    "garbage",
  ])("rejects advertised URL %s", (url) => {
    expect(
      eligibleRpcWebTransport({
        ...input,
        descriptor: { ...descriptor, rpcWebTransport: { url, wireVersion: 1 } },
      }),
    ).toBeUndefined();
  });
  it("normalizes hostnames while comparing discovery", () => {
    expect(
      eligibleRpcWebTransport({ ...input, httpBaseUrl: "https://HOST.EXAMPLE.:443" }),
    ).toBeDefined();
  });
  it.each([0, 2, -1, 1.5])("rejects unsupported wire version %s", (wireVersion) => {
    expect(
      eligibleRpcWebTransport({
        ...input,
        descriptor: {
          ...descriptor,
          rpcWebTransport: { url: "https://host.example/rpc", wireVersion },
        },
      }),
    ).toBeUndefined();
  });
  it("requires HTTPS discovery, an advertisement, and a usable connector", () => {
    expect(
      eligibleRpcWebTransport({ ...input, httpBaseUrl: "http://host.example" }),
    ).toBeUndefined();
    expect(eligibleRpcWebTransport({ ...input, connectorAvailable: false })).toBeUndefined();
    const { rpcWebTransport: _, ...older } = descriptor;
    expect(eligibleRpcWebTransport({ ...input, descriptor: older })).toBeUndefined();
  });
});
