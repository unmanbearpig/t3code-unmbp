import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { EnvironmentId } from "./baseSchemas.ts";
import { ExecutionEnvironmentDescriptor } from "./environment.ts";
import {
  ReconnectBearerAuth,
  ReconnectHello,
  ReconnectHandshakeError,
} from "./reconnectHandshake.ts";

const descriptor = {
  environmentId: EnvironmentId.make("test"),
  label: "Test",
  serverVersion: "test",
  platform: { os: "linux", arch: "x64" },
  capabilities: { repositoryIdentity: true },
};
const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
const decodeHello = Schema.decodeUnknownSync(ReconnectHello);
const decodeAuth = Schema.decodeUnknownSync(ReconnectBearerAuth);
const decodeError = Schema.decodeUnknownSync(ReconnectHandshakeError);
describe("reconnect handshake wire contracts", () => {
  it("keeps descriptors without hello support valid, and ignores future hello support versions", () => {
    expect(decodeDescriptor(descriptor).capabilities.websocketHello).toBeUndefined();
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, websocketHello: 2 },
      }).capabilities.websocketHello,
    ).toBeUndefined();
  });
  it("versions Hello independently from orchestration and only exposes its public fields", () => {
    const frame = {
      _tag: "Hello",
      handshakeVersion: 1,
      environment: descriptor,
      resume: { threadResumeCompletionMarker: true },
      settings: { private: true },
    };
    expect(decodeHello(frame)).not.toHaveProperty("settings");
    expect(decodeHello(frame).resume.threadResumeCompletionMarker).toBe(true);
    expect(() => decodeHello({ ...frame, handshakeVersion: 2 })).toThrow();
  });
  it("requires a nonempty bounded bearer frame and a bounded error code", () => {
    expect(() => decodeAuth({ _tag: "BearerAuth", handshakeVersion: 1, token: "" })).toThrow();
    expect(() =>
      decodeAuth({ _tag: "BearerAuth", handshakeVersion: 1, token: "x".repeat(8193) }),
    ).toThrow();
    expect(() =>
      decodeError({ _tag: "HandshakeError", handshakeVersion: 1, code: "arbitrary diagnostic" }),
    ).toThrow();
  });
});
