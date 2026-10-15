import * as Schema from "effect/Schema";
import { ExecutionEnvironmentDescriptor, ReconnectResumeCapabilities } from "./environment.ts";

export const RECONNECT_HANDSHAKE_QUERY_PARAM = "handshake";
export const RECONNECT_HANDSHAKE_MODE = "hello-v1";
export const RECONNECT_AUTH_DEADLINE_MS = 5_000;
export const RECONNECT_AUTH_MAX_BYTES = 8 * 1024;
export const RECONNECT_BUFFER_MAX_BYTES = 64 * 1024;
export const RECONNECT_BUFFER_MAX_FRAMES = 16;

export const ReconnectHello = Schema.Struct({
  _tag: Schema.Literal("Hello"),
  handshakeVersion: Schema.Literal(1),
  environment: ExecutionEnvironmentDescriptor,
  resume: ReconnectResumeCapabilities,
});
export type ReconnectHello = typeof ReconnectHello.Type;

// Never include decode errors or this frame's payload in diagnostics.
export const ReconnectBearerAuth = Schema.Struct({
  _tag: Schema.Literal("BearerAuth"),
  handshakeVersion: Schema.Literal(1),
  token: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(RECONNECT_AUTH_MAX_BYTES)),
});
export const ReconnectAuthenticated = Schema.Struct({
  _tag: Schema.Literal("Authenticated"),
  handshakeVersion: Schema.Literal(1),
});
export const ReconnectHandshakeError = Schema.Struct({
  _tag: Schema.Literal("HandshakeError"),
  handshakeVersion: Schema.Literal(1),
  code: Schema.Literals([
    "invalid_frame",
    "auth_rejected",
    "auth_timeout",
    "limit_exceeded",
    "internal_error",
  ]),
});
export type ReconnectHandshakeError = typeof ReconnectHandshakeError.Type;
export const ReconnectServerControl = Schema.Union([
  ReconnectHello,
  ReconnectAuthenticated,
  ReconnectHandshakeError,
]);
