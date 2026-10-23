import * as Schema from "effect/Schema";

const ConfigSchema = Schema.Struct({
  url: Schema.String,
  host: Schema.NonEmptyString,
  port: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })),
  certPath: Schema.NonEmptyString,
  keyPath: Schema.NonEmptyString,
});

const decodeConfig = Schema.decodeSync(Schema.fromJsonString(ConfigSchema));

type WebTransportConfig = typeof ConfigSchema.Type;

export class WebTransportUnavailableError extends Schema.TaggedError<WebTransportUnavailableError>()(
  "WebTransportUnavailableError",
  {
    stage: Schema.Literals(["config", "certificate", "backend", "listen", "admission", "session"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `RPC WebTransport is unavailable at ${this.stage}.`;
  }
}

/** Invalid opt-in disables QUIC without making HTTP/WSS configuration fail. */
export function parseWebTransportConfig(raw: string): WebTransportConfig {
  const config = decodeConfig(raw);
  const url = new URL(config.url);
  const authority = /^https:\/\/([^/?#]*)/iu.exec(config.url)?.[1];
  if (
    config.url.trim() !== config.url ||
    url.protocol !== "https:" ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    config.url.includes("?") ||
    config.url.includes("#") ||
    authority?.includes("@") ||
    config.host.trim() !== config.host ||
    !config.host.trim() ||
    /[\s\0]/u.test(config.host)
  )
    throw new WebTransportUnavailableError({ stage: "config" });
  return { ...config, url: url.toString() };
}
