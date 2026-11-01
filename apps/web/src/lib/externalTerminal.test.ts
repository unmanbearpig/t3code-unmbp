import {
  BearerConnectionTarget,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
} from "@t3tools/client-runtime/connection";
import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { externalTerminalCandidateUrls } from "./externalTerminal";

const environmentId = EnvironmentId.make("environment");
const directEndpoints = [{ kind: "tailnet", httpBaseUrl: "http://100.101.102.103:3773" }] as const;

describe("external terminal connection candidates", () => {
  it("uses the actual URL for desktop-managed and separately managed servers", () => {
    const url = "https://laptop.tail123.ts.net";
    expect(
      externalTerminalCandidateUrls({
        target: new PrimaryConnectionTarget({
          environmentId,
          label: "Local",
          httpBaseUrl: url,
          wsBaseUrl: "wss://laptop.tail123.ts.net",
        }),
        httpBaseUrl: null,
        directEndpoints: undefined,
      }),
    ).toEqual([url]);
    expect(
      externalTerminalCandidateUrls({
        target: new BearerConnectionTarget({
          environmentId,
          label: "Local",
          connectionId: "saved",
        }),
        httpBaseUrl: url,
        directEndpoints,
      }),
    ).toEqual([url, directEndpoints[0].httpBaseUrl]);
  });

  it("uses advertised endpoints for relay connections", () => {
    expect(
      externalTerminalCandidateUrls({
        target: new RelayConnectionTarget({ environmentId, label: "Relay" }),
        httpBaseUrl: "https://relay.example.com",
        directEndpoints,
      }),
    ).toEqual([directEndpoints[0].httpBaseUrl]);
  });

  it("does not mistake an SSH tunnel's loopback listener for the environment host", () => {
    expect(
      externalTerminalCandidateUrls({
        target: new SshConnectionTarget({ environmentId, label: "Remote", connectionId: "ssh" }),
        httpBaseUrl: "http://127.0.0.1:43210",
        directEndpoints,
      }),
    ).toEqual([directEndpoints[0].httpBaseUrl]);
  });

  it("ignores loopback and malformed advertised endpoints and deduplicates routes", () => {
    expect(
      externalTerminalCandidateUrls({
        target: new BearerConnectionTarget({
          environmentId,
          label: "Local",
          connectionId: "saved",
        }),
        httpBaseUrl: directEndpoints[0].httpBaseUrl,
        directEndpoints: [
          ...directEndpoints,
          { kind: "lan", httpBaseUrl: "http://127.0.0.1:3773" },
          { kind: "lan", httpBaseUrl: "http://localhost:3773" },
          { kind: "lan", httpBaseUrl: "http://[::1]:3773" },
          { kind: "lan", httpBaseUrl: "invalid" },
        ],
      }),
    ).toEqual([directEndpoints[0].httpBaseUrl]);
  });
});
