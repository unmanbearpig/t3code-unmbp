import { describe, expect, it } from "@effect/vitest";

import { projectThreadContentPresentation } from "./threadContentPresentation";

describe("thread content presentation", () => {
  it("renders cached detail while its environment reconnects", () => {
    expect(
      projectThreadContentPresentation({
        hasDetail: true,
        detailError: null,
        detailDeleted: false,
        connectionState: "reconnecting",
        connectionError: "The environment did not respond.",
      }),
    ).toEqual({ kind: "ready" });
  });

  it("explains uncached detail while disconnected instead of loading forever", () => {
    expect(
      projectThreadContentPresentation({
        hasDetail: false,
        detailError: null,
        detailDeleted: false,
        connectionState: "error",
        connectionError: null,
      }),
    ).toEqual({
      kind: "unavailable",
      title: "Messages not cached",
      detail: "Reconnect this environment to load the conversation.",
    });
  });

  it.each([
    ["connecting", "Connecting..."],
    ["reconnecting", "Reconnecting..."],
  ] as const)("shows connection progress in an uncached conversation while %s", (phase, title) => {
    expect(
      projectThreadContentPresentation({
        hasDetail: false,
        detailError: null,
        detailDeleted: false,
        connectionState: phase,
        connectionError: null,
      }),
    ).toEqual({ kind: "unavailable", title });
  });

  it("shows an unreachable environment's failure while it keeps retrying", () => {
    expect(
      projectThreadContentPresentation({
        hasDetail: false,
        detailError: null,
        detailDeleted: false,
        connectionState: "reconnecting",
        connectionError: "laptop did not respond during connection setup.",
      }),
    ).toEqual({
      kind: "unavailable",
      title: "Could not connect. Retrying...",
      detail: "laptop did not respond during connection setup.",
    });
  });

  it("surfaces a connection failure instead of a generic cache miss", () => {
    expect(
      projectThreadContentPresentation({
        hasDetail: false,
        detailError: "Could not synchronize the thread.",
        detailDeleted: false,
        connectionState: "error",
        connectionError: "The environment rejected this connection.",
      }),
    ).toEqual({
      kind: "unavailable",
      title: "Could not connect",
      detail: "The environment rejected this connection.",
    });
  });

  it("returns to loading messages when the environment reconnects", () => {
    const input = {
      hasDetail: false,
      detailError: null,
      detailDeleted: false,
      connectionError: "laptop did not respond during connection setup.",
    };
    expect(
      projectThreadContentPresentation({ ...input, connectionState: "reconnecting" }).kind,
    ).toBe("unavailable");
    expect(
      projectThreadContentPresentation({
        ...input,
        connectionState: "connected",
        connectionError: null,
      }),
    ).toEqual({ kind: "loading" });
    expect(
      projectThreadContentPresentation({
        ...input,
        hasDetail: true,
        connectionState: "connected",
        connectionError: null,
      }),
    ).toEqual({ kind: "ready" });
  });
});
