import { type EnvironmentConnectionPhase } from "@t3tools/client-runtime/connection";

export type ThreadContentPresentation =
  | { readonly kind: "ready" }
  | { readonly kind: "loading" }
  | {
      readonly kind: "unavailable";
      readonly title: string;
      readonly detail?: string;
    };

export function projectThreadContentPresentation(input: {
  readonly hasDetail: boolean;
  readonly detailError: string | null;
  readonly detailDeleted: boolean;
  readonly connectionState: EnvironmentConnectionPhase;
  readonly connectionError: string | null;
}): ThreadContentPresentation {
  if (input.hasDetail) {
    return { kind: "ready" };
  }
  if (input.detailDeleted) {
    return {
      kind: "unavailable",
      title: "Thread unavailable",
      detail: "This thread was deleted or is no longer available.",
    };
  }
  // An uncached thread has no conversation to render while its environment
  // reconnects. Keep the connection state in the feed as well as the floating pill.
  if (input.connectionState === "connecting" || input.connectionState === "reconnecting") {
    return input.connectionError !== null
      ? {
          kind: "unavailable",
          title: "Could not connect. Retrying...",
          detail: input.connectionError,
        }
      : {
          kind: "unavailable",
          title: input.connectionState === "connecting" ? "Connecting..." : "Reconnecting...",
        };
  }
  if (input.connectionState === "error" && input.connectionError !== null) {
    return {
      kind: "unavailable",
      title: "Could not connect",
      detail: input.connectionError,
    };
  }
  if (input.detailError !== null) {
    return {
      kind: "unavailable",
      title: "Could not load conversation",
      detail: input.detailError,
    };
  }
  if (input.connectionState === "connected") {
    return { kind: "loading" };
  }
  return {
    kind: "unavailable",
    title: "Messages not cached",
    detail: "Reconnect this environment to load the conversation.",
  };
}
