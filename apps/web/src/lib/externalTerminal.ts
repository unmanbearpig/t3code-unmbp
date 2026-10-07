import type { ConnectionTarget } from "@t3tools/client-runtime/connection";
import type { ServerDirectEndpoint } from "@t3tools/contracts";

/** SSH tunnel URLs point at this computer even when their environment does not. */
export function externalTerminalCandidateUrls(input: {
  readonly target: ConnectionTarget | null;
  readonly httpBaseUrl: string | null;
  readonly directEndpoints: ReadonlyArray<ServerDirectEndpoint> | undefined;
}): ReadonlyArray<string> {
  const urls: string[] = [];
  if (input.target?._tag === "PrimaryConnectionTarget") {
    urls.push(input.target.httpBaseUrl);
  } else if (input.target?._tag === "BearerConnectionTarget" && input.httpBaseUrl !== null) {
    urls.push(input.httpBaseUrl);
  }
  for (const endpoint of input.directEndpoints ?? []) {
    try {
      const hostname = new URL(endpoint.httpBaseUrl).hostname;
      // Advertised loopback addresses identify every client's computer.
      if (hostname === "localhost" || hostname === "[::1]" || hostname.startsWith("127.")) continue;
      urls.push(endpoint.httpBaseUrl);
    } catch {
      // Ignore an invalid hint without disabling the active direct connection.
    }
  }
  return [...new Set(urls)];
}
