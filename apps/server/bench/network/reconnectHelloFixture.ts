import type { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Persistence from "../../../../packages/client-runtime/src/platform/persistence.ts";
import { operationServerConfig } from "./operationsFixture.ts";

/** The persisted discovery hint from a prior session; no HTTP/TLS pool or authorization service is warmed. */
export function makeReconnectHelloSupportCache(origin: string, routes: 1 | 2, cached: boolean) {
  const keys = new Set<string>();
  const key = (id: EnvironmentId, http: string, ws: string) =>
    `${id.length}:${id}${Persistence.helloSupportEndpointKey(http, ws)}`;
  const has = (id: EnvironmentId, http: string, ws: string) => keys.has(key(id, http, ws));
  const environmentId = operationServerConfig.environment.environmentId;
  if (cached)
    for (const index of routes === 2 ? [0, 1] : [0]) {
      const http = index === 0 ? origin : origin.replace("127.0.0.1", "127.0.0.2");
      keys.add(key(environmentId, http, http.replace(/^http/, "ws")));
    }
  const store: NonNullable<Persistence.EnvironmentCacheStore["Service"]["helloSupport"]> = {
    load: (id, http, ws) => Effect.sync(() => has(id, http, ws)),
    save: (id, http, ws) =>
      Effect.sync(() => {
        keys.add(key(id, http, ws));
      }),
    remove: (id, http, ws) =>
      Effect.sync(() => {
        keys.delete(key(id, http, ws));
      }),
  };
  return { store, has };
}
