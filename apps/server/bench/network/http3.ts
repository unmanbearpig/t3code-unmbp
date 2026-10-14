// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Native benchmark adapter cleanup must run outside libcurl callbacks.
import * as Effect from "effect/Effect";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/http";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";

import type { TlsMaterial } from "./client.ts";
import type { Recorder } from "./workloads.ts";

export type SnapshotHttpProtocol = "h1" | "h3";
export const snapshotPort = 4407;

/** Loads the optional native benchmark dependency only when HTTP snapshots are benchmarked. */
export async function http3Environment() {
  const { Curl } = await import("node-libcurl");
  if (!(Curl.getVersionInfo().rawFeatures & (1 << 25)))
    throw new Error(
      "The benchmark needs libcurl with HTTP3. Rebuild node-libcurl from source against an HTTP3-enabled curl-config.",
    );
  return { libcurl: Curl.getVersion() };
}

/** One connection pool per logical client, shared by its snapshot requests and never by cases. */
export const makeSnapshotHttpClient = Effect.fnUntraced(function* (
  protocol: SnapshotHttpProtocol,
  tls: TlsMaterial,
  record: Recorder,
) {
  const { Curl, CurlFeature, CurlHttpVersion, Multi } = yield* Effect.promise(
    () => import("node-libcurl"),
  );
  const pool = yield* Effect.acquireRelease(
    Effect.sync(() => new Multi()),
    (pool) => Effect.sync(() => pool.close()),
  );
  const failure = (request: HttpClientRequest.HttpClientRequest, cause: unknown) =>
    new HttpClientError.HttpClientError({
      reason: new HttpClientError.TransportError({ request, cause }),
    });

  return HttpClient.make((request, url) =>
    Effect.scoped(
      Effect.gen(function* () {
        const curl = yield* Effect.acquireRelease(
          Effect.sync(() => new Curl()),
          (curl) =>
            Effect.promise(
              () =>
                new Promise<void>((resolve) =>
                  setImmediate(() => {
                    // Curl.close() assumes its global pool even after setMulti().
                    if (curl.handle.isInsideMultiHandle) pool.removeHandle(curl.handle);
                    curl.close();
                    resolve();
                  }),
                ),
            ),
        );
        yield* Effect.try({
          try: () => {
            curl.setMulti(pool);
            curl.enable(CurlFeature.NoDataParsing);
            curl.setOpt("URL", url.href);
            curl.setOpt("CUSTOMREQUEST", request.method);
            curl.setOpt(
              "HTTP_VERSION",
              protocol === "h3" ? CurlHttpVersion.V3Only : CurlHttpVersion.V1_1,
            );
            curl.setOpt("CAINFO_BLOB", tls.certPem);
            curl.setOpt("PROXY", "");
            curl.setOpt("FOLLOWLOCATION", false);
            curl.setOpt("ACCEPT_ENCODING", "gzip");
            curl.setOpt(
              "HTTPHEADER",
              Object.entries(request.headers).map(([name, value]) => name + ": " + value),
            );
          },
          catch: (cause) => failure(request, cause),
        });
        return yield* Effect.callback<
          HttpClientResponse.HttpClientResponse,
          HttpClientError.HttpClientError
        >((resume) => {
          curl.once("error", (cause: Error) => resume(Effect.fail(failure(request, cause))));
          curl.once(
            "end",
            (status: number, body: Buffer, blocks: ReadonlyArray<Record<string, unknown>>) => {
              const result = Effect.try({
                try: () => {
                  const version = Number(curl.getInfo("HTTP_VERSION"));
                  if (version !== (protocol === "h3" ? 30 : 2))
                    throw new Error("HTTP protocol fallback in snapshot benchmark");
                  const headers = new Headers();
                  for (const [name, value] of Object.entries(blocks.at(-1) ?? {}))
                    if (typeof value === "string") headers.set(name, value);
                  if (
                    headers.get("x-netbench-protocol") !==
                    (protocol === "h3" ? "HTTP/3.0" : "HTTP/1.1")
                  )
                    throw new Error("Snapshot server did not observe the required HTTP protocol");
                  record.set("httpProtocol", protocol);
                  record.set("httpNewConnections", Number(curl.getInfo("NUM_CONNECTS")));
                  record.set("httpConnectionId", Number(curl.getInfo("CONN_ID")));
                  record.set("httpConnectMs", Number(curl.getInfo("CONNECT_TIME_T")) / 1000);
                  record.set("httpTlsMs", Number(curl.getInfo("APPCONNECT_TIME_T")) / 1000);
                  record.set(
                    "httpFirstByteMs",
                    Number(curl.getInfo("STARTTRANSFER_TIME_T")) / 1000,
                  );
                  record.set("httpTotalMs", Number(curl.getInfo("TOTAL_TIME_T")) / 1000);
                  return HttpClientResponse.fromWeb(
                    request,
                    new Response(new Uint8Array(body), { status, headers }),
                  );
                },
                catch: (cause) => failure(request, cause),
              });
              resume(result);
            },
          );
          curl.perform();
        });
      }),
    ),
  );
});
