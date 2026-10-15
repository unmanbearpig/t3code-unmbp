/** Runs unchanged in browsers, Electron renderers, and the isolated Node fixture. */
export async function runWebTransportProbe({
  url,
  certificateHash,
  timeoutMs = 8000,
  WebTransportConstructor = globalThis.WebTransport,
}) {
  const report = {
    timestamp: new Date().toISOString(),
    origin: globalThis.location?.origin ?? null,
    userAgent: globalThis.navigator?.userAgent ?? null,
    secureContext: globalThis.isSecureContext ?? null,
    url,
    certificateMode: certificateHash ? "sha256-pin" : "system-trust",
    capabilities: { constructor: typeof WebTransportConstructor === "function" },
    steps: [],
  };
  let transport;
  let sessionClosed = false;
  let stage = "capability";
  const bounded = async (promise) => {
    let timer;
    try {
      return await Promise.race([
        promise,
        new Promise((_, reject) => {
          timer = setTimeout(() => {
            const error = new Error(`${stage} exceeded ${timeoutMs}ms`);
            error.name = "ProbeTimeout";
            reject(error);
          }, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  const step = async (name, operation) => {
    stage = name;
    const start = performance.now();
    const details = await bounded(operation());
    report.steps.push({ name, ok: true, elapsedMs: performance.now() - start, ...details });
  };
  const options = certificateHash
    ? {
        serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(certificateHash) }],
      }
    : {};
  const exchange = async (payload) => {
    const stream = await transport.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    // Read while writing to avoid depending on a peer's initial flow-control window.
    const receiving = (async () => {
      const chunks = [];
      let length = 0;
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        chunks.push(next.value);
        length += next.value.byteLength;
        if (length > payload.byteLength) throw new Error("Echo exceeded its expected size");
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      if (length !== payload.byteLength || bytes.some((byte, i) => byte !== payload[i])) {
        throw new Error("Echo content differs");
      }
    })();
    receiving.catch(() => {});
    try {
      await writer.write(new Uint8Array([0]));
      await writer.write(payload);
      await writer.close();
      await receiving;
      await writer.closed;
      await reader.closed;
    } finally {
      writer.releaseLock();
      reader.releaseLock();
    }
  };
  try {
    if (typeof WebTransportConstructor !== "function")
      throw new Error("WebTransport is unavailable");
    if (report.secureContext === false) throw new Error("WebTransport requires a secure context");
    if (certificateHash) {
      await step("reject-wrong-certificate", async () => {
        const wrongHash = new Uint8Array(certificateHash);
        wrongHash[0] ^= 255;
        const invalid = new WebTransportConstructor(url, {
          serverCertificateHashes: [{ algorithm: "sha-256", value: wrongHash }],
        });
        invalid.closed.catch(() => {});
        try {
          let rejected = false;
          try {
            await bounded(invalid.ready);
          } catch (error) {
            // A silent timeout does not establish that certificate validation worked.
            if (error.name === "ProbeTimeout") throw error;
            rejected = true;
          }
          if (!rejected) throw new Error("Wrong certificate pin was accepted");
          return { rejected: true };
        } finally {
          invalid.close();
        }
      });
    }
    transport = new WebTransportConstructor(url, options);
    transport.closed.then(
      () => {
        sessionClosed = true;
      },
      () => {
        sessionClosed = true;
      },
    );
    report.capabilities = {
      ...report.capabilities,
      createBidirectionalStream: typeof transport.createBidirectionalStream === "function",
      reliability: transport.reliability ?? null,
      getStats: typeof transport.getStats === "function",
      datagrams: "datagrams" in transport,
    };
    // Optional API members and requireUnreliable are deliberately not capability gates.
    await step("ready", () => transport.ready);
    report.capabilities.reliability = transport.reliability ?? null;
    await step("client-created-stream-write-read-finish", () =>
      exchange(new TextEncoder().encode("T3 QUIC probe: Unicode 🌍 and stream FIN")),
    );
    await step("four-simultaneous-streams", async () => {
      await Promise.all(
        Array.from({ length: 4 }, (_, stream) =>
          exchange(Uint8Array.from({ length: 256 * 1024 }, (_, i) => (i + stream) % 251)),
        ),
      );
      return { streams: 4, bytes: 4 * 256 * 1024 };
    });
    await step("client-cancel-preserves-session", async () => {
      const stream = await transport.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      writer.closed.catch(() => {});
      reader.closed.catch(() => {});
      await writer.write(new Uint8Array([0, 7]));
      await Promise.all([writer.abort("probe cancel"), reader.cancel("probe cancel")]);
      writer.releaseLock();
      reader.releaseLock();
      await exchange(new Uint8Array([91, 92, 93]));
    });
    await step("server-reset-preserves-session", async () => {
      const stream = await transport.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      writer.closed.catch(() => {});
      reader.closed.catch(() => {});
      await writer.write(new Uint8Array([1]));
      let rejection;
      try {
        for (;;) {
          if ((await reader.read()).done) break;
        }
      } catch (error) {
        rejection = error;
      } finally {
        await Promise.allSettled([writer.abort(), reader.cancel()]);
        writer.releaseLock();
        reader.releaseLock();
      }
      if (!rejection) throw new Error("Server reset did not reject the stream read");
      await exchange(new Uint8Array([94, 95, 96]));
      return { error: classifyProbeError(rejection, "stream") };
    });
    await step("server-session-disconnect", async () => {
      const stream = await transport.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      writer.closed.catch(() => {});
      await writer.write(new Uint8Array([2]));
      const info = await transport.closed;
      if (info?.closeCode !== 23) throw new Error("Unexpected disconnect close code");
      await Promise.allSettled([writer.abort(), stream.readable.cancel()]);
      writer.releaseLock();
      return { closeCode: info.closeCode, reason: info.reason };
    });
    report.ok = true;
  } catch (error) {
    report.ok = false;
    report.steps.push({ name: stage, ok: false, error: classifyProbeError(error, stage) });
  } finally {
    if (transport && !sessionClosed) transport.close();
  }
  return report;
}

export function classifyProbeError(error, stage) {
  return {
    stage,
    name: error?.name ?? typeof error,
    message: error?.message ?? String(error),
    source: error?.source ?? null,
    streamErrorCode: error?.streamErrorCode ?? null,
    classification:
      error?.name === "ProbeTimeout"
        ? "timeout"
        : error?.source === "stream" || stage === "stream"
          ? "stream"
          : error?.source === "session"
            ? "session"
            : stage === "ready" || stage === "reject-wrong-certificate"
              ? "handshake-or-certificate"
              : "operation",
  };
}
