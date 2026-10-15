import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
const { test } = NodeTest;
import { WebTransport, quicheLoaded } from "@fails-components/webtransport";
import { runWebTransportProbe } from "./browserProbe.mjs";
import { startProtocolFixture } from "./fixture.mjs";

for (const hideOptional of [false, true])
  test(
    `native QUIC verifies pins, concurrent streams, resets and disconnects, optional APIs hidden=${hideOptional}`,
    { timeout: 30000 },
    async () => {
      await quicheLoaded;
      const fixture = await startProtocolFixture();
      try {
        const report = await runWebTransportProbe({
          ...fixture.config,
          // The fixture only listens over UDP. Prevent the Node client's internal HTTP/2 fallback.
          WebTransportConstructor: class extends WebTransport {
            constructor(url, options) {
              super(url, { ...options, requireUnreliable: true });
              if (hideOptional)
                return new Proxy(this, {
                  get(target, property) {
                    if (["reliability", "getStats"].includes(property)) return undefined;
                    const value = Reflect.get(target, property, target);
                    return typeof value === "function" ? value.bind(target) : value;
                  },
                });
            }
          },
        });
        console.log(JSON.stringify(report));
        NodeAssert.equal(report.ok, true, JSON.stringify(report.steps));
      } finally {
        await fixture.close();
      }
    },
  );
