// Prints an expression for the current packaged T3 renderer's DevTools or preview_evaluate.
// It keeps the real origin and CSP. Run the loopback fixture and pass its emitted config.
import * as NodeFS from "node:fs";
const { readFileSync } = NodeFS;

const path = process.argv[2];
if (!path) throw new Error("Usage: node rendererSnippet.mjs <fixture-config.json>");
const config = JSON.parse(readFileSync(path, "utf8"));
if (new URL(config.url).protocol !== "https:")
  throw new Error("Expected an HTTPS WebTransport URL");
const source = readFileSync(new URL("browserProbe.mjs", import.meta.url), "utf8").replaceAll(
  "export ",
  "",
);
console.log(
  `(async () => {\n${source}\nreturn await runWebTransportProbe(${JSON.stringify(config)});\n})()`,
);
