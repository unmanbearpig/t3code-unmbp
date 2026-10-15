// Run only after browser/computer-use authorization. This starts a fresh Electron app,
// not a packaged T3 install. The caller keeps the loopback fixture running separately.
const { app, BrowserWindow, net, protocol } = require("electron");
const { mkdtempSync, mkdirSync, rmSync } = require("node:fs");
const { homedir } = require("node:os");
const { join } = require("node:path");

// Electron retains Chromium switches in argv, so locate the fixture URL explicitly.
const pageArgumentIndex = process.argv.findIndex((argument) => argument.startsWith("http://"));
const pageUrl = pageArgumentIndex < 0 ? undefined : process.argv[pageArgumentIndex];
const scheme =
  pageArgumentIndex < 0 ? undefined : (process.argv[pageArgumentIndex + 1] ?? "t3code");
if (!pageUrl || !["t3code", "t3code-dev"].includes(scheme)) {
  throw new Error("Usage: electron electronProbe.cjs <fixture-page-url> [t3code|t3code-dev]");
}
const destination = new URL(pageUrl);
if (
  destination.protocol !== "http:" ||
  !["localhost", "127.0.0.1"].includes(destination.hostname)
) {
  throw new Error("Electron fixture must use a loopback HTTP origin");
}
const temporaryRoot = join(homedir(), "codex-tmp");
mkdirSync(temporaryRoot, { recursive: true });
const userData = mkdtempSync(join(temporaryRoot, "quic-electron-"));
app.setPath("userData", userData);
// Match the production ElectronProtocol.ts privilege set. Never bypass TLS errors.
protocol.registerSchemesAsPrivileged([
  {
    scheme,
    privileges: {
      standard: true,
      secure: true,
      supportFetchAPI: true,
      corsEnabled: true,
      stream: true,
      ...(scheme === "t3code" ? { codeCache: true } : {}),
    },
  },
]);
app.on("quit", () => rmSync(userData, { recursive: true, force: true }));
app
  .whenReady()
  .then(async () => {
    protocol.handle(scheme, (request) => {
      const original = new URL(request.url);
      if (original.host !== "app") return new Response(null, { status: 404 });
      return net.fetch(new URL(original.pathname + original.search, destination).href, {
        method: request.method,
        ...(request.method === "POST" ? { body: request.body, duplex: "half" } : {}),
      });
    });
    const window = new BrowserWindow({
      width: 950,
      height: 750,
      webPreferences: { sandbox: true, contextIsolation: true, nodeIntegration: false },
    });
    await window.loadURL(`${scheme}://app/`);
    const report = await window.webContents.executeJavaScript(
      "document.querySelector('#run').click(); globalThis.quicProbeFinished",
    );
    console.log(
      JSON.stringify({
        kind: "standalone-electron-custom-origin",
        versions: process.versions,
        packagedT3Checked: false,
        ...report,
      }),
    );
    process.exitCode = report.ok ? 0 : 1;
    app.quit();
  })
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
    app.quit();
  });
