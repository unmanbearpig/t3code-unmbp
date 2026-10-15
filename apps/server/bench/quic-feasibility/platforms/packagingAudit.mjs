// oxlint-disable t3code/no-global-process-runtime -- Standalone artifact audit measures the real host ABI and architecture.
import * as NodeChildProcess from "node:child_process";
const { execFileSync } = NodeChildProcess;
import * as NodeCrypto from "node:crypto";
const { createHash } = NodeCrypto;
import * as NodeFS from "node:fs";
const { existsSync, readFileSync, readdirSync, realpathSync } = NodeFS;
import * as NodePath from "node:path";
const { join, resolve } = NodePath;
import * as NodeURL from "node:url";
const { fileURLToPath } = NodeURL;
import {
  isRuntimeExternalCliDependency,
  selectCliRuntimeExternalDependencies,
} from "../../../../../scripts/lib/cli-external-packages.ts";
import { isDesktopRuntimeExternalDependency } from "../../../../../scripts/lib/desktop-external-packages.ts";

const root = fileURLToPath(new URL("../../../../../", import.meta.url));
const manifest = JSON.parse(readFileSync(join(root, "apps/server/package.json")));
const desktopManifest = JSON.parse(readFileSync(join(root, "apps/desktop/package.json")));
const packages = [
  "@fails-components/webtransport",
  "@fails-components/webtransport-transport-http3-quiche",
];
const targets = [
  "darwin-x64",
  "darwin-arm64",
  "linux-x64",
  "linux-arm64",
  "win32-x64",
  "win32-arm64",
];

function nativeFiles(directory) {
  const found = [];
  const visit = (path, depth) => {
    if (depth > 4 || !existsSync(path)) return;
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      const candidate = join(path, entry.name);
      if (entry.isDirectory() && entry.name !== "node_modules") visit(candidate, depth + 1);
      else if (entry.isFile() && entry.name.endsWith(".node")) found.push(candidate);
    }
  };
  visit(directory, 0);
  return found.map((path) => {
    const bytes = readFileSync(path);
    const little = bytes[5] === 1;
    const elfMachine = bytes.subarray(0, 4).equals(Buffer.from([127, 69, 76, 70]))
      ? little
        ? bytes.readUInt16LE(18)
        : bytes.readUInt16BE(18)
      : null;
    return {
      path,
      bytes: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      elfMachine,
      architecture: elfMachine === 62 ? "x64" : elfMachine === 183 ? "arm64" : null,
    };
  });
}

/** Audits actual release assets, dependency selection functions, and installed native payloads. */
export async function auditPackaging({ release, stageDirectory } = {}) {
  const stagedNow = selectCliRuntimeExternalDependencies(manifest.dependencies);
  const promoted = selectCliRuntimeExternalDependencies({
    ...manifest.dependencies,
    ...Object.fromEntries(packages.map((name) => [name, manifest.devDependencies[name]])),
  });
  const dependencies = packages.map((name) => {
    const directory = realpathSync(join(root, "apps/server/node_modules", name));
    const installedManifest = JSON.parse(readFileSync(join(directory, "package.json")));
    return {
      name,
      pinnedVersion: manifest.dependencies[name] ?? manifest.devDependencies[name] ?? null,
      declaration: name in manifest.dependencies ? "production" : "development",
      installedVersion: installedManifest.version,
      cliExternal: isRuntimeExternalCliDependency(name),
      desktopMainExternal: isDesktopRuntimeExternalDependency(name),
      stagedNow: name in stagedNow,
      stagedIfPromotedOnly: name in promoted,
      runtimeDependencies: installedManifest.dependencies,
      nativeFiles: nativeFiles(directory),
    };
  });
  const native = dependencies.flatMap((dependency) => dependency.nativeFiles);
  let localLoad;
  try {
    // Real dlopen in a subprocess. A missing or ABI-incompatible addon cannot pass this.
    const source =
      "const w = await import('@fails-components/webtransport'); await w.quicheLoaded; const q = await import('@fails-components/webtransport-transport-http3-quiche'); console.log(JSON.stringify({node:process.versions.node,napi:process.versions.napi,platform:process.platform,arch:process.arch,nativeServer:typeof q.Http3WebTransportServer}));";
    localLoad = {
      ok: true,
      ...JSON.parse(
        execFileSync(process.execPath, ["--input-type=module", "-e", source], {
          cwd: join(root, "apps/server"),
          timeout: 15000,
          encoding: "utf8",
          stdio: ["ignore", "pipe", "pipe"],
        }),
      ),
    };
  } catch (error) {
    localLoad = { ok: false, message: error.message, stderr: error.stderr?.toString() };
  }
  const releaseSource =
    release ??
    (await fetch(
      "https://api.github.com/repos/fails-components/webtransport/releases/tags/v1.6.9",
      {
        headers: { "User-Agent": "t3-quic-feasibility" },
        signal: AbortSignal.timeout(15000),
      },
    ).then((response) => {
      if (!response.ok) throw new Error(`Release metadata HTTP ${response.status}`);
      return response.json();
    }));
  const binaryVersion = dependencies[1].pinnedVersion;
  if (releaseSource.tag_name !== `v${binaryVersion}`)
    throw new Error("Release metadata differs from the pinned native package version");
  const assets = releaseSource.assets.map((asset) => asset.name);
  return {
    timestamp: new Date().toISOString(),
    host: {
      platform: process.platform,
      arch: process.arch,
      node: process.versions.node,
      napi: process.versions.napi,
    },
    electronVersion: desktopManifest.dependencies.electron,
    dependencies,
    localLoad,
    localElfDependencies:
      process.platform === "linux"
        ? native.map(({ path }) => ({
            path,
            readelf: execFileSync("readelf", ["-d", path], { encoding: "utf8", timeout: 10000 }),
            versionNeeds:
              execFileSync("readelf", ["--version-info", path], {
                encoding: "utf8",
                timeout: 10000,
              }).split("Version needs section")[1] ?? null,
          }))
        : [],
    release: { url: releaseSource.html_url, tag: releaseSource.tag_name, assets },
    targets: targets.map((target) => ({
      target,
      releaseAsset:
        assets.find(
          (name) =>
            name ===
            `webtransport-transport-http3-quiche-v${binaryVersion}-napi-v6-${target}.tar.gz`,
        ) ?? null,
      nativeLoadExecuted: target === `${process.platform}-${process.arch}` && localLoad.ok,
      packagedHostExecuted: false,
    })),
    ...(stageDirectory ? { stage: auditStage(stageDirectory) } : {}),
  };
}

function auditStage(directory) {
  const stage = resolve(directory);
  return {
    directory: stage,
    dependencies: packages.map((name) => {
      const path = join(stage, "node_modules", name);
      if (!existsSync(path)) return { name, present: false };
      const physicalPath = realpathSync(path);
      return {
        name,
        present: true,
        physicalPath,
        insideStage: physicalPath.startsWith(`${stage}/`) || physicalPath.startsWith(`${stage}\\`),
        nativeFiles: nativeFiles(physicalPath),
      };
    }),
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const option = (name) => (args.includes(name) ? args[args.indexOf(name) + 1] : undefined);
  const releasePath = option("--release");
  console.log(
    JSON.stringify(
      await auditPackaging({
        release: releasePath ? JSON.parse(readFileSync(releasePath)) : undefined,
        stageDirectory: option("--stage"),
      }),
      null,
      2,
    ),
  );
}
