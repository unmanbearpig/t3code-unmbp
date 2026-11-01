#!/usr/bin/env node

// This bootstrap runs before workspace dependencies are available.
/* oxlint-disable t3code/no-global-process-runtime */
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeTimersPromises from "node:timers/promises";

const rustVersion = "1.96.1";
const rustupVersion = "1.28.2";
const moduleRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const buildRoot = NodePath.join(moduleRoot, "build");
const cargoHome = process.env.CARGO_HOME || NodePath.join(NodeOS.homedir(), ".cargo");
const cacheRoot = NodePath.join(
  process.env.XDG_CACHE_HOME || NodePath.join(NodeOS.homedir(), ".cache"),
  "t3-quic",
);
const rustEnv = {
  ...process.env,
  PATH: `${NodePath.join(cargoHome, "bin")}${NodePath.delimiter}${process.env.PATH || ""}`,
};
const androidTargets = {
  "arm64-v8a": { rust: "aarch64-linux-android", clang: "aarch64-linux-android" },
  x86_64: { rust: "x86_64-linux-android", clang: "x86_64-linux-android" },
};
const iosTargets = new Set(["aarch64-apple-ios", "aarch64-apple-ios-sim", "x86_64-apple-ios"]);

function run(command, args, options = {}) {
  const result = NodeChildProcess.spawnSync(command, args, {
    stdio: "inherit",
    env: rustEnv,
    ...options,
  });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(`${command} failed with ${result.signal || result.status}`);
}

function capture(command, args, options = {}) {
  const result = NodeChildProcess.spawnSync(command, args, {
    encoding: "utf8",
    timeout: 30_000,
    env: rustEnv,
    ...options,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr.trim()}`);
  return result.stdout.trim();
}

async function exists(path) {
  try {
    await NodeFSP.stat(path);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
}

async function withLock(path, body) {
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  const started = Date.now();
  for (;;) {
    if (Date.now() - started > 20 * 60_000) throw new Error(`Timed out waiting for ${path}`);
    try {
      await NodeFSP.mkdir(path);
      await NodeFSP.writeFile(
        NodePath.join(path, "owner.json"),
        JSON.stringify({ pid: process.pid }),
      );
      break;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      if (await staleLock(path)) {
        const reaper = NodePath.join(path, "reaper");
        try {
          await NodeFSP.writeFile(reaper, "", { flag: "wx" });
        } catch (error) {
          if (error.code === "ENOENT" || error.code === "EEXIST") {
            await NodeTimersPromises.setTimeout(500);
            continue;
          }
          throw error;
        }
        try {
          // Recheck after claiming cleanup so another waiter cannot remove a new owner.
          if (await staleLock(path)) await NodeFSP.rm(path, { recursive: true, force: true });
        } finally {
          await NodeFSP.rm(reaper, { force: true });
        }
        continue;
      }
      await NodeTimersPromises.setTimeout(500);
    }
  }
  try {
    return await body();
  } finally {
    await NodeFSP.rm(path, { recursive: true, force: true });
  }
}

async function staleLock(path) {
  try {
    const { pid } = JSON.parse(await NodeFSP.readFile(NodePath.join(path, "owner.json"), "utf8"));
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error(`Invalid lock owner at ${path}`);
    try {
      process.kill(pid, 0);
      return false;
    } catch (error) {
      if (error.code === "ESRCH") return true;
      if (error.code === "EPERM") return false;
      throw error;
    }
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    // Allow the lock holder to write its PID before treating an empty lock as stale.
    try {
      return Date.now() - (await NodeFSP.stat(path)).mtimeMs > 30_000;
    } catch (error) {
      if (error.code === "ENOENT") return false;
      throw error;
    }
  }
}

async function download(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`Download failed with HTTP ${response.status}: ${url}`);
  return Buffer.from(await response.arrayBuffer());
}

async function provisionRust(targets) {
  await withLock(NodePath.join(cacheRoot, `rustup-${rustVersion}.lock`), async () => {
    const available = NodeChildProcess.spawnSync("rustup", ["--version"], {
      env: rustEnv,
      stdio: "ignore",
      timeout: 30_000,
    });
    if (available.error?.code === "ENOENT") {
      const hosts = {
        "linux-x64": "x86_64-unknown-linux-gnu",
        "linux-arm64": "aarch64-unknown-linux-gnu",
        "darwin-x64": "x86_64-apple-darwin",
        "darwin-arm64": "aarch64-apple-darwin",
      };
      const host = hosts[`${NodeOS.platform()}-${NodeOS.arch()}`];
      if (!host)
        throw new Error(
          `Rust provisioning is unsupported on ${NodeOS.platform()}-${NodeOS.arch()}`,
        );
      const url = `https://static.rust-lang.org/rustup/archive/${rustupVersion}/${host}/rustup-init`;
      const [binary, checksum] = await Promise.all([download(url), download(`${url}.sha256`)]);
      const expected = checksum.toString("utf8").trim().split(/\s+/)[0];
      if (
        !/^[a-f0-9]{64}$/i.test(expected) ||
        NodeCrypto.createHash("sha256").update(binary).digest("hex") !== expected
      ) {
        throw new Error("The rustup installer checksum does not match");
      }
      const installer = NodePath.join(cacheRoot, `rustup-init-${process.pid}`);
      try {
        await NodeFSP.writeFile(installer, binary);
        await NodeFSP.chmod(installer, 0o700);
        run(
          installer,
          ["-y", "--profile", "minimal", "--default-toolchain", "none", "--no-modify-path"],
          { timeout: 180_000 },
        );
      } finally {
        await NodeFSP.rm(installer, { force: true });
      }
    } else if (available.error || available.status !== 0) {
      throw available.error || new Error("rustup --version failed");
    }
    const installed = capture("rustup", ["toolchain", "list"]);
    if (!installed.split("\n").some((line) => line.startsWith(`${rustVersion}-`))) {
      run(
        "rustup",
        ["toolchain", "install", rustVersion, "--profile", "minimal", "--no-self-update"],
        { timeout: 15 * 60_000 },
      );
    }
    const installedTargets = new Set(
      capture("rustup", ["target", "list", "--installed", "--toolchain", rustVersion]).split("\n"),
    );
    const missing = targets.filter((target) => !installedTargets.has(target));
    if (missing.length) {
      run("rustup", ["target", "add", "--toolchain", rustVersion, ...missing], {
        timeout: 15 * 60_000,
      });
    }
  });
}

function build(target, profile, env = {}) {
  run(
    "cargo",
    [
      `+${rustVersion}`,
      "build",
      "--locked",
      "--manifest-path",
      NodePath.join(moduleRoot, "rust", "Cargo.toml"),
      "--target-dir",
      NodePath.join(buildRoot, "rust"),
      "--target",
      target,
      ...(profile === "release" ? ["--release"] : []),
    ],
    { env: { ...rustEnv, ...env } },
  );
  return NodePath.join(buildRoot, "rust", target, profile);
}

async function copyArtifact(source, destination) {
  await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
  const temporary = `${destination}.${process.pid}.tmp`;
  try {
    await NodeFSP.copyFile(source, temporary);
    await NodeFSP.rename(temporary, destination);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}

async function findNdk(options) {
  const direct = options.ndk || process.env.ANDROID_NDK_HOME || process.env.ANDROID_NDK_ROOT;
  if (direct) return NodePath.resolve(direct);
  const sdk =
    process.env.ANDROID_HOME ||
    process.env.ANDROID_SDK_ROOT ||
    NodePath.join(NodeOS.homedir(), "Android", "Sdk");
  const version = options["ndk-version"] || process.env.ANDROID_NDK_VERSION;
  if (version) return NodePath.join(sdk, "ndk", version);
  const versions = (await NodeFSP.readdir(NodePath.join(sdk, "ndk"))).sort((a, b) =>
    b.localeCompare(a, undefined, { numeric: true }),
  );
  if (!versions.length) throw new Error("Install the Android NDK or set ANDROID_NDK_HOME");
  return NodePath.join(sdk, "ndk", versions[0]);
}

async function buildAndroid(options) {
  const abis = [...new Set((options.abis || "arm64-v8a,x86_64").split(","))];
  if (abis.some((abi) => !androidTargets[abi]))
    throw new Error("QUIC Android builds support arm64-v8a and x86_64 only");
  const ndk = await findNdk(options);
  const host =
    NodeOS.platform() === "darwin"
      ? "darwin-x86_64"
      : NodeOS.platform() === "linux"
        ? "linux-x86_64"
        : undefined;
  if (!host) throw new Error("QUIC Android builds require macOS or Linux");
  const bin = NodePath.join(ndk, "toolchains", "llvm", "prebuilt", host, "bin");
  if (!(await exists(bin))) throw new Error(`The Android NDK toolchain is missing at ${bin}`);
  await provisionRust(abis.map((abi) => androidTargets[abi].rust));
  const output = NodePath.resolve(options.output || NodePath.join(buildRoot, "android", "jniLibs"));
  for (const abi of abis) {
    const target = androidTargets[abi];
    const compiler = NodePath.join(bin, `${target.clang}24-clang`);
    if (!(await exists(compiler)))
      throw new Error(`The Android API 24 compiler is missing at ${compiler}`);
    const envTarget = target.rust.replaceAll("-", "_");
    const archive = NodePath.join(bin, "llvm-ar");
    const artifacts = build(target.rust, "release", {
      [`CARGO_TARGET_${envTarget.toUpperCase()}_LINKER`]: compiler,
      [`CC_${envTarget}`]: compiler,
      [`AR_${envTarget}`]: archive,
      ...(process.env.CARGO_ENCODED_RUSTFLAGS !== undefined
        ? {
            CARGO_ENCODED_RUSTFLAGS: `${process.env.CARGO_ENCODED_RUSTFLAGS}\x1f-C\x1flink-arg=-Wl,-z,max-page-size=16384`,
          }
        : { RUSTFLAGS: `${process.env.RUSTFLAGS || ""} -C link-arg=-Wl,-z,max-page-size=16384` }),
    });
    await copyArtifact(
      NodePath.join(artifacts, "libt3_quic.so"),
      NodePath.join(output, abi, "libt3_quic.so"),
    );
  }
}

async function buildIos(options) {
  if (NodeOS.platform() !== "darwin") throw new Error("QUIC iOS builds require macOS and Xcode");
  const explicit = options.target;
  const sdk =
    process.env.PLATFORM_NAME ||
    (explicit === "aarch64-apple-ios" ? "iphoneos" : "iphonesimulator");
  if (sdk !== "iphoneos" && sdk !== "iphonesimulator")
    throw new Error(`Unsupported iOS platform ${sdk}`);
  const architectures = (process.env.ARCHS || "arm64").trim().split(/\s+/);
  const targets = [
    ...new Set(
      explicit
        ? [explicit]
        : architectures.map((architecture) => {
            if (architecture === "arm64")
              return sdk === "iphoneos" ? "aarch64-apple-ios" : "aarch64-apple-ios-sim";
            if (architecture === "x86_64" && sdk === "iphonesimulator") return "x86_64-apple-ios";
            throw new Error(`Unsupported iOS architecture ${architecture} for ${sdk}`);
          }),
    ),
  ];
  if (targets.some((target) => !iosTargets.has(target)))
    throw new Error("Unsupported iOS Rust target");
  await provisionRust(targets);
  const profile = process.env.CONFIGURATION === "Debug" ? "debug" : "release";
  const archives = targets.map((target) => NodePath.join(build(target, profile), "libt3_quic.a"));
  const output = NodePath.resolve(
    options.output || NodePath.join(buildRoot, "ios", sdk, "libt3_quic.a"),
  );
  if (archives.length === 1) {
    await copyArtifact(archives[0], output);
  } else {
    await NodeFSP.mkdir(NodePath.dirname(output), { recursive: true });
    const temporary = `${output}.${process.pid}.tmp`;
    try {
      run("xcrun", ["lipo", "-create", ...archives, "-output", temporary]);
      await NodeFSP.rename(temporary, output);
    } finally {
      await NodeFSP.rm(temporary, { force: true });
    }
  }
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  if (!command || command === "--help") {
    console.log(
      "Usage: prepare-native.mjs prepare\n       prepare-native.mjs android [--abis arm64-v8a,x86_64] [--output jniLibs] [--ndk path] [--ndk-version version]\n       prepare-native.mjs ios [--target Rust-target] [--output libt3_quic.a]",
    );
    return;
  }
  if (command === "prepare") {
    if (args.length) throw new Error("The prepare command does not accept arguments");
    const targetPlatform =
      process.env.EAS_BUILD_PLATFORM || (NodeOS.platform() === "darwin" ? "ios" : "android");
    if (targetPlatform !== "android" && targetPlatform !== "ios")
      throw new Error(`Unsupported EAS platform ${targetPlatform}`);
    await provisionRust(
      targetPlatform === "android"
        ? Object.values(androidTargets).map((target) => target.rust)
        : [...iosTargets],
    );
    return;
  }
  if (command !== "android" && command !== "ios")
    throw new Error(`Unknown native build platform ${command}`);
  const options = {};
  for (let index = 0; index < args.length; index += 2) {
    const option = args[index];
    const value = args[index + 1];
    const accepted =
      command === "android"
        ? ["--abis", "--output", "--ndk", "--ndk-version"]
        : ["--target", "--output"];
    if (!accepted.includes(option) || !value || value.startsWith("--"))
      throw new Error(`Invalid native build option ${option}`);
    options[option.slice(2)] = value;
  }
  await withLock(NodePath.join(buildRoot, ".native-build.lock"), () =>
    command === "android" ? buildAndroid(options) : buildIos(options),
  );
}

main().catch((error) => {
  console.error(`[t3-quic] ${error.message}`);
  process.exitCode = 1;
});
