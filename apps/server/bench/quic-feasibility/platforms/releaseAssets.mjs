import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeZlib from "node:zlib";

/** Inspect downloaded native binaries without executing foreign code or extracting tar paths. */
function binaryTarget(bytes) {
  if (bytes.subarray(0, 4).equals(Buffer.from([127, 69, 76, 70]))) {
    const machine = bytes[5] === 1 ? bytes.readUInt16LE(18) : bytes.readUInt16BE(18);
    const glibcVersionStrings = [
      ...new Set(bytes.toString("latin1").match(/GLIBC_\d+\.\d+(?:\.\d+)?/g) ?? []),
    ].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    return {
      format: "ELF",
      arch: machine === 62 ? "x64" : machine === 183 ? "arm64" : "unknown",
      glibcVersionStrings,
    };
  }
  if (bytes.readUInt32LE(0) === 0xfeedfacf) {
    const cpu = bytes.readUInt32LE(4);
    return {
      format: "Mach-O",
      arch: cpu === 0x01000007 ? "x64" : cpu === 0x0100000c ? "arm64" : "unknown",
    };
  }
  if (bytes.subarray(0, 2).toString() === "MZ") {
    const offset = bytes.readUInt32LE(60);
    if (bytes.subarray(offset, offset + 4).toString() !== "PE\u0000\u0000")
      throw new Error("Bad PE signature");
    const machine = bytes.readUInt16LE(offset + 4);
    return {
      format: "PE",
      arch: machine === 0x8664 ? "x64" : machine === 0xaa64 ? "arm64" : "unknown",
    };
  }
  throw new Error("Unknown native binary format");
}

function tarNativeEntries(tar) {
  const entries = [];
  for (let offset = 0; offset + 512 <= tar.byteLength;) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;
    const name = header.subarray(0, 100).toString().split("\u0000")[0];
    const size = Number.parseInt(
      header.subarray(124, 136).toString().replaceAll("\u0000", "").trim(),
      8,
    );
    if (!Number.isSafeInteger(size) || size < 0 || offset + 512 + size > tar.byteLength)
      throw new Error("Bad tar size");
    const bytes = tar.subarray(offset + 512, offset + 512 + size);
    if (name.endsWith(".node"))
      entries.push({
        name,
        bytes: size,
        sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
        ...binaryTarget(bytes),
      });
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  return entries;
}

const releasePath = process.argv[2];
if (!releasePath) throw new Error("Usage: node releaseAssets.mjs <github-release-metadata.json>");
const release = JSON.parse(NodeFS.readFileSync(releasePath, "utf8"));
if (release.tag_name !== "v1.6.9") throw new Error("Expected pinned v1.6.9 release metadata");
const results = await Promise.all(
  release.assets.map(async (asset) => {
    const response = await fetch(asset.browser_download_url, {
      signal: AbortSignal.timeout(30000),
    });
    if (!response.ok) throw new Error(`${asset.name}: HTTP ${response.status}`);
    const compressed = Buffer.from(await response.arrayBuffer());
    if (compressed.byteLength !== asset.size)
      throw new Error(`${asset.name}: download size mismatch`);
    const native = tarNativeEntries(
      NodeZlib.gunzipSync(compressed, { maxOutputLength: 64 * 1024 * 1024 }),
    );
    if (native.length === 0) throw new Error(`${asset.name}: missing native binary`);
    const expectedArch = asset.name.includes("arm64") ? "arm64" : "x64";
    if (native.some((entry) => entry.arch !== expectedArch))
      throw new Error(`${asset.name}: CPU mismatch`);
    return {
      name: asset.name,
      compressedBytes: compressed.byteLength,
      compressedSha256: NodeCrypto.createHash("sha256").update(compressed).digest("hex"),
      native,
      executed: false,
    };
  }),
);
console.log(
  JSON.stringify(
    { timestamp: new Date().toISOString(), release: release.html_url, assets: results },
    null,
    2,
  ),
);
