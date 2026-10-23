import { describe, expect, it } from "vite-plus/test";

import { parseStartupCrashRecords } from "./crash-log-model";

const BUNDLE =
  "/Users/expo/workingdir/build/apps/mobile/ios/build/Build/Intermediates.noindex/ArchiveIntermediates/T3Code/BuildProductsPath/Release-iphoneos/main.jsbundle";
const FATAL = {
  timestamp: 1789277752000,
  level: "error",
  code: "JSRuntimeError",
  message: [
    "ErrorRecovery fatal exception: Fatal error: Time: 1789277752033.127930",
    "Domain: RCTErrorDomain",
    "Code: 0",
    "Description: Unhandled JS Exception: TypeError: Cannot read property 'defaultModelSelection' of null",
    "",
    "This error is located at:",
    `    at NewTaskFlowProvider (${BUNDLE}:590585:51)`,
    `    at NavigationProvider (${BUNDLE}:139067:3)`,
    "    at RNSSafeAreaView (<anonymous>)",
  ].join("\n"),
};

describe("parseStartupCrashRecords", () => {
  it("keeps Android recovery crashes with their exception and native stack", () => {
    const stacktrace = [
      "com.facebook.react.common.JavascriptException: broken",
      "com.facebook.react.runtime.ReactInstance.callFunctionOnModule(ReactInstance.java:123)",
    ];
    const [record] = parseStartupCrashRecords([
      {
        timestamp: 1,
        level: "error",
        code: "Unknown",
        message: "ErrorRecovery: exception encountered: TypeError: broken",
      },
      {
        timestamp: 2,
        level: "error",
        code: "Unknown",
        message: "UpdatesErrorRecovery: could not recover from error, crashing",
        stacktrace,
      },
    ]);
    expect(record).toMatchObject({
      timestamp: 2,
      description: "TypeError: broken",
      frames: stacktrace,
    });
    expect(record?.detail).toContain("ReactInstance.java:123");
  });

  it("extracts the JS error and a compact component stack from a fatal entry", () => {
    const [record] = parseStartupCrashRecords([FATAL]);
    expect(record?.description).toBe(
      "TypeError: Cannot read property 'defaultModelSelection' of null",
    );
    expect(record?.frames).toEqual([
      "NewTaskFlowProvider (main.jsbundle:590585:51)",
      "NavigationProvider (main.jsbundle:139067:3)",
      "RNSSafeAreaView",
    ]);
    expect(record?.detail).toContain("Domain: RCTErrorDomain");
  });

  it("ignores update-check noise and orders crashes newest first", () => {
    const records = parseStartupCrashRecords([
      { timestamp: 1, level: "info", code: "NoUpdatesAvailable", message: "checked" },
      { ...FATAL, timestamp: 10 },
      { timestamp: 5, level: "error", code: "UpdateFailedToLoad", message: "nope" },
      { ...FATAL, timestamp: 20 },
    ]);
    expect(records.map((record) => record.timestamp)).toEqual([20, 10]);
  });
});
