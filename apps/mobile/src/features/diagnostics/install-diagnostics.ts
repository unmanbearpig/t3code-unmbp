import { AppState, Platform } from "react-native";
import Constants from "expo-constants";

import { setDebugLogRecorder } from "../../lib/debugLog";
import { flushDiagnosticLog, installDiagnosticLog, recordDiagnosticLog } from "./diagnostic-log";
import { startPerformanceMonitoring } from "./performance-telemetry";

installDiagnosticLog();
startPerformanceMonitoring();
setDebugLogRecorder((message, data) => recordDiagnosticLog("debug", message, data));
recordDiagnosticLog("info", "App started", {
  platform: Platform.OS,
  version: Constants.expoConfig?.version,
});
flushDiagnosticLog();
AppState.addEventListener("change", (state) => {
  recordDiagnosticLog("info", "App state", { state });
  if (state !== "active") flushDiagnosticLog();
});
