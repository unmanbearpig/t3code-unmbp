import { File, Paths } from "expo-file-system";
import * as Sharing from "expo-sharing";

import { describeError, shareDebug } from "../sharing/share-debug";

export async function shareConnectionLog(report: string): Promise<void> {
  // Android receivers can read the URI after the sheet closes, so retain this cache file.
  const file = new File(Paths.cache, "t3-mobile-connections.json");
  await file.write(report);
  shareDebug("outgoing:shareAsync start", { bytes: report.length, size: file.size });
  try {
    await Sharing.shareAsync(file.uri, {
      mimeType: "application/json",
      UTI: "public.json",
      dialogTitle: "Connection log",
    });
    shareDebug("outgoing:shareAsync resolved");
  } catch (error) {
    shareDebug("outgoing:shareAsync rejected", describeError(error));
    throw error;
  }
}
