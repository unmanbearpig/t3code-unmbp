import { WS_METHODS, type ReconnectComparisonRecord } from "@t3tools/contracts";
import type { RpcSession } from "./session.ts";

export const recordReconnectComparison = (
  session: Pick<RpcSession, "client">,
  records: ReadonlyArray<ReconnectComparisonRecord>,
) => session.client[WS_METHODS.serverRecordReconnectComparison]({ records });
