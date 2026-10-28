import { useEffect, useState, useSyncExternalStore } from "react";
import * as Runtime from "../../lib/runtime";
import * as ConnectionLog from "./connection-log";

const subscribeNone = () => () => {};
// Before SQLite loads, keep experimental status cues hidden. The runtime loads
// the saved setting before it can connect, so normal reconnect UI is unchanged.
const pending = () => true;

export function useReconnectExperimentEnabled() {
  const [log, setLog] = useState<ConnectionLog.MobileConnectionLog["Service"] | null>(null);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- loads the connection log service for the external store subscription
  useEffect(() => {
    let active = true;
    void Runtime.runtime
      .runPromise(ConnectionLog.MobileConnectionLog)
      .then((service) => {
        if (active) setLog(service);
      })
      .catch(() => undefined);
    return () => {
      active = false;
    };
  }, []);
  return useSyncExternalStore(
    log?.subscribe ?? subscribeNone,
    log?.experimentEnabled ?? pending,
    log?.experimentEnabled ?? pending,
  );
}
