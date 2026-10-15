/** Evidence required before a reused worker may start another case. */
export interface IsolationEvidence {
  readonly epoch: string;
  readonly environmentId: string;
  readonly initialMessages: number;
  readonly initialSequence: number;
  readonly httpPoolFresh: boolean;
  readonly shellCacheEmpty: boolean;
  readonly helloSeeded: boolean;
  readonly socketsBefore: number;
  readonly socketsAfter: number;
  readonly serverConnectionsAfter: number;
}
export function assertIsolation(current: IsolationEvidence, previous?: IsolationEvidence) {
  if (
    current.initialMessages !== 100 ||
    current.initialSequence !== 410 ||
    !current.httpPoolFresh ||
    !current.shellCacheEmpty ||
    current.socketsBefore !== 0 ||
    current.socketsAfter !== 0 ||
    current.serverConnectionsAfter !== 0
  )
    throw new Error(`Case reset failed: ${JSON.stringify(current)}`);
  if (
    previous &&
    (previous.epoch === current.epoch || previous.environmentId === current.environmentId)
  )
    throw new Error("Reused case identity");
}

/** Reusing the same object across cases fails even if the row claims a fresh resource. */
export function makeResourceTracker() {
  const seen = new WeakSet<object>();
  let sequence = 0;
  return (kind: string, resource: object) => {
    if (seen.has(resource)) throw new Error(`Resource carried across cases: ${kind}`);
    seen.add(resource);
    return `${kind}/${++sequence}`;
  };
}
