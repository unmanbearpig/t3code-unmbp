/** The byte adapter is shared by browsers, the Node listener and native mobile. */
export interface ReliableByteStream {
  read(maxBytes: number, signal: AbortSignal): Promise<Uint8Array | null>;
  write(bytes: Uint8Array, signal: AbortSignal): Promise<void>;
  finish(signal: AbortSignal): Promise<void>;
  reset(applicationCode: number): void;
  stop(applicationCode: number): void;
}

export interface TransportClose {
  readonly category: "closed" | "aborted" | "rejected" | "network" | "protocol";
  readonly applicationCode?: number;
}

export interface ReliableSession {
  readonly closed: Promise<TransportClose>;
  openBidirectionalStream(signal: AbortSignal): Promise<ReliableByteStream>;
  /** Starts best-effort transport cleanup without waiting for peer acknowledgement. */
  close(applicationCode: number): void;
}

export interface ServerReliableSession extends ReliableSession {
  acceptBidirectionalStream(signal: AbortSignal): Promise<ReliableByteStream>;
}

export interface ReliableConnector {
  connect(input: { readonly url: string; readonly signal: AbortSignal }): Promise<ReliableSession>;
}

export class RpcWebTransportError extends Error {
  readonly _tag = "RpcWebTransportError";
  readonly category: "unavailable" | "aborted" | "rejected" | "network" | "protocol" | "capacity";

  constructor(category: RpcWebTransportError["category"], message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RpcWebTransportError";
    this.category = category;
  }
}

export const RPC_WEBTRANSPORT_WIRE_VERSION = 1;
export const RPC_WEBTRANSPORT_LIMITS = {
  messageBytes: 100 * 1024 * 1024,
  recordBytes: 64 * 1024,
  compressedRecordBytes: 8 * 1024,
  controlMessageBytes: 8 * 1024,
  controlReserveBytes: 64 * 1024,
  sessionInputBytes: 256 * 1024 * 1024,
  sessionOutputBytes: 256 * 1024 * 1024,
  listenerInputBytes: 512 * 1024 * 1024,
  listenerOutputBytes: 512 * 1024 * 1024,
  ordinaryRequestStreams: 63,
  probeRequestStreams: 1,
  pendingOrdinaryRequests: 64,
  sessions: 64,
  admissions: 16,
  candidateTimeoutMs: 2_000,
  streamPrefaceTimeoutMs: 2_000,
  assemblyIdleTimeoutMs: 15_000,
  pingIntervalMs: 5_000,
  pingTimeoutMs: 15_000,
} as const;
