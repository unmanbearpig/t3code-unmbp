import { RpcWebTransportError } from "./rpcWebTransportCore.ts";

export interface TransportByteLease {
  readonly release: () => void;
}

/** A synchronous, non-waiting accounting ceiling. Children charge their parent too. */
export interface TransportByteBudget {
  readonly maxBytes: number;
  readonly usedBytes: number;
  readonly acquire: (bytes: number) => TransportByteLease;
  readonly child: (maxBytes: number) => TransportByteBudget;
}

export function createTransportByteBudget(maxBytes: number): TransportByteBudget {
  return makeBudget(maxBytes);
}

/** Capacity is held in the parent until all retained control allocations are released. */
export function reserveTransportByteBudget(parent: TransportByteBudget, maxBytes: number) {
  const reservation = parent.acquire(maxBytes);
  let closed = false;
  const release = () => {
    if (closed && budget.usedBytes === 0) reservation.release();
  };
  const budget = makeBudget(maxBytes, undefined, { isClosed: () => closed, onRelease: release });
  return {
    budget,
    close() {
      closed = true;
      release();
    },
  };
}

function makeBudget(
  maxBytes: number,
  parent?: TransportByteBudget,
  lifecycle?: {
    readonly isClosed: () => boolean;
    readonly onRelease: () => void;
  },
): TransportByteBudget {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new RangeError("Invalid transport byte budget");
  }
  let used = 0;
  const budget: TransportByteBudget = {
    maxBytes,
    get usedBytes() {
      return used;
    },
    acquire(bytes) {
      if (lifecycle?.isClosed())
        throw new RpcWebTransportError("aborted", "Transport byte budget closed");
      if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > maxBytes - used) {
        throw new RpcWebTransportError("capacity", "Transport byte budget exhausted");
      }
      const upstream = parent?.acquire(bytes);
      used += bytes;
      let released = false;
      return {
        release() {
          if (released) return;
          released = true;
          used -= bytes;
          upstream?.release();
          lifecycle?.onRelease();
        },
      };
    },
    child: (limit) => makeBudget(limit, budget),
  };
  return budget;
}
