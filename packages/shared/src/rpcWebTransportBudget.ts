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

function makeBudget(maxBytes: number, parent?: TransportByteBudget): TransportByteBudget {
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
        },
      };
    },
    child: (limit) => makeBudget(limit, budget),
  };
  return budget;
}
