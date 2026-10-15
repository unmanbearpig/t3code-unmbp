import {
  wrapStandardWebTransportSession,
  type ServerReliableSession,
  type StandardWebTransportSession,
  type StandardWebTransportStream,
} from "./rpcWebTransport.ts";

export function memoryStreamPair(): [StandardWebTransportStream, StandardWebTransportStream] {
  const forward = new TransformStream<Uint8Array, Uint8Array>({
    transform(bytes, controller) {
      controller.enqueue(bytes.slice());
    },
  });
  const backward = new TransformStream<Uint8Array, Uint8Array>({
    transform(bytes, controller) {
      controller.enqueue(bytes.slice());
    },
  });
  return [
    { readable: backward.readable, writable: forward.writable },
    { readable: forward.readable, writable: backward.writable },
  ];
}

export function memorySessionPair(options: { readonly neverAcknowledgeClose?: boolean } = {}): {
  readonly client: ServerReliableSession;
  readonly server: ServerReliableSession;
  readonly rawClient: StandardWebTransportSession;
  readonly rawServer: StandardWebTransportSession;
  readonly opened: () => number;
} {
  let toClient!: ReadableStreamDefaultController<StandardWebTransportStream>;
  let toServer!: ReadableStreamDefaultController<StandardWebTransportStream>;
  const clientIncoming = new ReadableStream<StandardWebTransportStream>({
    start(controller) {
      toClient = controller;
    },
  });
  const serverIncoming = new ReadableStream<StandardWebTransportStream>({
    start(controller) {
      toServer = controller;
    },
  });
  let finish!: (value: { closeCode: number }) => void;
  const closed = new Promise<{ closeCode: number }>((resolve) => {
    finish = resolve;
  });
  let opened = 0;
  const rawClient: StandardWebTransportSession = {
    incomingBidirectionalStreams: clientIncoming,
    closed,
    async createBidirectionalStream() {
      const [local, remote] = memoryStreamPair();
      opened++;
      toServer.enqueue(remote);
      return local;
    },
    close(info) {
      if (!options.neverAcknowledgeClose) finish(info ?? { closeCode: 0 });
    },
  };
  const rawServer: StandardWebTransportSession = {
    incomingBidirectionalStreams: serverIncoming,
    closed,
    async createBidirectionalStream() {
      const [local, remote] = memoryStreamPair();
      toClient.enqueue(remote);
      return local;
    },
    close: rawClient.close,
  };
  return {
    client: wrapStandardWebTransportSession(rawClient),
    server: wrapStandardWebTransportSession(rawServer),
    rawClient,
    rawServer,
    opened: () => opened,
  };
}
