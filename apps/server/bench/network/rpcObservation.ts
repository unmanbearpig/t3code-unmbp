// @effect-diagnostics effectFnOpportunity:off - Protocol benchmarks must not add tracing spans to measured operations.
// @effect-diagnostics anyUnknownInErrorContext:off - Reflection preserves each original generic RPC method type at the proxy boundary.
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Session from "../../../../packages/client-runtime/src/rpc/session.ts";

export interface RpcObservation {
  readonly request: (tag: string, payload?: unknown) => void;
  readonly value: (tag: string, value: unknown) => void;
  readonly session: (
    event: "start" | "selected" | "ready",
    transport?: Session.RpcSession["transport"],
  ) => void;
}

/** Observe decoded application values equally for both codecs, without reparsing wire JSON. */
export function observeRpcClient<Client extends object>(
  client: Client,
  observation: RpcObservation,
): Client {
  return new Proxy(client, {
    get(target, property, receiver) {
      const method: unknown = Reflect.get(target, property, receiver);
      if (typeof property !== "string" || typeof method !== "function") return method;
      return (...args: readonly unknown[]) => {
        const operation: unknown = Reflect.apply(method, target, args);
        if (Stream.isStream(operation))
          return Stream.suspend(() => {
            observation.request(property, args[0]);
            return operation.pipe(
              Stream.tap((value) => Effect.sync(() => observation.value(property, value))),
            );
          });
        if (Effect.isEffect(operation))
          return Effect.suspend(() => {
            observation.request(property, args[0]);
            return operation.pipe(
              Effect.tap((value) => Effect.sync(() => observation.value(property, value))),
            );
          });
        return operation;
      };
    },
  });
}

/** Keep the real application-owned factory and cooldown; decorate only its returned diagnostics. */
export function observeRpcSessionFactory(
  factory: Session.RpcSessionFactory["Service"],
  observation: RpcObservation,
) {
  return Session.RpcSessionFactory.of({
    connect: Effect.fnUntraced(function* (connection) {
      observation.session("start");
      const session = yield* factory.connect(connection);
      observation.session("selected", session.transport);
      return {
        ...session,
        client: observeRpcClient(session.client, observation),
        initialConfig: session.initialConfig.pipe(
          Effect.tap((value) => Effect.sync(() => observation.value("initialConfig", value))),
        ),
        ready: session.ready.pipe(
          Effect.tap(() => Effect.sync(() => observation.session("ready", session.transport))),
        ),
        probe: Effect.sync(() => observation.request("session-probe")).pipe(
          Effect.andThen(session.probe),
        ),
        subscribeServerConfig: (...args) =>
          session
            .subscribeServerConfig(...args)
            .pipe(
              Stream.tap((value) =>
                Effect.sync(() => observation.value("subscribeServerConfig", value)),
              ),
            ),
      } satisfies Session.RpcSession;
    }),
  });
}
