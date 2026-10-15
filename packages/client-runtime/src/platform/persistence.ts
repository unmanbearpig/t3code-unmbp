import {
  EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadDetailSnapshot,
  type ServerConfig,
  type ThreadId,
  type VcsListRefsResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { ConnectionRegistration } from "../connection/catalog.ts";
import type { DeferredShellSnapshot } from "../state/shellPullRequests.ts";
import type { ConnectionTarget, PersistedConnectionTarget } from "../connection/model.ts";

export class ConnectionPersistenceError extends Schema.TaggedError<ConnectionPersistenceError>()(
  "ConnectionPersistenceError",
  {
    operation: Schema.Literals([
      "list-targets",
      "list-disabled-targets",
      "register-connection",
      "set-connection-routes",
      "remove-connection",
      "set-connection-enabled",
      "load-shell",
      "save-shell",
      "load-thread",
      "save-thread",
      "remove-thread",
      "load-server-config",
      "save-server-config",
      "load-vcs-refs",
      "save-vcs-refs",
      "remove-vcs-refs",
      "clear-vcs-refs",
      "clear-environment",
      "load-hello-support",
      "save-hello-support",
      "remove-hello-support",
    ]),
    message: Schema.String,
  },
) {}

export class ConnectionTargetStore extends Context.Service<
  ConnectionTargetStore,
  {
    readonly list: Effect.Effect<ReadonlyArray<ConnectionTarget>, ConnectionPersistenceError>;
    /** Saved environments the user switched off. See `ConnectionRegistrationStore.setEnabled`. */
    readonly listDisabled: Effect.Effect<ReadonlyArray<EnvironmentId>, ConnectionPersistenceError>;
  }
>()("@t3tools/client-runtime/platform/persistence/ConnectionTargetStore") {}

export class ConnectionRegistrationStore extends Context.Service<
  ConnectionRegistrationStore,
  {
    /**
     * Saves one route's records and sets the environment's full route list,
     * preferred first. Records of routes missing from `routes` are dropped.
     */
    readonly register: (
      registration: ConnectionRegistration,
      routes: ReadonlyArray<PersistedConnectionTarget>,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /** Reorders or drops routes without adding one. `routes` must not be empty. */
    readonly setRoutes: (
      environmentId: EnvironmentId,
      routes: ReadonlyArray<PersistedConnectionTarget>,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /** Forgets the environment and every route it had. */
    readonly remove: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly setEnabled: (
      environmentId: EnvironmentId,
      enabled: boolean,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
  }
>()("@t3tools/client-runtime/platform/persistence/ConnectionRegistrationStore") {}

/** A discovery hint only. Every H connection still checks the live Hello before sending credentials. */
export const StoredHelloSupport = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  environmentId: EnvironmentId,
  httpBaseUrl: Schema.String,
  wsBaseUrl: Schema.String,
});

export const helloSupportEndpointKey = (httpBaseUrl: string, wsBaseUrl: string) =>
  JSON.stringify([httpBaseUrl, wsBaseUrl]);

export class EnvironmentCacheStore extends Context.Service<
  EnvironmentCacheStore,
  {
    /** Opt-in prototype hints use the same durable storage and environment cleanup as other caches. */
    readonly helloSupport?: {
      readonly load: (
        environmentId: EnvironmentId,
        httpBaseUrl: string,
        wsBaseUrl: string,
      ) => Effect.Effect<boolean, ConnectionPersistenceError>;
      readonly save: (
        environmentId: EnvironmentId,
        httpBaseUrl: string,
        wsBaseUrl: string,
      ) => Effect.Effect<void, ConnectionPersistenceError>;
      readonly remove: (
        environmentId: EnvironmentId,
        httpBaseUrl: string,
        wsBaseUrl: string,
      ) => Effect.Effect<void, ConnectionPersistenceError>;
    };
    readonly loadShell: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<Option.Option<DeferredShellSnapshot>, ConnectionPersistenceError>;
    readonly saveShell: (
      environmentId: EnvironmentId,
      snapshot: OrchestrationV2ShellSnapshot,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly loadThread: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
    ) => Effect.Effect<
      Option.Option<OrchestrationV2ThreadDetailSnapshot>,
      ConnectionPersistenceError
    >;
    readonly saveThread: (
      environmentId: EnvironmentId,
      snapshot: OrchestrationV2ThreadDetailSnapshot,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly removeThread: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * The last complete server configuration. This deliberately includes provider
     * metadata so offline task creation can still offer the models a user last saw.
     */
    readonly loadServerConfig: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<Option.Option<ServerConfig>, ConnectionPersistenceError>;
    readonly saveServerConfig: (
      environmentId: EnvironmentId,
      config: ServerConfig,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * The unfiltered branch list for a workspace. Query-specific lists are not
     * cached because they are incomplete and unsafe to present as a full picker.
     */
    readonly loadVcsRefs: (
      environmentId: EnvironmentId,
      cwd: string,
    ) => Effect.Effect<Option.Option<VcsListRefsResult>, ConnectionPersistenceError>;
    readonly saveVcsRefs: (
      environmentId: EnvironmentId,
      cwd: string,
      refs: VcsListRefsResult,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly removeVcsRefs: (
      environmentId: EnvironmentId,
      cwd: string,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * Removes every persisted branch-list snapshot for an environment. Git ref
     * mutations are repository-wide, and linked worktrees may have cached the
     * same refs under different working-directory keys.
     */
    readonly clearVcsRefs: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly clear: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
  }
>()("@t3tools/client-runtime/platform/persistence/EnvironmentCacheStore") {}

export class EnvironmentOwnedDataCleanup extends Context.Reference<{
  readonly clear: (environmentId: EnvironmentId) => Effect.Effect<void>;
}>("@t3tools/client-runtime/platform/persistence/EnvironmentOwnedDataCleanup", {
  defaultValue: () => ({
    clear: () => Effect.void,
  }),
}) {}
