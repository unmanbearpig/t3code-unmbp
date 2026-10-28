import {
  DesktopTerminalLaunchInput,
  DesktopTerminalLaunchResult,
  DesktopTerminalTarget,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as DesktopExternalTerminal from "../../shell/DesktopExternalTerminal.ts";
import * as DesktopIpc from "../DesktopIpc.ts";
import * as IpcChannels from "../channels.ts";

export const isLocalTerminalEnvironment = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.IS_LOCAL_TERMINAL_ENVIRONMENT_CHANNEL,
  payload: DesktopTerminalTarget,
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.externalTerminal.isLocalEnvironment")(function* (target) {
    const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
    return yield* terminal.isLocalEnvironment(target);
  }),
});

export const openInTerminal = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.OPEN_IN_TERMINAL_CHANNEL,
  payload: DesktopTerminalLaunchInput,
  result: DesktopTerminalLaunchResult,
  handler: Effect.fn("desktop.ipc.externalTerminal.open")(function* (input) {
    const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
    return yield* terminal.launch(input).pipe(
      Effect.map((opened) => ({ opened })),
      Effect.catchTags({
        ExternalTerminalLaunchError: (error) =>
          Effect.succeed({ opened: false, error: error.message }),
      }),
    );
  }),
});
