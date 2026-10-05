import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export const EXTERNAL_TERMINALS = [
  { id: "foot", label: "foot", command: "foot", directoryFlag: "--working-directory" },
  { id: "kitty", label: "kitty", command: "kitty", directoryFlag: "--directory" },
  {
    id: "alacritty",
    label: "Alacritty",
    command: "alacritty",
    directoryFlag: "--working-directory",
  },
] as const;

export const ExternalTerminalId = Schema.Literals(
  EXTERNAL_TERMINALS.map((terminal) => terminal.id),
);
export type ExternalTerminalId = typeof ExternalTerminalId.Type;

export const LaunchTerminalInput = Schema.Struct({
  cwd: TrimmedNonEmptyString,
  terminal: ExternalTerminalId,
});
export type LaunchTerminalInput = typeof LaunchTerminalInput.Type;

export class ExternalTerminalLaunchError extends Schema.TaggedError<ExternalTerminalLaunchError>()(
  "ExternalTerminalLaunchError",
  {
    ...LaunchTerminalInput.fields,
    reason: Schema.Literals([
      "unsupported-platform",
      "display-unavailable",
      "invalid-directory",
      "command-not-found",
      "spawn-failed",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "unsupported-platform":
        return `${this.terminal} is not supported on this platform.`;
      case "display-unavailable":
        return `No graphical session is available to open ${this.terminal}.`;
      case "invalid-directory":
        return `Cannot open a terminal because '${this.cwd}' is not a directory.`;
      case "command-not-found":
        return `${this.terminal} is not installed or is not available on PATH.`;
      case "spawn-failed":
        return `Failed to open ${this.terminal} in '${this.cwd}'.`;
    }
  }
}
