import { useAtomValue } from "@effect/atom-react";
import { useCallback } from "react";

import { useRemoteOpenResolution } from "../remoteOpen";
import { serverEnvironment } from "../state/server";
import { useProjects } from "../state/entities";
import { useHandleNewThread } from "./useHandleNewThread";
import { shellEnvironment } from "../state/shell";
import { useAtomCommand } from "../state/use-atom-command";
import { useClientSettings } from "./useSettings";

export function useExternalTerminal() {
  const { activeDraftThread, activeThread } = useHandleNewThread();
  const projects = useProjects();
  const environmentId = activeThread?.environmentId ?? activeDraftThread?.environmentId ?? null;
  const projectId = activeThread?.projectId ?? activeDraftThread?.projectId ?? null;
  const project = projects.find(
    (project) => project.environmentId === environmentId && project.id === projectId,
  );
  const cwd = project
    ? (activeThread?.worktreePath ?? activeDraftThread?.worktreePath ?? project.workspaceRoot)
    : null;
  const config = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  const remote = useRemoteOpenResolution(environmentId);
  const terminal = useClientSettings((settings) => settings.externalTerminal);
  const launch = useAtomCommand(shellEnvironment.openInTerminal, "open external terminal");
  const available =
    environmentId !== null &&
    cwd !== null &&
    config?.shellOpenInTerminal === true &&
    remote.isResolved &&
    remote.state.mode === "local-exec";

  const open = useCallback(async () => {
    if (!available || environmentId === null || cwd === null) return;
    await launch({ environmentId, input: { cwd, terminal } });
  }, [available, environmentId, cwd, terminal, launch]);

  return { available, open };
}
