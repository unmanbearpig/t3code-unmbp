import * as Option from "effect/Option";
import { useEffect, useEffectEvent, useState } from "react";

import { externalTerminalCandidateUrls } from "../lib/externalTerminal";
import { useProjects } from "../state/entities";
import { useEnvironmentPresentation } from "../state/presentation";
import { useHandleNewThread } from "./useHandleNewThread";
import { useClientSettings } from "./useSettings";
import { stackedThreadToast, toastManager } from "../components/ui/toast";

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
  const { presentation } = useEnvironmentPresentation(environmentId);
  const connectionTarget = presentation?.entry.target ?? null;
  const profile = presentation ? Option.getOrNull(presentation.entry.profile) : null;
  const httpBaseUrl = profile?._tag === "BearerConnectionProfile" ? profile.httpBaseUrl : null;
  const directEndpoints = presentation?.serverConfig?.directEndpoints;
  const hasPresentation = presentation !== null;
  const candidateUrls = externalTerminalCandidateUrls({
    target: connectionTarget,
    httpBaseUrl,
    directEndpoints,
  });
  const target =
    environmentId !== null && hasPresentation ? { environmentId, candidateUrls } : null;
  const terminal = useClientSettings((settings) => settings.externalTerminal);
  const probe = window.desktopBridge?.isLocalTerminalEnvironment;
  const launch = window.desktopBridge?.openInTerminal;
  const [locality, setLocality] = useState<{
    key: string;
    available: boolean;
  } | null>(null);

  const targetKey = target === null ? null : JSON.stringify(target);
  const readTarget = useEffectEvent(() => target);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- probes the native desktop terminal and ignores responses after unmount
  useEffect(() => {
    const currentTarget = readTarget();
    if (targetKey === null || currentTarget === null || probe === undefined || launch === undefined)
      return;
    let cancelled = false;
    void probe(currentTarget).then(
      (available) => {
        if (!cancelled) setLocality({ key: targetKey, available });
      },
      () => {
        if (!cancelled) setLocality({ key: targetKey, available: false });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [targetKey, probe, launch]);

  const available =
    cwd !== null &&
    probe !== undefined &&
    launch !== undefined &&
    locality?.key === targetKey &&
    locality?.available === true;

  const open = async () => {
    if (!available || target === null || cwd === null || launch === undefined) return;
    try {
      const result = await launch({ ...target, cwd, terminal });
      if (result.error) throw new Error(result.error);
    } catch (error) {
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: "Unable to open terminal",
          description:
            error instanceof Error ? error.message : "The desktop terminal launch failed.",
        }),
      );
    }
  };

  return { available, open };
}
