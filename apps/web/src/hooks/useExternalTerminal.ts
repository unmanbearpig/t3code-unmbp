import type { DesktopTerminalTarget } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { useCallback, useEffect, useMemo, useState } from "react";

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
  const candidateUrls = useMemo(
    () => externalTerminalCandidateUrls({ target: connectionTarget, httpBaseUrl, directEndpoints }),
    [connectionTarget, httpBaseUrl, directEndpoints],
  );
  const target = useMemo(
    () => (environmentId !== null && hasPresentation ? { environmentId, candidateUrls } : null),
    [environmentId, hasPresentation, candidateUrls],
  );
  const terminal = useClientSettings((settings) => settings.externalTerminal);
  const probe = window.desktopBridge?.isLocalTerminalEnvironment;
  const launch = window.desktopBridge?.openInTerminal;
  const [locality, setLocality] = useState<{
    target: DesktopTerminalTarget;
    available: boolean;
  } | null>(null);

  useEffect(() => {
    if (target === null || probe === undefined || launch === undefined) return;
    let cancelled = false;
    void probe(target).then(
      (available) => {
        if (!cancelled) setLocality({ target, available });
      },
      () => {
        if (!cancelled) setLocality({ target, available: false });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [target, probe, launch]);

  const available =
    cwd !== null &&
    probe !== undefined &&
    launch !== undefined &&
    locality?.target === target &&
    locality?.available === true;

  const open = useCallback(async () => {
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
  }, [available, target, cwd, terminal, launch]);

  return { available, open };
}
