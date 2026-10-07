import type {
  DesktopTerminalLaunchInput,
  DesktopTerminalLaunchResult,
  DesktopTerminalTarget,
} from "@t3tools/contracts";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { useExternalTerminal } from "./useExternalTerminal";

const state = vi.hoisted(() => ({
  activeThread: { environmentId: "local", projectId: "project", worktreePath: "/project/worktree" },
  presentation: {
    entry: {
      target: {
        _tag: "BearerConnectionTarget",
        environmentId: "local",
        connectionId: "saved",
        label: "Local",
      },
      profile: {
        _tag: "Some",
        value: { _tag: "BearerConnectionProfile", httpBaseUrl: "http://127.0.0.1:3773" },
      },
    },
    serverConfig: undefined,
  },
  toast: vi.fn(),
}));

vi.mock("./useHandleNewThread", () => ({
  useHandleNewThread: () => ({ activeThread: state.activeThread, activeDraftThread: null }),
}));
vi.mock("../state/entities", () => ({
  useProjects: () => [
    { environmentId: "local", id: "project", workspaceRoot: "/project" },
    { environmentId: "remote", id: "project", workspaceRoot: "/project" },
  ],
}));
vi.mock("../state/presentation", () => ({
  useEnvironmentPresentation: () => ({ presentation: state.presentation }),
}));
vi.mock("./useSettings", () => ({ useClientSettings: () => "foot" }));
vi.mock("../components/ui/toast", () => ({
  stackedThreadToast: (value: unknown) => value,
  toastManager: { add: state.toast },
}));

let renderer: ReactTestRenderer;
let value: ReturnType<typeof useExternalTerminal>;
function Probe() {
  const terminal = useExternalTerminal();
  useLayoutEffect(() => {
    value = terminal;
  });
  return null;
}

const probe = vi.fn<(target: DesktopTerminalTarget) => Promise<boolean>>();
const launch = vi.fn<(input: DesktopTerminalLaunchInput) => Promise<DesktopTerminalLaunchResult>>();

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", {
    desktopBridge: { isLocalTerminalEnvironment: probe, openInTerminal: launch },
  });
  probe.mockReset().mockResolvedValue(true);
  launch.mockReset().mockResolvedValue({ opened: true });
  state.toast.mockClear();
  state.activeThread = {
    environmentId: "local",
    projectId: "project",
    worktreePath: "/project/worktree",
  };
  state.presentation.entry.profile.value.httpBaseUrl = "http://127.0.0.1:3773";
});
afterEach(() => {
  if (renderer) act(() => renderer.unmount());
  vi.unstubAllGlobals();
});

async function mount() {
  await act(async () => {
    renderer = create(<Probe />);
  });
}

it("opens the active worktree through the desktop without a server capability flag", async () => {
  await mount();
  expect(value.available).toBe(true);
  await value.open();
  expect(launch).toHaveBeenCalledWith({
    environmentId: "local",
    candidateUrls: ["http://127.0.0.1:3773"],
    cwd: "/project/worktree",
    terminal: "foot",
  });
});

it("does not launch for a remote thread with the same project path", async () => {
  probe.mockResolvedValue(false);
  state.activeThread.environmentId = "remote";
  await mount();
  expect(value.available).toBe(false);
  await value.open();
  expect(launch).not.toHaveBeenCalled();
});

it("stops using the previous environment while a new locality check is pending", async () => {
  await mount();
  let finishProbe: (local: boolean) => void = () => undefined;
  probe.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finishProbe = resolve;
      }),
  );
  state.activeThread = { ...state.activeThread, environmentId: "remote" };
  await act(async () => {
    renderer.update(<Probe />);
  });
  expect(value.available).toBe(false);
  await value.open();
  expect(launch).not.toHaveBeenCalled();
  await act(async () => {
    finishProbe(false);
  });
  expect(value.available).toBe(false);
});

it("ignores a late local result after switching to a remote thread", async () => {
  let finishProbe: (local: boolean) => void = () => undefined;
  probe.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finishProbe = resolve;
      }),
  );
  await mount();
  state.activeThread = { ...state.activeThread, environmentId: "remote" };
  probe.mockResolvedValue(false);
  await act(async () => {
    renderer.update(<Probe />);
  });
  await act(async () => {
    finishProbe(true);
  });
  expect(value.available).toBe(false);
  await value.open();
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch in a browser or an older desktop shell", async () => {
  vi.stubGlobal("window", {});
  await mount();
  expect(value.available).toBe(false);
  await value.open();
  expect(probe).not.toHaveBeenCalled();
  expect(launch).not.toHaveBeenCalled();
});

it("shows an error from the desktop terminal launcher", async () => {
  launch.mockResolvedValue({
    opened: false,
    error: "foot is not installed or is not available on PATH.",
  });
  await mount();
  await value.open();
  expect(state.toast).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "error",
      description: "foot is not installed or is not available on PATH.",
    }),
  );
});
